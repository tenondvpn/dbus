package dbus

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"sync"
	"time"

	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/client/v3"
)

type LeaseItem struct {
	ls *etcd.Lease
	lg *etcd.LeaseGrantResponse
}

type Queue struct {
	queueInfo         *QueueInfo
	queuesPrefix      string
	queuePath         string
	receivers         [MaxReceivers]*Receiver
	transmitters      map[string]*Transmitter
	txMutex           sync.Mutex
	queueNameWithId   string
	mat               *Material
	expirePath        string
	seqIdPath         string
	txPath            string
	txClusterPath     string
	rxClusterPath     string
	rxPath            string
	leaderRxId        uint16
	leaderTxKeys      map[string]string
	txLease           *LeaseItem
	rxLease           *LeaseItem
	localTx           *Transmitter
	localRx           *Receiver
	prevCheckRxLeader int64
	prevCheckTxLeader int64
	prevGetAllRx      int64
	prevStatusTmMilli int64
	localMaxSeqId     uint64
	maxSeqId          uint64 // 这个队列当前leader最大序号
	canCheckRxLeader  bool
}

func GetOrCreateQueue(queueName string, ha bool, expireSecond int64, protoType int) *Queue {
	q := new(Queue)
	q.queueInfo = new(QueueInfo)
	q.queuesPrefix = QueuePathPrefix + "/queues/"
	q.queueInfo.QueueName = queueName
	q.queuePath = q.queuesPrefix + queueName
	if expireSecond == 0 || expireSecond >= 31*24*3600 {
		expireSecond = 24 * 3600
	}

	q.maxSeqId = MaxUint64
	q.queueInfo.ExpireSeconds = expireSecond
	q.queueInfo.ProtoType = protoType
	q.queueInfo.Ha = ha
	q.queueInfo.WindowSize = DefaultTxWindowSize
	q.expirePath = q.queuePath + "/expire"
	q.seqIdPath = q.queuePath + "/seqid"
	q.txPath = q.queuePath + "/tx/"
	q.rxPath = q.queuePath + "/rx/"
	q.txClusterPath = q.queuePath + "/cluster_tx/"
	q.mat = NewMaterial(etcdClient)
	q.transmitters = make(map[string]*Transmitter)
	q.leaderTxKeys = make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, q.queuePath)
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", q.queuePath, err)
		return nil
	}

	// just use exists queue
	if resp != nil && resp.Count > 0 {
		if len(resp.Kvs) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, resp)
			return nil
		}

		if !q.getQueueInfo(resp.Kvs[0].Value) {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, resp)
			return nil
		}

		q.getQueueSeqId()
		return q
	}

	q.queueInfo.Id = q.mat.GetQueueId()
	data, err := json.Marshal(q.queueInfo)
	if err != nil {
		logrus.Errorf("create queue: %s failed, json dump failed!", queueName)
		return nil
	}

	tresp, err := etcdClient.Txn(context.TODO()).
		If(etcd.Compare(etcd.Version(q.queuePath), "=", 0)).
		Then(etcd.OpPut(q.queuePath, string(data)), etcd.OpPut(AllQueuePath+"/"+queueName, "1")).
		Else(etcd.OpGet(q.queuePath)).
		Commit()
	if err != nil {
		logrus.Error(err)
		q.mat.RemoveQueueId(q.queueInfo.Id)
		return nil
	}

	if !tresp.Succeeded {
		q.mat.RemoveQueueId(q.queueInfo.Id)
		if len(tresp.Responses) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if len(tresp.Responses[0].GetResponseRange().Kvs) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if !q.getQueueInfo(tresp.Responses[0].GetResponseRange().Kvs[0].Value) {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		q.getQueueSeqId()
	} else {
		q.writeSeqId(0) // init queue messge sequence id
	}

	return q
}

func (q *Queue) DeleteQueue() bool {
	q.mat.RemoveQueueId(q.queueInfo.Id)
	{
		q.txMutex.Lock()
		for _, v := range q.transmitters {
			q.DeleteTransmitter(v)
		}
		q.txMutex.Unlock()
	}

	for _, rx := range q.receivers {
		if rx != nil {
			q.DeleteReceiver(rx.rxInfo.Id)
		}
	}

	{
		_, err := etcdClient.Txn(context.TODO()).
			If(etcd.Compare(etcd.Version(q.queuePath), "!=", 0)).
			Then(
				etcd.OpDelete(q.queuePath, etcd.WithPrefix()),
				etcd.OpDelete(AllQueuePath+"/"+q.queueInfo.QueueName, etcd.WithPrefix())).Commit()
		if err != nil {
			logrus.Error(err)
			return false
		}
	}

	return true
}

func (q *Queue) ResetExpireTime() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Put(ctx, q.expirePath, fmt.Sprintf("%d", time.Now().Unix()+q.queueInfo.ExpireSeconds))
	cancel()
	if err != nil {
		logrus.Errorf("etcd put reset expire time %s failed: %v", q.expirePath, err)
		return
	}

	logrus.Debugf("etcd put success: %v", resp)
}

func (q *Queue) GetExpireTime() int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, q.expirePath)
	cancel()
	if err != nil {
		logrus.Errorf("etcd put reset expire time %s failed: %v", q.expirePath, err)
		return 0
	}

	if resp.Count <= 0 {
		return 0
	}

	val, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		logrus.Errorf("etcd put reset expire time %s failed: %v", q.expirePath, err)
		return 0
	}

	logrus.Debugf("etcd put success: %v", resp)
	return val
}

func (q *Queue) GetProtoType() int {
	return q.queueInfo.ProtoType
}

// cluster is empty, then use ip_port as cluster
// same ip port will fail
func (q *Queue) CreateTransmitter(cluster string) *Transmitter {
	tx := q.createTx(cluster)
	if tx != nil {
		queuManager.getQueueDetail(q)
	}

	return tx
}

// same ip port will fail
func (q *Queue) CreateReceiver(cb ReceiverCallback) *Receiver {
	rx := q.createRx(cb)
	if rx != nil {
		queuManager.getQueueDetail(q)
	}

	return rx
}

func (q *Queue) createRx(cb ReceiverCallback) *Receiver {
	if q.localRx != nil {
		return q.localRx
	}

	rxPath := q.rxPath + globalLocalIp + "_" + fmt.Sprintf("%d", globalLocalPort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, rxPath)
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", rxPath, err)
		return nil
	}

	rxInfo := new(ReceiverInfo)
	rxInfo.Ip = globalLocalIp
	rxInfo.Port = globalLocalPort
	// just use exists tx
	if resp != nil && resp.Count > 0 {
		if len(resp.Kvs) <= 0 {
			logrus.Errorf("get exists queue path res kv error: %s, %v", rxPath, resp)
			return nil
		}

		if !q.getRxInfo(resp.Kvs[0].Value, rxInfo) {
			logrus.Errorf("get exists queue path res kv error: %s, %v", rxPath, resp)
			return nil
		}

		rx := CreateNewReceiver(q, rxInfo, cb)
		q.addNewReceiver(rx)
		q.localRx = rx
		q.checkRxLeader(TimeStampMilli())
		q.canCheckRxLeader = true
		return rx
	}

	rxInfo.QueueId = q.queueInfo.Id
	rxInfo.Id = q.mat.GetTxOrRxId()
	data, err := json.Marshal(rxInfo)
	if err != nil {
		logrus.Errorf("create tx: %s failed, json dump failed!", rxPath)
		return nil
	}

	tresp, err := etcdClient.Txn(context.TODO()).
		If(etcd.Compare(etcd.Version(rxPath), "=", 0)).
		Then(etcd.OpPut(rxPath, string(data))).
		Else(etcd.OpGet(rxPath)).
		Commit()
	if err != nil {
		logrus.Error(err)
		q.mat.RemoveTxOrRxId(rxInfo.Id)
		return nil
	}

	if !tresp.Succeeded {
		q.mat.RemoveTxOrRxId(rxInfo.Id)
		if len(tresp.Responses) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if len(tresp.Responses[0].GetResponseRange().Kvs) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if !q.getRxInfo(tresp.Responses[0].GetResponseRange().Kvs[0].Value, rxInfo) {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}
	}

	rx := CreateNewReceiver(q, rxInfo, cb)
	q.addNewReceiver(rx)
	q.localRx = rx
	q.checkRxLeader(TimeStampMilli())
	q.canCheckRxLeader = true
	return rx
}

// rx distinct with id
func (q *Queue) GetRxLeader() *Receiver {
	return q.receivers[q.leaderRxId]
}

func (q *Queue) GetTxLeader(cluster string) *Transmitter {
	key, ok := q.leaderTxKeys[cluster]
	if !ok {
		return nil
	}

	var tx *Transmitter
	{
		q.txMutex.Lock()
		tx, ok = q.transmitters[key]
		q.txMutex.Unlock()
		if !ok {
			return nil
		}
	}

	return tx
}

func (q *Queue) GetAllReceivers() []*Receiver {
	resRxs := make([]*Receiver, 0)
	for _, rx := range q.receivers {
		if rx != nil {
			resRxs = append(resRxs, rx)
		}
	}
	return resRxs
}

// callback for watch or round events
func (q *Queue) OnNewReceiver(jsonStr []byte) {
	rxInfo := new(ReceiverInfo)
	if !q.getRxInfo(jsonStr, rxInfo) {
		return
	}

	if q.localRx != nil {
		q.localRx.handleReceiver(rxInfo)
	}

	if q.localTx != nil {
		q.localTx.handleReceiver(rxInfo)
	}
}

func (q *Queue) OnNewTransmitter(cluster string, jsonStr []byte) {
	txInfo := new(TransmitterInfo)
	if !q.getTxInfo(jsonStr, txInfo) {
		return
	}

	if q.localRx != nil {
		q.localRx.handleNewTransmitter(txInfo)
	}
}

func (q *Queue) DeleteReceiver(id uint16) {
	rx := q.receivers[id]
	if rx == nil {
		return
	}

	rxPath := q.rxPath + "/" + rx.rxInfo.Ip + "_" + fmt.Sprintf("%d", rx.rxInfo.Port)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	_, err := etcdClient.Delete(ctx, rxPath)
	cancel()
	q.receivers[id] = nil
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", rxPath, err)
		return
	}

	q.mat.RemoveTxOrRxId(rx.rxInfo.Id)
}

func (q *Queue) DeleteTransmitterWithLock(tx *Transmitter) {
	key := fmt.Sprintf("%d_%s_%d", tx.txInfo.Id, tx.txInfo.Ip, tx.txInfo.Port)
	{
		q.txMutex.Lock()
		delete(q.transmitters, key)
		q.txMutex.Unlock()
	}
	q.mat.RemoveTxOrRxId(tx.txInfo.Id)
}

func (q *Queue) DeleteTransmitter(tx *Transmitter) {
	key := fmt.Sprintf("%d_%s_%d", tx.txInfo.Id, tx.txInfo.Ip, tx.txInfo.Port)
	delete(q.transmitters, key)
	q.mat.RemoveTxOrRxId(tx.txInfo.Id)
}

func (q *Queue) checkTxLeader(tm int64) {
	if q.localTx == nil {
		return
	}

	if q.prevCheckTxLeader+int64(CheckLeaderTimeout) >= tm {
		return
	}

	q.prevCheckTxLeader = tm
	if q.txLease != nil {
		_, err := (*q.txLease.ls).KeepAliveOnce(context.TODO(), q.txLease.lg.ID)
		if err != nil {
			logrus.Println("keep alive once err: ", err)
			q.localTx.txInfo.IsLeader = false
			q.txLease = nil
			return
		}

		q.localTx.txInfo.IsLeader = true
		return
	}

	ls := etcd.NewLease(etcdClient)
	grantResp, err := ls.Grant(context.TODO(), 10)
	if err != nil {
		logrus.Error("grant err: ", err)
		return
	}

	lockKey := q.queuePath + "/locktx/" + fmt.Sprintf("%d", q.localTx.txInfo.Id)
	val := fmt.Sprintf("%d_%s_%d", q.localTx.txInfo.Id, q.localTx.txInfo.Ip, q.localTx.txInfo.Port)
	tresp, err := etcdClient.Txn(context.TODO()).
		If(etcd.Compare(etcd.Version(lockKey), "=", 0)).
		Then(etcd.OpPut(lockKey, val, etcd.WithLease(grantResp.ID))).
		Else(etcd.OpGet(lockKey)).
		Commit()
	if err != nil {
		logrus.Error(err)
		return
	}

	if !tresp.Succeeded {
		return
	}

	logrus.Debugf("create tx leader success: %v", tresp)
	q.localTx.txInfo.IsLeader = true
	q.txLease = &LeaseItem{&ls, grantResp}
}

// for ha
func (q *Queue) IsHa() bool {
	return q.queueInfo.Ha
}

func (q *Queue) checkRxLeader(tm int64) {
	if q.localRx == nil || !q.IsHa() {
		return
	}

	q.prevCheckRxLeader = tm
	if q.rxLease != nil {
		_, err := (*q.rxLease.ls).KeepAliveOnce(context.TODO(), q.rxLease.lg.ID)
		if err != nil {
			logrus.Errorf("keep alive once err: %v", err)
			q.rxLease = nil
			q.leaderRxId = 0
			return
		}

		q.leaderRxId = q.localRx.rxInfo.Id
		return
	}

	// must synced seqid with leader
	if q.maxSeqId == MaxUint64 || math.Abs(float64(q.maxSeqId-q.localMaxSeqId)) > float64(SyncToleranceRange) {
		return
	}

	ls := etcd.NewLease(etcdClient)
	grantResp, err := ls.Grant(context.TODO(), 10)
	if err != nil {
		logrus.Error("grant err: ", err)
	}

	lockKey := q.queuePath + "/lockrx"
	tresp, err := etcdClient.Txn(context.TODO()).
		If(etcd.Compare(etcd.Version(lockKey), "=", 0)).
		Then(etcd.OpPut(lockKey, fmt.Sprintf("%d", q.localRx.rxInfo.Id), etcd.WithLease(grantResp.ID))).
		Else(etcd.OpGet(lockKey)).
		Commit()
	if err != nil {
		logrus.Error(err)
		return
	}

	if !tresp.Succeeded {
		if len(tresp.Responses) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return
		}

		if len(tresp.Responses[0].GetResponseRange().Kvs) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return
		}

		leaderId, err := strconv.ParseUint(string(tresp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 16)
		if err != nil {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return
		}

		q.leaderRxId = uint16(leaderId)
		return
	}

	q.rxLease = &LeaseItem{&ls, grantResp}
	q.leaderRxId = q.localRx.rxInfo.Id
	logrus.Infof("get leader rx: %d", q.leaderRxId)
}

//  private functional
func (q *Queue) getQueueInfo(data []byte) bool {
	q.queueInfo = new(QueueInfo)
	err := json.Unmarshal(data, q.queueInfo)
	if err != nil {
		return false
	}

	return true
}

func (q *Queue) getTxInfo(data []byte, txInfo *TransmitterInfo) bool {
	err := json.Unmarshal(data, txInfo)
	if err != nil {
		return false
	}

	return true
}

func (q *Queue) getRxInfo(data []byte, rxInfo *ReceiverInfo) bool {
	err := json.Unmarshal(data, rxInfo)
	if err != nil {
		return false
	}

	return true
}

func (q *Queue) addNewReceiver(rx *Receiver) {
	q.receivers[rx.rxInfo.Id] = rx
	if q.localTx != nil {
		q.localTx.handleReceiver(rx.rxInfo)
	}
}

// nak, ack with this thread
func (q *Queue) callTimer(nowTm int64) {
	if q.localRx != nil {
		q.localRx.handleTimer(nowTm)
	}

	if q.localTx != nil {
		q.localTx.handleTimer(nowTm)
	}
	q.status(nowTm)
}

// config change, connection check with this thread
func (q *Queue) configTimer(nowTm int64) {
	q.checkTxLeader(nowTm)
	oldLeaderId := q.leaderRxId
	if q.canCheckRxLeader {
		q.checkRxLeader(nowTm)
	}

	if oldLeaderId != q.leaderRxId {
		logrus.Infof("rx leader changed: %d, %d", oldLeaderId, q.leaderRxId)
		if q.localRx != nil {
			// q.localRx.leaderChange()
		}
	}
	// q.getAllReceivers(nowTm)
}

func (q *Queue) writeSeqId(seqId uint64) {
	if !q.IsHa() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	_, err := etcdClient.Put(ctx, q.seqIdPath, fmt.Sprintf("%d", seqId))
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", q.seqIdPath, err)
	}

	q.maxSeqId = seqId
}

func (q *Queue) getQueueSeqId() {
	if !q.IsHa() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, q.seqIdPath)
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", q.seqIdPath, err)
		return
	}

	if resp == nil || resp.Count == 0 || len(resp.Kvs) <= 0 {
		q.maxSeqId = 0
		return
	}

	// just use exists tx
	q.maxSeqId, err = strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return
	}

	logrus.Infof("backup get q.maxSeqId: %d\n", q.maxSeqId)
}

func (q *Queue) canRxSendSeqId() bool {
	if !q.IsHa() {
		return true
	}

	if q.leaderRxId != q.localRx.rxInfo.Id {
		return false
	}

	if q.maxSeqId == MaxUint64 {
		return false
	}

	return true
}

func (q *Queue) status(tm int64) {
	if q.prevStatusTmMilli+StatusTimePeriodMilli >= tm {
		return
	}

	q.prevStatusTmMilli = tm
	if q.localRx != nil {
		q.localRx.status()
	}

	if q.localTx != nil {
		q.localTx.status()
	}
}

func (q *Queue) createTx(cluster string) *Transmitter {
	if q.localTx != nil {
		return q.localTx
	}

	if cluster == "" {
		cluster = globalLocalIp + fmt.Sprintf("_%d", globalLocalPort)
	}

	txPath := q.txPath + cluster + "/" + globalLocalIp + fmt.Sprintf("_%d", globalLocalPort)
	txInfo := new(TransmitterInfo)
	existsTx := q.checkExistsTx(txPath, txInfo)
	if existsTx != nil {
		return existsTx
	}

	txInfo.Ip = globalLocalIp
	txInfo.Port = globalLocalPort
	removeTxId := false
	// save tx cluster id
	txClusterPath := q.txClusterPath + cluster
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, txClusterPath)
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", txPath, err)
		return nil
	}

	if resp != nil && resp.Count > 0 {
		if len(resp.Kvs) <= 0 {
			logrus.Errorf("get exists queue path res kv error: %s, %v", txPath, resp)
			return nil
		}

		txId, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 16)
		if err != nil {
			return nil
		}

		txInfo.Id = uint16(txId)
	} else {
		removeTxId = true
		txInfo.Id = q.mat.GetTxOrRxId()
		tresp, err := etcdClient.Txn(context.TODO()).
			If(etcd.Compare(etcd.Version(txClusterPath), "=", 0)).
			Then(etcd.OpPut(txClusterPath, fmt.Sprintf("%d", txInfo.Id))).
			Else(etcd.OpGet(txClusterPath)).
			Commit()
		if err != nil {
			logrus.Error(err)
			q.mat.RemoveTxOrRxId(txInfo.Id)
			return nil
		}

		if !tresp.Succeeded {
			q.mat.RemoveTxOrRxId(txInfo.Id)
			if len(tresp.Responses) <= 0 {
				logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
				return nil
			}

			if len(tresp.Responses[0].GetResponseRange().Kvs) <= 0 {
				logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
				return nil
			}

			txId, err := strconv.ParseUint(string(tresp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 16)
			if err != nil {
				return nil
			}

			txInfo.Id = uint16(txId)
		}
	}

	txInfo.QueueId = q.queueInfo.Id
	data, err := json.Marshal(txInfo)
	if err != nil {
		logrus.Errorf("create tx: %s failed, json dump failed!", txPath)
		return nil
	}

	tresp, err := etcdClient.Txn(context.TODO()).
		If(etcd.Compare(etcd.Version(txPath), "=", 0)).
		Then(etcd.OpPut(txPath, string(data))).
		Else(etcd.OpGet(txPath)).
		Commit()
	if err != nil {
		logrus.Error(err)
		if removeTxId {
			q.mat.RemoveTxOrRxId(txInfo.Id)
		}
		return nil
	}

	if !tresp.Succeeded {
		if removeTxId {
			q.mat.RemoveTxOrRxId(txInfo.Id)
		}

		if len(tresp.Responses) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if len(tresp.Responses[0].GetResponseRange().Kvs) <= 0 {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}

		if !q.getTxInfo(tresp.Responses[0].GetResponseRange().Kvs[0].Value, txInfo) {
			logrus.Errorf("get exists queçue path res kv error: %s, %v", q.queuePath, tresp)
			return nil
		}
	}

	tx := NewTransmitter(q, txInfo)
	key := fmt.Sprintf("%d_%s_%d", tx.txInfo.Id, tx.txInfo.Ip, tx.txInfo.Port)
	{
		q.txMutex.Lock()
		q.transmitters[key] = tx
		q.txMutex.Unlock()
	}
	q.localTx = tx
	return tx
}

func (q *Queue) checkExistsTx(txPath string, txInfo *TransmitterInfo) *Transmitter {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, txPath)
	cancel()
	if err != nil {
		logrus.Errorf("get queue failed, path: %s, err: %v", txPath, err)
		return nil
	}

	txInfo.AckedIndex = MaxUint64
	// just use exists tx
	if resp != nil && resp.Count > 0 {
		if len(resp.Kvs) <= 0 {
			logrus.Errorf("get exists queue path res kv error: %s, %v", txPath, resp)
			return nil
		}

		if !q.getTxInfo(resp.Kvs[0].Value, txInfo) {
			logrus.Errorf("get exists queue path res kv error: %s, %v", txPath, resp)
			return nil
		}

		tx := NewTransmitter(q, txInfo)
		key := fmt.Sprintf("%d_%s_%d", txInfo.Id, txInfo.Ip, txInfo.Port)
		{
			q.txMutex.Lock()
			q.transmitters[key] = tx
			q.txMutex.Unlock()
		}

		q.localTx = tx
		return tx
	}

	return nil
}
