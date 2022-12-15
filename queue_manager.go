package dbus

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/client/v3"
)

// watch all queue and thread to query all queues for update
type QueueManager struct {
	queues      map[uint16]*Queue
	queuesLock  sync.Mutex
	validQueues [SingleProcMaxQueues]*Queue
	queuesName  map[string]*Queue
}

func NewQueueManger() *QueueManager {
	queueMgr := new(QueueManager)
	queueMgr.queues = make(map[uint16]*Queue)
	queueMgr.queuesName = make(map[string]*Queue)
	go queueMgr.run()
	go queueMgr.watchQueues()
	return queueMgr
}

func (qm *QueueManager) CreateQueue(queueName string, ha bool, expireSecond int64, protoType int) *Queue {
	q := GetOrCreateQueue(queueName, ha, expireSecond, protoType)
	if q == nil {
		return nil
	}

	valid := false
	for i := 0; i < SingleProcMaxQueues; i++ {
		if qm.validQueues[i] == nil {
			qm.validQueues[i] = q
			valid = true
			break
		}
	}

	if !valid {
		return nil
	}

	qm.queuesLock.Lock()
	defer qm.queuesLock.Unlock()
	qm.queues[q.queueInfo.Id] = q
	qm.queuesName[queueName] = q
	return q
}

func (qm *QueueManager) RemoveQueue(id uint16) {
	for i := 0; i < SingleProcMaxQueues; i++ {
		if qm.validQueues[i] != nil && qm.validQueues[i].queueInfo.Id == id {
			qm.validQueues[i] = nil
			break
		}
	}

	qm.queuesLock.Lock()
	defer qm.queuesLock.Unlock()
	if q, ok := qm.queues[id]; ok {
		delete(qm.queuesName, q.queueInfo.QueueName)
		delete(qm.queues, id)
	}
}

func (qm *QueueManager) run() {
	for {
		qm.getAllQueuesFromEtcd()
		time.Sleep(time.Second * 3)
	}
}

// timing to refresh all queues
func (qm *QueueManager) getAllQueuesFromEtcd() {
	nowTm := TimeStampMilli()
	queues := make([]*Queue, 0)
	{
		qm.queuesLock.Lock()
		for _, q := range qm.queues {
			queues = append(queues, q)
		}
		qm.queuesLock.Unlock()
	}

	for _, q := range queues {
		qm.getQueueDetail(q)
		q.configTimer(nowTm)
	}
}

func (qm *QueueManager) getQueueDetail(q *Queue) {
	queuePath := QueuesPath + q.queueInfo.QueueName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := etcdClient.Get(ctx, queuePath, etcd.WithPrefix())
	cancel()
	if err != nil {
		logrus.Error(err)
	}

	for _, ev := range resp.Kvs {
		qm.handleRxAndTx(q.queueInfo, string(ev.Key), ev.Value)
	}
}

func (qm *QueueManager) handleRxAndTx(queueInfo *QueueInfo, key string, val []byte) {
	rxKey := QueuesPath + queueInfo.QueueName + "/rx/"
	txKey := QueuesPath + queueInfo.QueueName + "/tx/"
	if strings.HasPrefix(string(key), rxKey) {
		cfgMsg := new(ConfigMsg)
		cfgMsg.queueId = queueInfo.Id
		cfgMsg.msgType = TypeCfgNewRx
		cfgMsg.data = val
		msgHandler.callConfig(cfgMsg)
	}

	if strings.HasPrefix(string(key), txKey) {
		slitItems := strings.Split(string(key), "/")
		if len(slitItems) < 6 {
			return
		}

		cfgMsg := new(ConfigMsg)
		cfgMsg.queueId = queueInfo.Id
		cfgMsg.msgType = TypeCfgNewTx
		cfgMsg.txCluster = slitItems[5]
		cfgMsg.data = val
		msgHandler.callConfig(cfgMsg)
	}
}

func (qm *QueueManager) watchQueues() {
	rch := etcdClient.Watch(context.Background(), QueuesPath, etcd.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			slitItems := strings.Split(string(ev.Kv.Key), "/")
			if len(slitItems) < 5 {
				continue
			}

			var q *Queue = nil
			var ok bool = false
			{
				if q, ok = qm.queuesName[slitItems[3]]; !ok {
					continue
				}
			}

			qm.handleRxAndTx(q.queueInfo, string(ev.Kv.Key), ev.Kv.Value)
		}
	}
}

func (q *QueueManager) getRxInfo(data []byte, rxInfo *ReceiverInfo) bool {
	err := json.Unmarshal(data, rxInfo)
	if err != nil {
		return false
	}

	return true
}
