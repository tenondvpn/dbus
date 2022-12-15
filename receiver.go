package dbus

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	levigo "github.com/jmhodges/levigo"
	"github.com/sirupsen/logrus"
)

type Receiver struct {
	rxInfo                  *ReceiverInfo
	q                       *Queue
	cb                      ReceiverCallback
	ackedInfo               [MaxTransmitters]*TxAckedInfo
	queueMsgInfo            *QueueMessageInfo
	validTxInfo             [SingleQueueMaxTransmitters]*TxAckedInfo
	receiversArray          [MaxReceivers]*Connection
	validReceiverArray      [SingleQueueMaxReceivers]*Connection
	prvTimer                int64
	prvCheckSeqNakTimer     int64
	prvCleanTxLimitMapTimer int64
	prvSaveQueueTimeMilli   int64
	isSynedSeqId            bool
	rxDbBatch               *levigo.WriteBatch
	rxDbBatchDirty          bool
	rxCacheSeqIndex         map[uint64][]byte
	rxCacheTxMsgs           map[string]string
	seqInfo                 *SequenceIdInfo
	validFollower           int
	syncSequeceIdBuffer     []byte
}

func CreateNewReceiver(q *Queue, rxInfo *ReceiverInfo, cb ReceiverCallback) *Receiver {
	rx := &Receiver{rxInfo: rxInfo, q: q, cb: cb}
	for i := 0; i < MaxTransmitters; i++ {
		rx.ackedInfo[i] = nil
	}

	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		rx.validTxInfo[i] = nil
	}

	for i := 0; i < MaxReceivers; i++ {
		rx.receiversArray[i] = nil
	}

	for i := 0; i < SingleQueueMaxReceivers; i++ {
		rx.validReceiverArray[i] = nil
	}

	rx.recoverQueueMessage()
	rx.recoverSeqInfo()
	rx.rxCacheSeqIndex = make(map[uint64][]byte)
	rx.rxCacheTxMsgs = make(map[string]string)
	rx.syncSequeceIdBuffer = make([]byte, MaxPackageSize)
	rx.rxDbBatch = levigo.NewWriteBatch()
	return rx
}

func (r *Receiver) OnMsg(data []byte, conn *net.Conn) {
	nowTimeMilli := TimeStampMilli()
	msgType := binary.BigEndian.Uint16(data[4:])
	switch msgType {
	case TypeTxMsg:
		r.txValid(data, conn)
		r.handleTxMsg(data, conn, nowTimeMilli)
	case TypeHearbeatRequest:
		r.txValid(data, conn)
		r.handleHeartbeat(data, conn, nowTimeMilli)
	case TypeNakMore:
		r.txValid(data, conn)
		txId := binary.BigEndian.Uint16(data[8:])
		r.nak(txId)
	case TypeSeqId:
		r.rxValid(data, conn)
		r.handleSeqId(data, conn, nowTimeMilli)
	case TypeSeqIdAck:
		r.rxValid(data, conn)
		r.handleSeqIdAck(data, conn, nowTimeMilli)
	case TypeSeqHearbeatRequest:
		r.rxValid(data, conn)
		r.handleSeqHeartbeatReq(data, conn, nowTimeMilli)
	case TypeSeqHearbeatResponse:
		r.rxValid(data, conn)
		r.handleSeqIdAck(data, conn, nowTimeMilli)
	case TypeSeqIdNak:
		r.rxValid(data, conn)
		r.handleSeqIdNakReq(data, conn, nowTimeMilli)
	case TypeSeqIdNakRes:
		r.rxValid(data, conn)
		r.handleSeqIdNakRes(data, conn, nowTimeMilli)
	case TypeSeqIdNakMore:
		r.rxValid(data, conn)
		r.checkSeqIdNak(nowTimeMilli)
	default:
		logrus.Errorf("rx invalid message type: %d", msgType)
	}

	r.checkCallback()
}

func (r *Receiver) removeReceiver() {
	if r.rxDbBatch != nil {
		r.rxDbBatch.Close()
	}
}

func (r *Receiver) handleNewTransmitter(txInfo *TransmitterInfo) {
	if r.ackedInfo[txInfo.Id] != nil {
		return
	}

	ackInfo := new(TxAckedInfo)
	ackInfo.receivedIndex = txInfo.AckedIndex
	ackInfo.increasedIndex = MaxUint64
	ackInfo.queueLimitTxIndex = make(map[uint64]uint64)
	ackInfo.queueLimitTxIndex[0] = 0
	if txInfo.AckedIndex != MaxUint64 {
		ackInfo.ackedIndex = txInfo.AckedIndex
		ackInfo.receivedMaxIndex = txInfo.AckedIndex
	}

	ackInfo.txId = txInfo.Id
	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		if r.validTxInfo[i] == nil {
			r.validTxInfo[i] = ackInfo
			logrus.Infof("add valid tx index: %d, txId: %d\n", i, txInfo.Id)
			break
		}
	}

	r.recoverTxInfo(ackInfo)
	logrus.Infof("add transmitter success: %d", txInfo.Id)
	r.ackedInfo[txInfo.Id] = ackInfo
}

func (r *Receiver) txValid(data []byte, conn *net.Conn) {
	txId := binary.BigEndian.Uint16(data[8:])
	if r.ackedInfo[txId] == nil {
		txInfo := new(TransmitterInfo)
		txInfo.Id = txId
		txInfo.AckedIndex = MaxUint64
		r.handleNewTransmitter(txInfo)
		// logrus.Errorf("invalid tx message coming.[%d]", txId)
		// return false
	}

	r.ackedInfo[txId].conn = conn
}

func (r *Receiver) rxValid(data []byte, conn *net.Conn) {
	rxId := binary.BigEndian.Uint16(data[8:])
	if r.receiversArray[rxId] == nil {
		rxInfo := new(ReceiverInfo)
		rxInfo.Id = rxId
		rxInfo.AckedIndex = MaxUint64
		txConn := &Connection{nil, nil, time.Now().Unix(), rxInfo.Id, rxInfo.AckedIndex, 0, false, 0, conn, false, false}
		for idx, validConn := range r.validReceiverArray {
			if validConn == nil {
				r.validReceiverArray[idx] = txConn
				r.validFollower++
				logrus.Infof("add new receiver: %d, %d", idx, rxInfo.Id)
				break
			}
		}

		r.receiversArray[rxId] = txConn
		logrus.Errorf("new rx message coming.[%d]", rxId)
	}

}

// 12 bytes db key
func (r *Receiver) handleTxMsg(data []byte, conn *net.Conn, nowTimeMilli int64) {
	qId := binary.BigEndian.Uint16(data[6:])
	txId := binary.BigEndian.Uint16(data[8:])
	msgId := binary.BigEndian.Uint64(data[12:])
	key := make([]byte, 12)
	binary.BigEndian.PutUint16(key[0:], qId)
	binary.BigEndian.PutUint16(key[2:], txId)
	binary.BigEndian.PutUint64(key[4:], msgId)
	dbBatch := levigo.NewWriteBatch()
	defer dbBatch.Close()
	dbBatch.Put(key, data[20:])
	if msgId > r.ackedInfo[txId].receivedMaxIndex {
		r.ackedInfo[txId].receivedMaxIndex = msgId + 1
	}

	if r.ackedInfo[txId].receivedIndex == MaxUint64 {
		if msgId == 0 {
			r.ackedInfo[txId].receivedIndex = msgId
			r.increaseQueueIndex(txId, msgId, dbBatch)
			r.checkReceivedIndex(txId, msgId+1, dbBatch)
		}
	} else {
		if msgId == r.ackedInfo[txId].receivedIndex+1 {
			r.ackedInfo[txId].receivedIndex = msgId
			r.increaseQueueIndex(txId, msgId, dbBatch)
			r.checkReceivedIndex(txId, msgId+1, dbBatch)
		}
	}

	// fmt.Printf("msgId: %d, r.ackedInfo[txId].ackedIndex: %d, r.q.queueInfo.WindowSize: %d, r.ackedInfo[txId].receivedIndex: %d, valid window: %d, r.ackedInfo[txId].receivedMaxIndex: %d\n",
	// 	msgId, r.ackedInfo[txId].ackedIndex, r.q.queueInfo.WindowSize, r.ackedInfo[txId].receivedIndex,
	// 	r.ackedInfo[txId].ackedIndex+uint64(r.q.queueInfo.WindowSize/4) < r.ackedInfo[txId].receivedIndex,
	// 	r.ackedInfo[txId].receivedMaxIndex)
	if r.q.queueInfo.AckRatio == -1 ||
		r.ackedInfo[txId].ackedIndex+uint64(r.q.queueInfo.WindowSize/4) < r.ackedInfo[txId].receivedIndex ||
		r.ackedInfo[txId].prevAckedTime+AckTimeoutMilli < nowTimeMilli {
		// fmt.Printf("r.ackedInfo[txId].ackedIndex: %d, r.ackedInfo[txId].receivedIndex: %d\n", r.ackedInfo[txId].ackedIndex, r.ackedInfo[txId].receivedIndex)
		r.ack(txId, conn, nowTimeMilli)
	}

	// save to db
	err := db.Write(dbWOption, dbBatch)
	if err != nil {
		logrus.Fatalf("write db failed: %v", err)
		panic("write db error!")
	}

	r.rxCacheTxMsgs[Bytes2Str(key)] = string(data[20:]) // copy data
}

func (r *Receiver) checkReceivedIndex(txId uint16, msgId uint64, batch *levigo.WriteBatch) {
	if msgId == r.ackedInfo[txId].receivedMaxIndex {
		return
	}

	key := make([]byte, 12)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], txId)
	for i := msgId; i < r.ackedInfo[txId].receivedMaxIndex; i++ {
		binary.BigEndian.PutUint64(key[4:], i)
		val, err := db.Get(dbROption, key)
		if err != nil || val == nil || len(val) <= 0 {
			break
		}

		r.ackedInfo[txId].receivedIndex = i
		// fmt.Printf("checkReceivedIndex get receivedIndex: %d\n", r.ackedInfo[txId].receivedIndex)
	}

	r.increaseQueueIndex(txId, r.ackedInfo[txId].receivedIndex, batch)
}

func (r *Receiver) checkNak(tm int64) {
	for i := 0; i < MaxTransmitters; i++ {
		if r.ackedInfo[i] != nil {
			if r.ackedInfo[i].prevNakedTime+NakTimeoutMilli < tm {
				r.nak(uint16(i))
			}
		}
	}
}

func (r *Receiver) handleTimer(tm int64) {
	r.checkCallback()
	r.checkNak(tm)
	r.sendAllSeqHeartbeat(tm)
	r.checkConnections(tm)
	r.clearQueueLimitIndex(tm)
	if r.prvCheckSeqNakTimer+3000 < tm {
		r.checkSeqIdNak(tm)
	}

	if r.prvSaveQueueTimeMilli+3000 < tm {
		for i := 0; i < SingleQueueMaxTransmitters; i++ {
			if r.validTxInfo[i] != nil {
				r.saveTxInfo(r.validTxInfo[i].txId, r.rxDbBatch)
			}
		}

		r.saveQueueMessage(r.rxDbBatch)
		r.saveSeqInfo(r.rxDbBatch)
		r.prvSaveQueueTimeMilli = tm
		r.rxDbBatchDirty = true
	}

	if r.rxDbBatchDirty {
		err := db.Write(dbWOption, r.rxDbBatch)
		if err != nil {
			logrus.Fatalf("write db failed: %v", err)
		}

		r.rxDbBatch.Clear()
		r.rxDbBatchDirty = false
	}
}

func (r *Receiver) handleHeartbeat(data []byte, conn *net.Conn, nowTimeMilli int64) {
	txId := binary.BigEndian.Uint16(data[8:])
	if r.ackedInfo[txId] == nil {
		return
	}

	r.ackedInfo[txId].conn = conn
	msgId := binary.BigEndian.Uint64(data[12:])
	r.ackedInfo[txId].prevAckedTime = nowTimeMilli
	r.ackedInfo[txId].receivedMaxIndex = msgId
	r.ackedInfo[txId].hbCount++
	val := make([]byte, 20)
	binary.BigEndian.PutUint32(val[0:], uint32(AckMsgHeadLen))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeHearbeatResponse))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], 0)
	if r.ackedInfo[txId].receivedIndex == MaxUint64 {
		binary.BigEndian.PutUint64(val[12:], MaxUint64)
	} else {
		binary.BigEndian.PutUint64(val[12:], r.ackedInfo[txId].receivedIndex+1)
	}

	(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*conn).Write(val)
}

func (r *Receiver) ack(txId uint16, conn *net.Conn, nowTm int64) {
	val := make([]byte, 20)
	ackedIdx := r.ackedInfo[txId].receivedIndex
	if r.ackedInfo[txId].receivedIndex == MaxUint64 {
		binary.BigEndian.PutUint64(val[12:], 0)
	} else {
		if r.q.IsHa() && r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize) < r.queueMsgInfo.queueIndex {
			ackedIdx = r.ackedInfo[txId].queueLimitTxIndex[r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize)]
		}

		binary.BigEndian.PutUint64(val[12:], ackedIdx+1)
	}

	if r.ackedInfo[txId].ackedIndex+uint64(DefaultTxWindowSize)/4 > ackedIdx {
		return
	}

	binary.BigEndian.PutUint32(val[0:], 20)
	binary.BigEndian.PutUint16(val[4:], uint16(TypeAck))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], 0)
	r.ackedInfo[txId].ackedIndex = ackedIdx
	(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*conn).Write(val)
	r.ackedInfo[txId].prevAckedTime = nowTm
	// logrus.Infof("rx ack called: %d", r.ackedInfo[txId].receivedIndex)
	r.ackedInfo[txId].ackCount++
}

func (r *Receiver) nak(txId uint16) {
	r.ackedInfo[txId].prevNakedTime = TimeStampMilli()
	if r.ackedInfo[txId] == nil || r.ackedInfo[txId].conn == nil {
		return
	}

	key := make([]byte, 12)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], txId)
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	r.checkReceivedIndex(txId, r.ackedInfo[txId].receivedIndex+1, batch)
	err := db.Write(dbWOption, batch)
	if err != nil {
		panic("write db failed!")
	}

	idx := r.ackedInfo[txId].receivedIndex + 1
	if r.ackedInfo[txId].receivedIndex == 0 {
		idx = 0
	}

	// nakedIds := make([]uint64, 0)
	type NakItem struct {
		min uint64
		max uint64
	}

	nakItems := make([]*NakItem, 0)
	var item *NakItem = nil
	for {
		if idx >= r.ackedInfo[txId].receivedMaxIndex {
			break
		}

		binary.BigEndian.PutUint64(key[4:], idx)
		_, ok := r.rxCacheTxMsgs[Bytes2Str(key)]
		if !ok {
			if item == nil {
				item = new(NakItem)
				item.min = idx
			}
		} else {
			if item != nil {
				item.max = idx
				nakItems = append(nakItems, item)
				if len(nakItems) >= MaxNakCount {
					break
				}
				item = nil
			}
		}

		// val, err := db.Get(dbROption, key)
		// if err != nil || val == nil || len(val) <= 0 {
		// 	if item == nil {
		// 		item = new(NakItem)
		// 		item.min = idx
		// 	}
		// } else {
		// 	if item != nil {
		// 		item.max = idx
		// 		nakItems = append(nakItems, item)
		// 		if len(nakItems) >= MaxNakCount {
		// 			break
		// 		}
		// 		item = nil
		// 	}
		// }

		idx++
	}

	if item != nil {
		if item.max == 0 {
			item.max = r.ackedInfo[txId].receivedMaxIndex
			nakItems = append(nakItems, item)
		}
	}

	if len(nakItems) <= 0 {
		return
	}

	val := make([]byte, 12+len(nakItems)*2*8)
	binary.BigEndian.PutUint32(val[0:], uint32(12+len(nakItems)*2*8))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeNak))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], uint16(len(nakItems)))
	offset := 12
	for _, tmpItem := range nakItems {
		binary.BigEndian.PutUint64(val[offset:], tmpItem.min)
		binary.BigEndian.PutUint64(val[offset+8:], tmpItem.max)
		offset += 16
	}

	(*r.ackedInfo[txId].conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*r.ackedInfo[txId].conn).Write(val)
	r.ackedInfo[txId].nakCount++
}

func (r *Receiver) checkCallback() {
	for {
		maxIndex := r.queueMsgInfo.queueIndex
		if r.q.IsHa() {
			maxIndex = r.seqInfo.AckedSeqId
		}

		if r.queueMsgInfo.calledIndex < maxIndex {
			val, ok1 := r.rxCacheSeqIndex[r.queueMsgInfo.calledIndex]
			var err error
			if !ok1 {
				key := make([]byte, 10)
				binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
				binary.BigEndian.PutUint64(key[2:], r.queueMsgInfo.calledIndex)
				val, err = db.Get(dbROption, key)
				if err != nil || len(val) <= 0 {
					logrus.Errorf("get data failed queue: %d_%d", r.q.queueInfo.Id, r.queueMsgInfo.calledIndex)
					break
				}
			}

			var data []byte
			tmp_str, ok := r.rxCacheTxMsgs[Bytes2Str(val)]
			if !ok {
				data, err = db.Get(dbROption, val)
				if err != nil || len(val) <= 0 {
					logrus.Errorf("get data failed queue: %d_%d", r.q.queueInfo.Id, r.queueMsgInfo.calledIndex)
					break
				}
			} else {
				data = Str2Bytes(tmp_str)
			}

			status := r.cb(r.queueMsgInfo.calledIndex, data)
			if status != 0 {
				break
			}

			if ok1 {
				delete(r.rxCacheSeqIndex, r.queueMsgInfo.calledIndex)
			}

			if ok {
				delete(r.rxCacheTxMsgs, Bytes2Str(val))
			}

			r.queueMsgInfo.calledIndex++
			if !r.queueMsgInfo.dirty {
				r.queueMsgInfo.dirty = true
			}
		} else {
			break
		}
	}
}

func (r *Receiver) recoverTxInfo(ackInfo *TxAckedInfo) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[0:], r.rxInfo.Id)
	binary.BigEndian.PutUint16(key[2:], ackInfo.txId)
	value, err := db.Get(dbROption, key)
	if err != nil || len(value) <= 0 {
		logrus.Errorf("get data failed: %d %d", r.rxInfo.Id, ackInfo.txId)
		return
	}

	ackInfo.receivedIndex = binary.BigEndian.Uint64(value)
	ackInfo.ackedIndex = binary.BigEndian.Uint64(value[8:])
	ackInfo.receivedMaxIndex = binary.BigEndian.Uint64(value[16:])
	ackInfo.increasedIndex = binary.BigEndian.Uint64(value[24:])
	if ackInfo.receivedMaxIndex == MaxUint64 {
		ackInfo.receivedMaxIndex = 0
	}
}

// 4 bytes db key
func (r *Receiver) saveTxInfo(txId uint16, batch *levigo.WriteBatch) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[0:], r.rxInfo.Id)
	binary.BigEndian.PutUint16(key[2:], txId)
	val := make([]byte, 32)
	binary.BigEndian.PutUint64(val[0:], uint64(r.ackedInfo[txId].receivedIndex))
	binary.BigEndian.PutUint64(val[8:], uint64(r.ackedInfo[txId].ackedIndex))
	binary.BigEndian.PutUint64(val[16:], uint64(r.ackedInfo[txId].receivedMaxIndex))
	binary.BigEndian.PutUint64(val[24:], uint64(r.ackedInfo[txId].increasedIndex))
	batch.Put(key, val)
}

// 2 bytes db key
func (r *Receiver) saveQueueMessage(batch *levigo.WriteBatch) {
	if !r.queueMsgInfo.dirty {
		return
	}

	key := make([]byte, 2)
	binary.BigEndian.PutUint16(key, r.q.queueInfo.Id)
	val := make([]byte, 24)
	binary.BigEndian.PutUint64(val, r.queueMsgInfo.queueIndex)
	binary.BigEndian.PutUint64(val[8:], r.queueMsgInfo.syncQueueIndex)
	binary.BigEndian.PutUint64(val[16:], r.queueMsgInfo.calledIndex)
	batch.Put(key, val)
}

func (r *Receiver) recoverQueueMessage() {
	r.queueMsgInfo = new(QueueMessageInfo)
	key := make([]byte, 2)
	binary.BigEndian.PutUint16(key, r.q.queueInfo.Id)
	val, err := db.Get(dbROption, key)
	if err != nil || len(val) <= 0 {
		return
	}

	if len(val) != 24 {
		return
	}
	r.queueMsgInfo.queueIndex = binary.BigEndian.Uint64(val)
	r.queueMsgInfo.syncQueueIndex = binary.BigEndian.Uint64(val[8:])
	r.queueMsgInfo.calledIndex = binary.BigEndian.Uint64(val[16:])
	logrus.Infof("queue: %d, q.queueMsgInfo.queueIndex: %d, q.queueMsgInfo.syncQueueIndex: %d, r.queueMsgInfo.calledIndex: %d",
		r.q.queueInfo.Id, r.queueMsgInfo.queueIndex, r.queueMsgInfo.syncQueueIndex, r.queueMsgInfo.calledIndex)
}

// 10 bytes db key
func (r *Receiver) increaseQueueIndex(txId uint16, msgId uint64, batch *levigo.WriteBatch) {
	if !r.q.canRxSendSeqId() || msgId >= r.ackedInfo[txId].receivedMaxIndex {
		return
	}

	startIdx := r.ackedInfo[txId].increasedIndex + 1
	if r.ackedInfo[txId].increasedIndex == MaxUint64 {
		startIdx = 0
	}

	for idx := startIdx; idx <= msgId; idx++ {
		r.ackedInfo[txId].increasedIndex = idx
		key := make([]byte, 10)
		binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
		binary.BigEndian.PutUint64(key[2:], r.queueMsgInfo.queueIndex)
		val := make([]byte, 12)
		binary.BigEndian.PutUint16(val[0:], r.q.queueInfo.Id)
		binary.BigEndian.PutUint16(val[2:], txId)
		binary.BigEndian.PutUint64(val[4:], idx)
		batch.Put(key, val)
		r.queueMsgInfo.dirty = true
		r.rxCacheSeqIndex[r.queueMsgInfo.queueIndex] = val
		r.sendSeqId()
		r.ackedInfo[txId].queueLimitTxIndex[r.queueMsgInfo.queueIndex] = idx
		if r.queueMsgInfo.queueIndex != 0 {
			for i := 0; i < SingleQueueMaxTransmitters; i++ {
				if r.validTxInfo[i] == nil {
					continue
				}

				if r.validTxInfo[i].txId == txId {
					continue
				}

				r.ackedInfo[txId].queueLimitTxIndex[r.queueMsgInfo.queueIndex] = r.ackedInfo[txId].queueLimitTxIndex[r.queueMsgInfo.queueIndex-1]
			}
		}

		r.queueMsgInfo.queueIndex++
	}
}

func (r *Receiver) clearQueueLimitIndex(tm int64) {
	if r.prvCleanTxLimitMapTimer+3000 >= tm {
		return
	}

	r.prvCleanTxLimitMapTimer = tm
	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		if r.validTxInfo[i] == nil {
			continue
		}

		for qidx := r.validTxInfo[i].queueLimitStartIndex; qidx < r.seqInfo.AckedSeqId; qidx++ {
			delete(r.validTxInfo[i].queueLimitTxIndex, qidx)
		}

		r.validTxInfo[i].queueLimitStartIndex = r.seqInfo.AckedSeqId
	}
}

func (r *Receiver) sendAllSeqHeartbeat(tm int64) {
	if r.q.leaderRxId != r.rxInfo.Id {
		return
	}

	for _, conn := range r.validReceiverArray {
		if conn != nil && conn.cliEv != nil {
			if conn.prevHeatbeatTimer == 0 {
				conn.prevHeatbeatTimer = tm
			}

			if conn.prevHeatbeatTimer+TxHeartbeatPeriod < tm {
				r.sendSeqHeartbeat(conn)
				conn.prevHeatbeatTimer = tm
			}
		}
	}
}

func (r *Receiver) sendSeqHeartbeat(conn *Connection) {
	val := make([]byte, 20)
	binary.BigEndian.PutUint32(val[0:], uint32(20))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeSeqHearbeatRequest))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], 0)
	binary.BigEndian.PutUint64(val[12:], r.queueMsgInfo.queueIndex)
	_, err := tcpClient.Send(conn.cliEv, val)
	if err != nil {
		logrus.Errorf("send heartbeat faled: %s", err)
		tcpClient.Close(conn.cliEv)
		conn.cliEv = nil
	}
}

func (r *Receiver) handleSeqHeartbeatReq(data []byte, conn *net.Conn, nowTimeMilli int64) {
	rxId := binary.BigEndian.Uint16(data[8:])
	if r.q.leaderRxId != rxId {
		return
	}

	qIdx := binary.BigEndian.Uint64(data[12:])
	if r.seqInfo.MaxSeqId < qIdx {
		r.seqInfo.MaxSeqId = qIdx
	}

	val := make([]byte, 18)
	binary.BigEndian.PutUint32(val[0:], 18)
	binary.BigEndian.PutUint16(val[4:], TypeSeqHearbeatResponse)
	binary.BigEndian.PutUint16(val[6:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint16(val[8:], r.rxInfo.Id)
	binary.BigEndian.PutUint64(val[10:], r.queueMsgInfo.queueIndex)
	r.seqInfo.AckedSeqId = r.queueMsgInfo.queueIndex
	(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*conn).Write(val)
}

func (r *Receiver) sendSeqId() {
	if !r.q.IsHa() {
		return
	}

	if r.validFollower <= 0 {
		return
	}

	if r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize) < r.seqInfo.SentSeqId {
		return
	}

	binary.BigEndian.PutUint32(r.syncSequeceIdBuffer[0:], 28)
	binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[4:], TypeSeqId)
	binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[6:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[8:], r.rxInfo.Id)
	binary.BigEndian.PutUint64(r.syncSequeceIdBuffer[10:], r.seqInfo.SentSeqId)

	key := make([]byte, 10)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	count := 0
	offset := 20
	oldTxId := uint16(0)
	txId := uint16(0)
	msgId := MaxUint64
	oldMsgId := MaxUint64
	i := r.seqInfo.SentSeqId
	for ; i < r.queueMsgInfo.queueIndex; i++ {
		item, ok := r.rxCacheSeqIndex[i]
		if !ok {
			binary.BigEndian.PutUint64(key[2:], i)
			val, err := db.Get(dbROption, key)
			if err != nil {
				break
			}

			txId = binary.BigEndian.Uint16(val[2:])
			msgId = binary.BigEndian.Uint64(val[4:])
		} else {
			txId = binary.BigEndian.Uint16(item[2:])
			msgId = binary.BigEndian.Uint64(item[4:])
		}

		if oldTxId == 0 {
			oldTxId = txId
			oldMsgId = msgId
			binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[offset:], oldTxId)
			binary.BigEndian.PutUint64(r.syncSequeceIdBuffer[offset+2:], oldMsgId)
		}

		if txId != oldTxId {
			binary.BigEndian.PutUint64(r.syncSequeceIdBuffer[offset+10:], oldMsgId)
			count++
			if int64(count) >= DefaultSequenceWindowSize {
				txId = 0
				break
			}

			offset += 18
			binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[offset:], txId)
			binary.BigEndian.PutUint64(r.syncSequeceIdBuffer[offset+2:], msgId)
			oldMsgId = msgId
			txId = oldTxId
		}
	}

	if msgId == MaxUint64 {
		return
	}

	if txId != 0 {
		count++
		binary.BigEndian.PutUint64(r.syncSequeceIdBuffer[offset+10:], msgId)
	}

	// fmt.Printf("%d direct send seqid: %d, %d\n", TimeStampMilli(), r.seqInfo.SentSeqId, i)
	r.seqInfo.SentSeqId = i
	packageLen := uint32(offset + 18)
	binary.BigEndian.PutUint32(r.syncSequeceIdBuffer[0:], packageLen)
	binary.BigEndian.PutUint16(r.syncSequeceIdBuffer[18:], uint16(count))
	for _, conn := range r.validReceiverArray {
		if conn != nil && conn.connectValid /*&& conn.seqIdSyncedValid*/ {
			_, err := tcpClient.Send(conn.cliEv, r.syncSequeceIdBuffer[0:packageLen])
			if err != nil {
				logrus.Errorf("send seqid faled: %s", err)
				tcpClient.Close(conn.cliEv)
			}
		}
	}
}

func (r *Receiver) handleSeqId(data []byte, conn *net.Conn, nowTimeMilli int64) {
	if !r.q.IsHa() {
		return
	}

	rxId := binary.BigEndian.Uint16(data[8:])
	if r.q.leaderRxId != rxId || r.rxInfo.Id == rxId {
		return
	}

	count := binary.BigEndian.Uint16(data[18:])
	beginQueueIdx := binary.BigEndian.Uint64(data[10:])
	offset := 20
	for i := uint16(0); i < count; i++ {
		txId := binary.BigEndian.Uint16(data[offset:])
		beginMsgId := binary.BigEndian.Uint64(data[offset+2:])
		endMsgId := binary.BigEndian.Uint64(data[offset+10:])
		// fmt.Printf("%d direct get seq info begin queue index: %d, msgBegin: %d, msgEnd: %d, tx: %d\n", TimeStampMilli(), beginQueueIdx, beginMsgId, endMsgId, txId)
		for msgId := beginMsgId; msgId <= endMsgId; msgId++ {
			key := make([]byte, 10)
			binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
			binary.BigEndian.PutUint64(key[2:], beginQueueIdx)
			val := make([]byte, 12)
			binary.BigEndian.PutUint16(val[0:], r.q.queueInfo.Id)
			binary.BigEndian.PutUint16(val[2:], txId)
			binary.BigEndian.PutUint64(val[4:], msgId)
			r.rxDbBatch.Put(key, val)
			r.rxCacheSeqIndex[beginQueueIdx] = val
			r.rxDbBatchDirty = true
			r.queueMsgInfo.dirty = true
			if r.seqInfo.MaxSeqId < beginQueueIdx {
				r.seqInfo.MaxSeqId = beginQueueIdx
			}

			if beginQueueIdx == r.queueMsgInfo.queueIndex+1 {
				r.queueMsgInfo.queueIndex = beginQueueIdx
			}

			beginQueueIdx++
		}

		offset += 18
	}

	r.sendSeqIdAck(conn, nowTimeMilli)
	r.sendAckToTxs(nowTimeMilli)
}

func (r *Receiver) sendSeqIdAck(conn *net.Conn, nowTimeMilli int64) {
	if r.q.queueInfo.SeqAckRatio == -1 ||
		r.seqInfo.AckedSeqId+uint64(r.q.queueInfo.WindowSize/4) < r.queueMsgInfo.queueIndex ||
		r.seqInfo.prevSeqAckedTime+AckTimeoutMilli < nowTimeMilli {
		val := make([]byte, 18)
		binary.BigEndian.PutUint32(val[0:], 18)
		binary.BigEndian.PutUint16(val[4:], TypeSeqIdAck)
		binary.BigEndian.PutUint16(val[6:], r.q.queueInfo.Id)
		binary.BigEndian.PutUint16(val[8:], r.rxInfo.Id)
		binary.BigEndian.PutUint64(val[10:], r.queueMsgInfo.queueIndex)
		r.seqInfo.AckedSeqId = r.queueMsgInfo.queueIndex
		(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
		(*conn).Write(val)
		r.seqInfo.prevSeqAckedTime = nowTimeMilli
		r.seqInfo.ackedCount++
		// fmt.Printf("%d, ack seq r.queueMsgInfo.queueIndex: %d\n", TimeStampMilli(), r.queueMsgInfo.queueIndex)
	}
}

func (r *Receiver) handleSeqIdAck(data []byte, conn *net.Conn, nowTimeMilli int64) {
	if !r.q.IsHa() || r.q.leaderRxId != r.rxInfo.Id {
		return
	}

	rxId := binary.BigEndian.Uint16(data[8:])
	if r.receiversArray[rxId] == nil {
		return
	}

	r.receiversArray[rxId].timeout = nowTimeMilli
	r.receiversArray[rxId].ackedIndex = binary.BigEndian.Uint64(data[10:])
	// fmt.Printf("leader rxId: %d, lr.receiversArray[rxId].ackedIndex: %d, r.queueMsgInfo.queueIndex: %d\n", rxId, r.receiversArray[rxId].ackedIndex, r.queueMsgInfo.queueIndex)
	if int(r.q.queueInfo.SeqAckRatio) == 1 {
		if r.seqInfo.AckedSeqId < r.receiversArray[rxId].ackedIndex {
			r.seqInfo.AckedSeqId = r.receiversArray[rxId].ackedIndex
		}

		return
	}

	minAckIdx := r.receiversArray[rxId].ackedIndex
	h := &IntHeap{}
	heap.Init(h)
	for i := 0; i < SingleQueueMaxReceivers; i++ {
		if r.validReceiverArray[i] == nil || !r.validReceiverArray[i].connectValid {
			continue
		}

		if r.validReceiverArray[i].ackedIndex < minAckIdx {
			minAckIdx = r.validReceiverArray[i].ackedIndex
		}

		heap.Push(h, r.validReceiverArray[i].ackedIndex)
	}

	if h.Len() <= 0 {
		r.seqInfo.AckedSeqId = minAckIdx
		return
	}

	// -1: 所有接受者确认，0: 只要有一个确认即可，0 ～ 1：当前活跃节点比率个数, >= 1: 至少SeqAckRatio个
	minValidCount := int(0)
	if r.q.queueInfo.SeqAckRatio > 0 && r.q.queueInfo.SeqAckRatio < 1 {
		minValidCount = int(r.q.queueInfo.SeqAckRatio * float32(h.Len()))
	} else {
		minValidCount = int(r.q.queueInfo.SeqAckRatio)
	}

	if minValidCount == -1 {
		r.seqInfo.AckedSeqId = minAckIdx
	} else if minValidCount == 0 || h.Len() == minValidCount {
		r.seqInfo.AckedSeqId = (*h)[0]
	} else {
		for i := 0; i < minValidCount-1; i++ {
			heap.Pop(h)
		}

		r.seqInfo.AckedSeqId = (*h)[0]
	}
	// fmt.Printf("%d, handle ack seq r.seqInfo.AckedSeqId: %d, queueIndex: %d\n", TimeStampMilli(), r.seqInfo.AckedSeqId, r.queueMsgInfo.queueIndex)
	r.sendSeqId()
	r.sendAckToTxs(nowTimeMilli)
}

func (r *Receiver) sendAckToTxs(nowTm int64) {
	if r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize/4) < r.queueMsgInfo.queueIndex {
		return
	}

	val := make([]byte, 20)
	binary.BigEndian.PutUint32(val[0:], 20)
	binary.BigEndian.PutUint16(val[4:], uint16(TypeAck))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], 0)
	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		if r.validTxInfo[i] == nil || r.validTxInfo[i].conn == nil {
			continue
		}

		ackIndex := r.validTxInfo[i].receivedIndex
		if r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize) < r.queueMsgInfo.queueIndex {
			ackIndex = uint64(r.validTxInfo[i].queueLimitTxIndex[r.seqInfo.AckedSeqId+uint64(DefaultSequenceWindowSize)])
		}

		if r.validTxInfo[i].ackedIndex+uint64(DefaultTxWindowSize)/4 > ackIndex {
			continue
		}

		binary.BigEndian.PutUint64(val[12:], ackIndex+1)
		r.validTxInfo[i].ackedIndex = ackIndex
		(*r.validTxInfo[i].conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
		(*r.validTxInfo[i].conn).Write(val)
		r.validTxInfo[i].prevAckedTime = nowTm
		r.validTxInfo[i].ackCount++
	}
}

func (r *Receiver) checkSeqIdNak(tm int64) {
	r.prvCheckSeqNakTimer = tm
	if r.q.leaderRxId == r.rxInfo.Id {
		return
	}

	if r.receiversArray[r.q.leaderRxId] == nil || r.receiversArray[r.q.leaderRxId].conn == nil {
		return
	}

	// nakedIds := make([]uint64, 0)
	type NakItem struct {
		min uint64
		max uint64
	}

	nakItems := make([]*NakItem, 0)
	var item *NakItem = nil
	for i := r.queueMsgInfo.queueIndex; i < r.seqInfo.MaxSeqId; i++ {
		_, ok := r.rxCacheSeqIndex[i]
		if !ok {
			if item == nil {
				item = new(NakItem)
				item.min = i
			}
		} else {
			if item != nil {
				item.max = i
				nakItems = append(nakItems, item)
				if len(nakItems) >= MaxNakCount {
					break
				}
				item = nil
			}
		}
	}

	if item != nil {
		if item.max == 0 {
			item.max = r.seqInfo.MaxSeqId
			nakItems = append(nakItems, item)
		}
	}

	if len(nakItems) <= 0 {
		return
	}

	val := make([]byte, 12+len(nakItems)*2*8)
	binary.BigEndian.PutUint32(val[0:], uint32(12+len(nakItems)*2*8))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeSeqIdNak))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	binary.BigEndian.PutUint16(val[10:], uint16(len(nakItems)))
	offset := 12
	for _, tmpItem := range nakItems {
		binary.BigEndian.PutUint64(val[offset:], tmpItem.min)
		binary.BigEndian.PutUint64(val[offset+8:], tmpItem.max)
		offset += 16
	}

	(*r.receiversArray[r.q.leaderRxId].conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*r.receiversArray[r.q.leaderRxId].conn).Write(val)
	// fmt.Printf("%d handle nak: %d, %d, %d, %s\n", TimeStampMilli(), len(nakItems), nakItems[0].min, nakItems[0].max, "")
}

func (r *Receiver) handleSeqIdNakReq(data []byte, conn *net.Conn, nowTimeMilli int64) {
	if r.q.leaderRxId != r.rxInfo.Id {
		return
	}

	count := binary.BigEndian.Uint16(data[10:])
	offset := 12
	nakCount := 0
	key := make([]byte, 10)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	for i := uint16(0); i < count; i++ {
		min := binary.BigEndian.Uint64(data[offset:])
		max := binary.BigEndian.Uint64(data[offset+8:])
		// fmt.Printf("%d, ddd rx nak seq: %d, %d\n", TimeStampMilli(), min, max)
		if min > r.queueMsgInfo.queueIndex {
			logrus.Warnf("resend by resend not nak, msgId: %d, tx.txInfo.SentIndex: %d", min, r.queueMsgInfo.queueIndex)
			return
		}

		if max > r.queueMsgInfo.queueIndex {
			max = r.queueMsgInfo.queueIndex
		}

		offset += 16
		for seqId := min; seqId < max; seqId++ {
			binary.BigEndian.PutUint64(key[2:], seqId)
			data, err := db.Get(dbROption, key)
			if err != nil || len(data) <= 0 {
				fmt.Println("get data failed!")
				return
			}

			val := make([]byte, 28)
			binary.BigEndian.PutUint32(val[0:], 28)
			binary.BigEndian.PutUint16(val[4:], TypeSeqIdNakRes)
			binary.BigEndian.PutUint16(val[6:], r.q.queueInfo.Id)
			binary.BigEndian.PutUint16(val[8:], r.rxInfo.Id)
			copy(val[10:], data[2:])
			binary.BigEndian.PutUint64(val[20:], seqId)
			(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
			_, err = (*conn).Write(val)
			if err != nil {
				logrus.Errorf("write data failed!")
				return
			}

			nakCount++
			if nakCount >= MaxNakCount {
				r.sendSeqNakMore(conn)
				return
			}
		}
	}
}

func (r *Receiver) handleSeqIdNakRes(data []byte, conn *net.Conn, nowTimeMilli int64) {
	if !r.q.IsHa() {
		return
	}

	rxId := binary.BigEndian.Uint16(data[8:])
	if r.q.leaderRxId != rxId || r.rxInfo.Id == rxId {
		return
	}

	txId := binary.BigEndian.Uint16(data[10:])
	msgId := binary.BigEndian.Uint64(data[12:])
	qIdx := binary.BigEndian.Uint64(data[20:])
	if qIdx == r.queueMsgInfo.queueIndex+1 {
		r.queueMsgInfo.queueIndex = qIdx
	}

	// all reset by leader, keep consistence
	key := make([]byte, 10)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint64(key[2:], qIdx)
	val := make([]byte, 12)
	binary.BigEndian.PutUint16(val[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint16(val[2:], txId)
	binary.BigEndian.PutUint64(val[4:], msgId)
	// fmt.Printf("get seq id: %d, %d, %d\n", qIdx, txId, msgId)
	r.rxDbBatch.Put(key, val)
	r.rxCacheSeqIndex[qIdx] = val
	r.rxDbBatchDirty = true
	r.queueMsgInfo.dirty = true
}

func (r *Receiver) sendSeqNakMore(conn *net.Conn) {
	val := make([]byte, 10)
	binary.BigEndian.PutUint32(val[0:], uint32(10))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeSeqIdNakMore))
	binary.BigEndian.PutUint16(val[6:], uint16(r.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(r.rxInfo.Id))
	(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*conn).Write(val)
}

func (r *Receiver) removeTx(txId uint16) {
	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		if r.validTxInfo[i] != nil && r.validTxInfo[i] == r.ackedInfo[txId] {
			r.validTxInfo[i] = nil
		}
	}

	r.ackedInfo[txId] = nil
}

func (r *Receiver) leaderChange() {
	// for i := 0; i < MaxReceivers; i++ {
	// 	if r.receiversArray[i] != nil {
	// 		if r.receiversArray[i].cliEv != nil {
	// 			tcpClient.Close(r.receiversArray[i].cliEv)
	// 			r.receiversArray[i].cliEv = nil
	// 		}

	// 		r.receiversArray[i] = nil
	// 	}
	// }

	for i := 0; i < SingleQueueMaxReceivers; i++ {
		if r.validReceiverArray[i] != nil {
			r.validReceiverArray[i].conn = nil
			r.validReceiverArray[i].endpoint = nil
			if r.validReceiverArray[i].cliEv != nil {
				tcpClient.Close(r.validReceiverArray[i].cliEv)
				r.validReceiverArray[i].cliEv.conn = nil
				r.validReceiverArray[i].cliEv = nil
			}

			r.receiversArray[r.validReceiverArray[i].rxId] = nil
			r.validReceiverArray[i] = nil
		}
	}
}

func (r *Receiver) handleReceiver(rxInfo *ReceiverInfo) {
	if !r.q.IsHa() {
		return
	}

	if r.q.leaderRxId != r.rxInfo.Id {
		return
	}

	if rxInfo.Id == r.rxInfo.Id {
		return
	}

	txConn := r.receiversArray[rxInfo.Id]
	if txConn != nil && txConn.endpoint != nil {
		if txConn.endpoint.ip == rxInfo.Ip && txConn.endpoint.port == rxInfo.Port {
			return
		}

		txConn.endpoint.ip = rxInfo.Ip
		txConn.endpoint.port = rxInfo.Port
		if rxInfo.AckedIndex > txConn.ackedIndex {
			txConn.ackedIndex = rxInfo.AckedIndex
		}
	} else {
		endpoint := EndPoint{rxInfo.Ip, rxInfo.Port}
		txConn = &Connection{&endpoint, nil, time.Now().Unix(), rxInfo.Id, rxInfo.AckedIndex, 0, false, 0, nil, false, false}
	}

	evItem := tcpClient.ConnectServer(txConn.endpoint.ip + ":" + fmt.Sprintf("%d", txConn.endpoint.port))
	if evItem == nil {
		logrus.Errorf("1 conenct receiver failed, %s:%d", txConn.endpoint.ip, txConn.endpoint.port)
		txConn.connectValid = false
	} else {
		txConn.connectValid = true
		evItem.parentConn = txConn
	}

	txConn.cliEv = evItem
	txConn.timeout = TimeStampMilli()
	if r.receiversArray[rxInfo.Id] == nil {
		for idx, validConn := range r.validReceiverArray {
			if validConn == nil {
				r.validReceiverArray[idx] = txConn
				r.validFollower++
				logrus.Infof("add new receiver: %d, %d", idx, rxInfo.Id)
				break
			}
		}
	}

	r.receiversArray[rxInfo.Id] = txConn
}

func (r *Receiver) checkConnections(nowtm int64) {
	for idx, conn := range r.validReceiverArray {
		if conn != nil && conn.timeout+ConnectionTimeoutMilli > nowtm {
			continue
		}

		if conn == nil {
			continue
		}

		if conn.endpoint == nil {
			continue
		}

		evItem := tcpClient.ConnectServer(conn.endpoint.ip + ":" + fmt.Sprintf("%d", conn.endpoint.port))
		if evItem == nil {
			r.validReceiverArray[idx] = nil
			r.validFollower--
			r.receiversArray[conn.rxId] = nil
			logrus.Errorf("CheckConnections connect failed. %d, %v", idx, conn)
			conn.connectValid = false
			continue
		}

		conn.connectValid = true
		evItem.parentConn = conn
		conn.cliEv = evItem
		conn.timeout = nowtm
		logrus.Infof("connect receiver success. %d, %v", idx, conn)
	}
}

func (r *Receiver) status() {
	for i := 0; i < SingleQueueMaxTransmitters; i++ {
		if r.validTxInfo[i] != nil {
			fmt.Printf("i: %d, txId: %d, ackedIndex: %d, receivedIndex: %d, increasedIndex: %d, receivedMaxIndex: %d, qId: %d, qIndex: %d, qCalledIndex: %d, nak: %d, ack: %d, hb: %d, MaxSeqId: %d, seqAckCount: %d\n",
				i,
				r.validTxInfo[i].txId,
				r.validTxInfo[i].ackedIndex,
				r.validTxInfo[i].receivedIndex,
				r.validTxInfo[i].increasedIndex,
				r.validTxInfo[i].receivedMaxIndex,
				r.q.queueInfo.Id,
				r.queueMsgInfo.queueIndex,
				r.queueMsgInfo.calledIndex,
				r.validTxInfo[i].nakCount,
				r.validTxInfo[i].ackCount,
				r.validTxInfo[i].hbCount,
				r.seqInfo.MaxSeqId,
				r.seqInfo.ackedCount)
		}
	}
}

func (r *Receiver) recoverSeqInfo() {
	r.seqInfo = new(SequenceIdInfo)
	key := make([]byte, 6)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint32(key[2:], 0)
	value, err := db.Get(dbROption, key)
	if err != nil || len(value) <= 0 {
		logrus.Errorf("get data failed: %d", r.q.queueInfo.Id)
		return
	}

	r.seqInfo.MaxSeqId = binary.BigEndian.Uint64(value)
	r.seqInfo.CalledSeqId = binary.BigEndian.Uint64(value[8:])
	logrus.Infof("receiver qx: %d, MaxSeqId: %d, CalledSeqId: %d",
		r.q.queueInfo.Id, r.seqInfo.MaxSeqId, r.seqInfo.CalledSeqId)
}

// 6 bytes db key
func (r *Receiver) saveSeqInfo(batch *levigo.WriteBatch) {
	key := make([]byte, 6)
	binary.BigEndian.PutUint16(key[0:], r.q.queueInfo.Id)
	binary.BigEndian.PutUint32(key[2:], 0)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[0:], uint64(r.seqInfo.MaxSeqId))
	binary.BigEndian.PutUint64(val[8:], uint64(r.seqInfo.CalledSeqId))
	batch.Put(key, val)
}
