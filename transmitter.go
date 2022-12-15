package dbus

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	levigo "github.com/jmhodges/levigo"
	"github.com/sirupsen/logrus"
	lfqueue "github.com/smallnest/queue"
)

type Transmitter struct {
	receiversArray     [MaxReceivers]*Connection
	validReceiverArray [SingleQueueMaxReceivers]*Connection
	zkPath             string
	expireMilli        int64
	txInfo             *TransmitterInfo
	q                  *Queue
	prvTimer           int64
	resendChan         chan int
	cachedMsgs         map[uint64][]byte
	msgQueue           *lfqueue.LKQueue
	sendMsgCount       uint64
	resendMsgCount     uint64
	resendKey          []byte
}

func NewTransmitter(queue *Queue, txInfo *TransmitterInfo) *Transmitter {
	expireMilli := 0
	tx := Transmitter{
		zkPath:      QueuesPath + "/" + queue.queueInfo.QueueName,
		expireMilli: int64(expireMilli),
		txInfo:      txInfo,
		q:           queue,
	}

	for i := 0; i < MaxReceivers; i++ {
		tx.receiversArray[i] = nil
	}

	for i := 0; i < SingleQueueMaxReceivers; i++ {
		tx.validReceiverArray[i] = nil
	}

	tx.recoverTxInfo()
	tx.resendChan = make(chan int)
	if txInfo.MaxCachedMsgCount == 0 {
		txInfo.MaxCachedMsgCount = 300000
	}

	tx.cachedMsgs = make(map[uint64][]byte)
	tx.msgQueue = lfqueue.NewLKQueue()
	tx.resendKey = make([]byte, 14)
	binary.BigEndian.PutUint16(tx.resendKey[0:], tx.q.queueInfo.Id)
	binary.BigEndian.PutUint16(tx.resendKey[2:], tx.txInfo.Id)
	binary.BigEndian.PutUint16(tx.resendKey[4:], 0)
	go tx.resendThread()
	return &tx
}

// 14 bytes db key
func (tx *Transmitter) Send(data []byte) int {
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	key := make([]byte, 14)
	binary.BigEndian.PutUint16(key[0:], tx.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], tx.txInfo.Id)
	binary.BigEndian.PutUint16(key[4:], 0)
	binary.BigEndian.PutUint64(key[6:], uint64(tx.txInfo.MaxIndex))
	val := make([]byte, TxSaveHeadLen)
	directSend := 0
	if tx.txInfo.AckedIndex == MaxUint64 {
		if tx.txInfo.MaxIndex < uint64(tx.q.queueInfo.WindowSize) {
			directSend = 1
		}
	} else if tx.txInfo.AckedIndex+uint64(tx.q.queueInfo.WindowSize) > tx.txInfo.MaxIndex {
		directSend = 1
	}

	// directSend = 1
	binary.BigEndian.PutUint64(val[0:], uint64(tx.expireMilli))
	binary.BigEndian.PutUint32(val[8:], uint32(directSend))
	binary.BigEndian.PutUint32(val[12:], uint32(TxMsgHeadLen+len(data)))
	binary.BigEndian.PutUint16(val[16:], uint16(TypeTxMsg))
	binary.BigEndian.PutUint16(val[18:], uint16(tx.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[20:], uint16(tx.txInfo.Id))
	binary.BigEndian.PutUint16(val[22:], 0)
	binary.BigEndian.PutUint64(val[24:], uint64(tx.txInfo.MaxIndex))
	val = append(val, data...)
	if tx.sendMsgCount > tx.resendMsgCount && tx.sendMsgCount-tx.resendMsgCount >= tx.txInfo.MaxCachedMsgCount {
		return 1
	}

	if directSend != 1 {
		tx.sendMsgCount++
		tx.msgQueue.Enqueue(val)
	}

	tx.txInfo.MaxIndex++
	batch.Put([]byte(key), val)
	tx.saveTxInfo(batch)
	err := db.Write(dbWOption, batch)
	if err != nil {
		logrus.Fatalf("write to db failed")
		panic("write to db failed")
	}

	if directSend == 1 {
		// fmt.Printf("directSend message tx.txInfo.SentIndex: %d, realIdx: %d\n", tx.txInfo.resendMaxIndex, binary.BigEndian.Uint64(val[24:]))
		for _, conn := range tx.validReceiverArray {
			if conn != nil && conn.connectValid {
				if conn.cliEv == nil {
					continue
				}

				if !conn.syncedValid {
					continue
				}

				_, err := tcpClient.Send(conn.cliEv, val[TxSaveHeadOffsetLen:])
				if err != nil {
					tcpClient.Close(conn.cliEv)
					conn.cliEv = nil
					logrus.Errorf("direct send data failed: %s", err)
				}
			}
		}
		tx.txInfo.directSendCount++
	} else {
		// tx.resendChan <- 1
	}

	tx.txInfo.resendMaxIndex = tx.txInfo.MaxIndex
	return 0
}

// func (tx *Transmitter) reconnectDirect(conn *Connection) bool {
// 	if conn.txDirectCliEv != nil {
// 		tcpClient.Close(conn.txDirectCliEv)
// 	}

// 	evItem := tcpClient.ConnectServer(conn.endpoint.ip + ":" + fmt.Sprintf("%d", conn.endpoint.port))
// 	if evItem == nil {
// 		return false
// 	}

// 	conn.txDirectCliEv = evItem
// 	return true
// }

func (tx *Transmitter) checkConnections(nowtm int64) {
	for idx, conn := range tx.validReceiverArray {
		if conn != nil && conn.timeout+ConnectionTimeoutMilli > nowtm {
			continue
		}

		if conn == nil {
			continue
		}

		if conn.timeout == 0 {
			conn.timeout = nowtm
			continue
		}

		if conn.cliEv != nil {
			tcpClient.Close(conn.cliEv)
		}

		evItem := tcpClient.ConnectServer(conn.endpoint.ip + ":" + fmt.Sprintf("%d", conn.endpoint.port))
		if evItem == nil {
			tx.validReceiverArray[idx] = nil
			tx.receiversArray[conn.rxId] = nil
			conn.connectValid = false
			logrus.Infof("reconnect failed %s:%d", conn.endpoint.ip, conn.endpoint.port)
			continue
		}

		conn.connectValid = true
		conn.cliEv = evItem
		evItem.parentConn = conn
		conn.timeout = nowtm
		logrus.Infof("reconnect success %s:%d", conn.endpoint.ip, conn.endpoint.port)
		tx.sendHeartbeat(conn)
	}
}

func (tx *Transmitter) checkTimeout() {
	if tx.expireMilli <= 0 {
		return
	}

	nowtm := time.Now().Unix()
	for idx := tx.txInfo.MinIndex; idx < tx.txInfo.MaxIndex && idx < tx.txInfo.AckedIndex; idx++ {
		key := fmt.Sprintf("%d_%d", tx.q.queueInfo.Id, idx)
		data, err := db.Get(dbROption, []byte(key))
		if err != nil || len(data) <= 0 {
			fmt.Println("get data failed!")
			return
		}

		timeout := int64(binary.BigEndian.Uint64(data))
		if timeout+tx.expireMilli >= nowtm {
			err = db.Delete(dbWOption, []byte(key))
			if err != nil {
				return
			}

			tx.txInfo.MinIndex++
			tx.txInfo.dirty = true
		} else {
			return
		}
	}
}

func (tx *Transmitter) resendThread() {
	tiker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case _ = <-tx.resendChan:
			// fmt.Printf("tx.resendChan called: %d\n", tx.txInfo.AckedIndex)
			for {
				if !tx.resend() {
					break
				}
			}
		case <-tiker.C:
			tx.resend()
		}
	}
}

func (tx *Transmitter) getCachedMsg() {
	getCount := uint64(0)
	for {
		if getCount >= tx.txInfo.MaxCachedMsgCount {
			break
		}

		data := tx.msgQueue.Dequeue()
		if data == nil {
			break
		}

		tmpBuf, ok := data.([]byte)
		if !ok {
			panic("transfer data msg buffer failed!")
		}

		msgId := binary.BigEndian.Uint64(tmpBuf[24:])
		tx.cachedMsgs[msgId] = tmpBuf
		getCount++
	}
}

// func (tx *Transmitter) reconnectResend(conn *Connection) bool {
// 	if conn.txResendCliEv != nil {
// 		tcpClient.Close(conn.txResendCliEv)
// 	}

// 	evItem := tcpClient.ConnectServer(conn.endpoint.ip + ":" + fmt.Sprintf("%d", conn.endpoint.port))
// 	if evItem == nil {
// 		return false
// 	}

// 	conn.txResendCliEv = evItem
// 	return true
// }

func (tx *Transmitter) resend() bool {
	tx.getCachedMsg()
	ackedIndex := tx.txInfo.AckedIndex
	if ackedIndex == MaxUint64 {
		ackedIndex = 0
	}

	// fmt.Printf("resend message tx.txInfo.SentIndex: %d, ackedIndex: %d\n", tx.txInfo.SentIndex, ackedIndex)
	for idx := tx.txInfo.SentIndex; idx < tx.txInfo.resendMaxIndex && idx < ackedIndex+uint64(tx.q.queueInfo.WindowSize); idx++ {
		data, ok := tx.cachedMsgs[idx]
		var err error = nil
		if !ok {
			binary.BigEndian.PutUint64(tx.resendKey[6:], idx)
			data, err = db.Get(dbROption, tx.resendKey)
			if err != nil || len(data) <= 0 {
				logrus.Infof("resend get data failed: %d!", idx)
				return false
			}
		}

		directSend := int32(binary.BigEndian.Uint32(data[8:]))
		if directSend != 1 {
			for _, conn := range tx.validReceiverArray {
				if conn == nil {
					continue
				}

				if conn.cliEv == nil {
					continue
				}

				if !conn.syncedValid {
					continue
				}

				_, err := tcpClient.Send(conn.cliEv, data[TxSaveHeadOffsetLen:])
				if err != nil {
					logrus.Errorf("resend data failed: %s", err)
					tcpClient.Close(conn.cliEv)
					conn.cliEv = nil
					// } else {
					// 	fmt.Printf("real resend message tx.txInfo.SentIndex: %d, realIIdx: %d\n", idx, binary.BigEndian.Uint64(data[24:]))
				}
			}
		}

		tx.txInfo.SentIndex++
		if !tx.txInfo.dirty {
			tx.txInfo.dirty = true
		}

		if ok {
			delete(tx.cachedMsgs, idx)
			tx.resendMsgCount++
		}

		tx.txInfo.resendCount++
	}

	if tx.txInfo.dirty {
		batch := levigo.NewWriteBatch()
		defer batch.Close()
		tx.saveTxInfo(batch)
		err := db.Write(dbWOption, batch)
		if err != nil {
			panic("write db failed!")
		}
		tx.txInfo.dirty = false
		// logrus.Infof("resend success, acked: %d, sent: %d, max: %d", tx.txInfo.AckedIndex, tx.txInfo.SentIndex, tx.txInfo.MaxIndex)
		return true
	}
	return false
}

func (tx *Transmitter) sendHeartbeat(conn *Connection) {
	val := make([]byte, 20)
	binary.BigEndian.PutUint32(val[0:], uint32(AckMsgHeadLen))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeHearbeatRequest))
	binary.BigEndian.PutUint16(val[6:], uint16(tx.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(tx.txInfo.Id))
	binary.BigEndian.PutUint16(val[10:], 0)
	binary.BigEndian.PutUint64(val[12:], tx.txInfo.SentIndex)
	_, err := tcpClient.Send(conn.cliEv, val)
	if err != nil {
		logrus.Errorf("send heartbeat failed: %v", err)
		tcpClient.Close(conn.cliEv)
	}
}

func (tx *Transmitter) handleReceiver(rxInfo *ReceiverInfo) {
	txConn := tx.receiversArray[rxInfo.Id]
	if txConn != nil {
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
		txConn = &Connection{&endpoint, nil, 0, rxInfo.Id, rxInfo.AckedIndex, 0, false, 0, nil, false, false}
	}

	evItem := tcpClient.ConnectServer(txConn.endpoint.ip + ":" + fmt.Sprintf("%d", txConn.endpoint.port))
	if evItem == nil {
		logrus.Errorf("conenct receiver failed, %s:%d", txConn.endpoint.ip, txConn.endpoint.port)
		txConn.connectValid = false
		return
	}

	if tx.receiversArray[rxInfo.Id] == nil {
		for idx := 0; idx < SingleQueueMaxReceivers; idx++ {
			if tx.validReceiverArray[idx] == nil {
				tx.validReceiverArray[idx] = txConn
				break
			}
		}
	}

	tx.receiversArray[rxInfo.Id] = txConn
	logrus.Infof("conenct receiver success, %s:%d", txConn.endpoint.ip, txConn.endpoint.port)
	txConn.connectValid = true
	evItem.parentConn = txConn
	txConn.cliEv = evItem
	// remove timeout rx
	// for idx, conn := range tx.validReceiverArray {
	// 	if conn != nil {
	// 		if conn.timeout+RxTimeoutSeconds > time.Now().Unix() {
	// 			tx.validReceiverArray[idx] = nil
	// 			tx.receiversArray[conn.rxId] = nil
	// 			fmt.Printf()
	// 		}
	// 	}
	// }
}

func (tx *Transmitter) reconnectCli(conn *Connection) bool {
	if conn.cliEv != nil {
		tcpClient.Close(conn.cliEv)
	}

	evItem := tcpClient.ConnectServer(conn.endpoint.ip + ":" + fmt.Sprintf("%d", conn.endpoint.port))
	if evItem == nil {
		return false
	}

	conn.cliEv = evItem
	return true
}

// func (r *Transmitter) getClientConn(conn *net.Conn) *net.Conn {
// 	var cliConn *net.Conn = nil
// 	for i := 0; i < SingleQueueMaxReceivers; i++ {
// 		if r.validReceiverArray[i] == nil {
// 			continue
// 		}

// 		if r.validReceiverArray[i].cliEv == nil {
// 			if !r.reconnectCli(r.validReceiverArray[i]) {
// 				continue
// 			}
// 		}

// 		if r.validReceiverArray[i].cliEv != nil && &r.validReceiverArray[i].cliEv.conn == conn {
// 			cliConn = &r.validReceiverArray[i].cliEv.conn
// 			break
// 		}

// 		if r.validReceiverArray[i].txDirectCliEv != nil && &r.validReceiverArray[i].txDirectCliEv.conn == conn {
// 			cliConn = &r.validReceiverArray[i].cliEv.conn
// 			break
// 		}

// 		if r.validReceiverArray[i].txResendCliEv != nil && &r.validReceiverArray[i].txResendCliEv.conn == conn {
// 			cliConn = &r.validReceiverArray[i].cliEv.conn
// 			break
// 		}
// 	}

// 	return cliConn
// }

func (r *Transmitter) OnMsg(data []byte, conn *net.Conn) {
	// cliConn := r.getClientConn(conn)
	// if cliConn == nil {
	// 	return
	// }

	msgType := binary.BigEndian.Uint16(data[4:])
	switch msgType {
	case TypeAck:
		rxId := binary.BigEndian.Uint16(data[8:])
		msgId := binary.BigEndian.Uint64(data[12:])
		r.ack(msgId, rxId)
	case TypeHearbeatResponse:
		rxId := binary.BigEndian.Uint16(data[8:])
		msgId := binary.BigEndian.Uint64(data[12:])
		r.ack(msgId, rxId)
	case TypeNak:
		r.nak(data, conn)
	default:
		logrus.Errorf("tx invalid message type: %d", msgType)
	}

	r.handleTimer(TimeStampMilli())
}

func (tx *Transmitter) ack(msgIndex uint64, rxId uint16) {
	// logrus.Infof("tx ack called: %d, msgIndex: %d", tx.txInfo.AckedIndex, msgIndex)
	if tx.receiversArray[rxId] == nil {
		logrus.Errorf("invalid rx id: %d", rxId)
		return
	}

	// logrus.Infof("received ack rxId: %d, msgId: %d", rxId, msgIndex)
	tx.receiversArray[rxId].timeout = TimeStampMilli()
	tx.receiversArray[rxId].prevHeatbeatTimer = TimeStampMilli()
	if msgIndex == MaxUint64 || tx.receiversArray[rxId].ackedIndex > msgIndex {
		return
	}

	tx.receiversArray[rxId].ackedIndex = msgIndex
	if int(tx.txInfo.MinAckedCount) == 1 {
		if tx.txInfo.AckedIndex == MaxUint64 || tx.txInfo.AckedIndex < tx.receiversArray[rxId].ackedIndex {
			tx.txInfo.AckedIndex = tx.receiversArray[rxId].ackedIndex
		}

		return
	}

	txAckedIndex := tx.txInfo.AckedIndex
	if txAckedIndex == MaxUint64 {
		txAckedIndex = 0
	}

	minAckIdx := msgIndex
	h := &IntHeap{}
	heap.Init(h)
	// ackedIdxStr := ""
	for i := 0; i < SingleQueueMaxReceivers; i++ {
		if tx.validReceiverArray[i] == nil {
			continue
		}

		if !tx.validReceiverArray[i].syncedValid {
			if tx.validReceiverArray[i].ackedIndex+SyncToleranceRange > txAckedIndex {
				tx.validReceiverArray[i].syncedValid = true
			} else {
				continue
			}
		}

		if tx.validReceiverArray[i].ackedIndex < minAckIdx {
			minAckIdx = tx.validReceiverArray[i].ackedIndex
		}

		// ackedIdxStr += fmt.Sprintf("%d:%d:%v,", tx.validReceiverArray[i].rxId, tx.validReceiverArray[i].ackedIndex, tx.validReceiverArray[i].syncedValid)
		heap.Push(h, tx.validReceiverArray[i].ackedIndex)
	}

	if h.Len() <= 0 {
		return
	}

	// validCount := h.Len()
	// 0: 所有接受者确认，-1: 只要有一个确认即可，0 ～ 1：当前活跃节点比率个数, >= 1: 至少MinAckedCount个
	minValidCount := int(0)
	if tx.txInfo.MinAckedCount > 0 && tx.txInfo.MinAckedCount < 1 {
		minValidCount = int(tx.txInfo.MinAckedCount * float32(h.Len()))
	} else {
		minValidCount = int(tx.txInfo.MinAckedCount)
	}

	if minValidCount == 0 {
	} else if minValidCount == -1 || h.Len() == minValidCount {
		minAckIdx = (*h)[0]
	} else {
		if h.Len() < minValidCount {
			panic("invalid ack 0.")
			return
		}

		for i := 0; i < minValidCount-1; i++ {
			heap.Pop(h)
		}

		minAckIdx = (*h)[0]
	}

	if minAckIdx > txAckedIndex {
		tx.txInfo.AckedIndex = minAckIdx
	}
	// logrus.Infof("00000 valid receivers: %d, minValidCount: %d, ackedIndex: %d, minAckIdx: %d, acks: %s",
	// 	validCount, minValidCount, tx.txInfo.AckedIndex, minAckIdx, ackedIdxStr)
	tx.resendChan <- 1
	// logrus.Infof("11111 valid receivers: %d, minValidCount: %d, ackedIndex: %d, minAckIdx: %d, acks: %s",
	// 	validCount, minValidCount, tx.txInfo.AckedIndex, minAckIdx, ackedIdxStr)
}

func (tx *Transmitter) sendAllHeartbeat(tm int64) {
	for _, conn := range tx.validReceiverArray {
		if conn != nil && conn.cliEv != nil {
			if conn.prevHeatbeatTimer == 0 {
				conn.prevHeatbeatTimer = tm
			}

			if conn.prevHeatbeatTimer+TxHeartbeatPeriod < tm {
				tx.sendHeartbeat(conn)
				conn.prevHeatbeatTimer = tm
			}
		}
	}
}

func (tx *Transmitter) handleTimer(tm int64) {
	tx.sendAllHeartbeat(tm)
	// tx.resend()
	tx.checkConnections(tm)
}

// 6 bytes for tx send index
func (tx *Transmitter) recoverTxInfo() {
	key := make([]byte, 6)
	binary.BigEndian.PutUint16(key[0:], tx.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], tx.txInfo.Id)
	binary.BigEndian.PutUint16(key[4:], 0)
	val, err := db.Get(dbROption, key)
	if err != nil || len(val) <= 0 {
		//logrus.Infof("get data from db failed qid: %d, txId: %d!", tx.q.queueInfo.Id, tx.txInfo.Id)
		return
	}

	tx.txInfo.MinIndex = binary.BigEndian.Uint64(val[0:])
	tx.txInfo.MaxIndex = binary.BigEndian.Uint64(val[8:])
	tx.txInfo.SentIndex = binary.BigEndian.Uint64(val[16:])
	tx.txInfo.AckedIndex = binary.BigEndian.Uint64(val[24:])
	tx.txInfo.ValidBeginIndex = binary.BigEndian.Uint64(val[32:])
	tx.txInfo.resendMaxIndex = tx.txInfo.MaxIndex
}

func (tx *Transmitter) saveTxInfo(batch *levigo.WriteBatch) {
	key := make([]byte, 6)
	binary.BigEndian.PutUint16(key[0:], tx.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], tx.txInfo.Id)
	binary.BigEndian.PutUint16(key[4:], 0)
	val := make([]byte, 40)
	binary.BigEndian.PutUint64(val[0:], tx.txInfo.MinIndex)
	binary.BigEndian.PutUint64(val[8:], tx.txInfo.MaxIndex)
	binary.BigEndian.PutUint64(val[16:], tx.txInfo.SentIndex)
	binary.BigEndian.PutUint64(val[24:], tx.txInfo.AckedIndex)
	binary.BigEndian.PutUint64(val[32:], tx.txInfo.ValidBeginIndex)
	batch.Put(key, val)
}

func (tx *Transmitter) nak(data []byte, conn *net.Conn) {
	// max 10M then wait next nak
	count := binary.BigEndian.Uint16(data[10:])
	offset := 12
	nakSize := 0
	nakCount := 0
	key := make([]byte, 14)
	binary.BigEndian.PutUint16(key[0:], tx.q.queueInfo.Id)
	binary.BigEndian.PutUint16(key[2:], tx.txInfo.Id)
	binary.BigEndian.PutUint16(key[4:], 0)
	for i := uint16(0); i < count; i++ {
		min := binary.BigEndian.Uint64(data[offset:])
		max := binary.BigEndian.Uint64(data[offset+8:])
		if min > tx.txInfo.SentIndex {
			logrus.Warnf("resend by resend not nak, msgId: %d, tx.txInfo.SentIndex: %d", min, tx.txInfo.SentIndex)
			break
		}

		if max > tx.txInfo.SentIndex {
			max = tx.txInfo.SentIndex
		}

		offset += 16
		for msgId := min; msgId < max; msgId++ {
			binary.BigEndian.PutUint64(key[6:], msgId)
			data, err := db.Get(dbROption, key)
			if err != nil || len(data) <= 0 {
				fmt.Println("get data failed!")
				return
			}

			(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
			_, err = (*conn).Write(data[TxSaveHeadOffsetLen:])
			if err != nil {
				logrus.Errorf("write data failed: %v", err)
				(*conn).Close()
				break
			}

			nakSize += len(data)
			nakCount++
			if nakSize >= MaxNakBufferSize || nakCount >= MaxNakCount {
				tx.sendNakMore(conn)
				break
			}
		}
	}
}

func (tx *Transmitter) sendNakMore(conn *net.Conn) {
	val := make([]byte, 10)
	binary.BigEndian.PutUint32(val[0:], uint32(10))
	binary.BigEndian.PutUint16(val[4:], uint16(TypeNakMore))
	binary.BigEndian.PutUint16(val[6:], uint16(tx.q.queueInfo.Id))
	binary.BigEndian.PutUint16(val[8:], uint16(tx.txInfo.Id))
	(*conn).SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	(*conn).Write(val)
}

func (tx *Transmitter) status() {
	for i := 0; i < SingleQueueMaxReceivers; i++ {
		if tx.validReceiverArray[i] == nil {
			continue
		}

		fmt.Printf("i: %d, txId: %d, ackedIdx: %d, rxId: %d, ackedIdx: %d, sentIndex: %d, maxIndex: %d, direct: %d, resend: %d, cacheSend: %d, cacheResend: %d\n",
			i,
			tx.txInfo.Id,
			tx.txInfo.AckedIndex,
			tx.validReceiverArray[i].rxId,
			tx.validReceiverArray[i].ackedIndex,
			tx.txInfo.SentIndex,
			tx.txInfo.MaxIndex,
			tx.txInfo.directSendCount,
			tx.txInfo.resendCount,
			tx.sendMsgCount,
			tx.resendMsgCount)
	}
}
