package dbus

import (
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	queue "github.com/smallnest/queue"
)

type MessageHandler struct {
	threadCount int
	ioChans     []chan *QueueMsg
	cfgChans    []chan *ConfigMsg
	bufPool     [][]byte
}

func NewMessageHandler(threadCount int) *MessageHandler {
	handler := new(MessageHandler)
	handler.threadCount = threadCount
	handler.ioChans = make([]chan *QueueMsg, threadCount)
	handler.cfgChans = make([]chan *ConfigMsg, threadCount)

	for i := 0; i < threadCount; i++ {
		handler.ioChans[i] = make(chan *QueueMsg, IoPoolSize)
		handler.cfgChans[i] = make(chan *ConfigMsg, IoPoolSize)
		go handler.thredFunc(i)
	}

	return handler
}

func (h *MessageHandler) handleMessage(items []*EventItem, epoller *epoll, cb TcpCallback, bufQueue *queue.LKQueue) {
	for _, item := range items {
		if item == nil {
			break
		}

		for {
			n, buf, err := item.msgBuf.readFromReader()
			if err != nil {
				logrus.Errorf("failed to read data %v", err)
				if err := epoller.Remove(item); err != nil {
					logrus.Errorf("failed to remove %v", err)
				}
				item.conn.Close()
				break
			}

			if n == 0 {
				break
			}

			if n > 0 {
				qMsg := &QueueMsg{0, 0, buf, &item.conn, item.msgBuf, bufQueue}
				hold := cb(qMsg)
				data := bufQueue.Dequeue()
				if data == nil {
					panic("get data from queue failed!")
				}
				tmpBuf, ok := data.(*MsgBuffer)
				if !ok {
					panic("transfer data msg buffer failed!")
				}

				tmpBuf.maxLen = MaxPackageSize
				tmpBuf.pkgLen = 0
				tmpBuf.start = 0
				tmpBuf.reader = item.msgBuf.reader
				exchangeBuf := item.msgBuf
				item.msgBuf = tmpBuf
				if hold {
					bufQueue.Enqueue(exchangeBuf)
				}

				break
			}
		}
	}
}

func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (h *MessageHandler) onMsg(qMsg *QueueMsg) bool {
	qMsg.msgType = binary.BigEndian.Uint16(qMsg.data[4:6])
	qMsg.queueId = binary.BigEndian.Uint16(qMsg.data[6:8])
	// h.threadIoCallMsg(qMsg)
	// logrus.Infof("now in q: %d", GoID())
	h.ioChans[qMsg.queueId%uint16(h.threadCount)] <- qMsg
	// logrus.Infof("now ined q: %d", GoID())
	// queuManager.callTimer(TimeStampMilli())
	return false
}

func (h *MessageHandler) threadIoCallMsg(queueMsg *QueueMsg) {
	if queueMsg.msgType >= TypeTxMsg && queueMsg.msgType <= TypeSeqHearbeatResponse {
		h.onRxMessage(queueMsg.data, queueMsg.conn)
	} else if queueMsg.msgType >= TypeQueueIndex && queueMsg.msgType <= TypeHearbeatResponse {
		h.onTxMessage(queueMsg.data, queueMsg.conn)
	} else {
		logrus.Errorf("invalid msg type: %d", queueMsg.msgType)
	}

	queueMsg.bufQueue.Enqueue(queueMsg.msgBuf)
}

func (h *MessageHandler) callConfig(cfgMsg *ConfigMsg) {
	h.cfgChans[cfgMsg.queueId%uint16(h.threadCount)] <- cfgMsg
}

func (h *MessageHandler) threadCfgMsg(cfgMsg *ConfigMsg) {
	if cfgMsg.msgType == TypeCfgNewRx {
		queuManager.queues[cfgMsg.queueId].OnNewReceiver(cfgMsg.data)
	} else if cfgMsg.msgType == TypeCfgNewTx {
		queuManager.queues[cfgMsg.queueId].OnNewTransmitter(cfgMsg.txCluster, cfgMsg.data)
	} else {
		logrus.Errorf("invalid config type: %d", cfgMsg.msgType)
	}
}

func (h *MessageHandler) thredFunc(threadIdx int) {
	tiker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case msg := <-h.ioChans[threadIdx]:
			h.threadIoCallMsg(msg)
		case cfgMsg := <-h.cfgChans[threadIdx]:
			h.threadCfgMsg(cfgMsg)
		case <-tiker.C:
			for i := 0; i < SingleProcMaxQueues; i++ {
				if queuManager.validQueues[i] == nil {
					continue
				}

				qId := queuManager.validQueues[i].queueInfo.Id
				if qId%uint16(h.threadCount) == uint16(threadIdx) {
					queuManager.queues[qId].callTimer(TimeStampMilli())
				}

			}
		}
	}
}

func (h *MessageHandler) onRxMessage(data []byte, conn *net.Conn) {
	if len(data) < 8 {
		logrus.Errorf("invalid data len: %d", len(data))
		return
	}

	qId := binary.BigEndian.Uint16(data[6:])
	if queuManager.queues[qId] == nil {
		logrus.Errorf("invalid queue id: %d", qId)
		return
	}

	queuManager.queues[qId].localRx.OnMsg(data, conn)
}

func (h *MessageHandler) onTxMessage(data []byte, conn *net.Conn) {
	if len(data) < 8 {
		logrus.Errorf("invalid data len: %d", len(data))
		return
	}

	qId := binary.BigEndian.Uint16(data[6:])
	if queuManager.queues[qId] == nil {
		logrus.Errorf("invalid queue id: %d", qId)
		return
	}

	queuManager.queues[qId].localTx.OnMsg(data, conn)
}
