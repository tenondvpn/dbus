package dbus

import (
	"fmt"
	"github.com/google/gops/agent"
	"testing"
	"time"
)

var (
	maxMsgId   uint64 = 0
	dataPrefix string = "sdfasdfasdf-"
)

func TmpRxCallback(msgId uint64, data []byte) int {
	if msgId%10000 == 0 {
		fmt.Println("receiver message coming.", msgId)
	}

	if string(data) != dataPrefix+fmt.Sprintf("%d", msgId) {
		fmt.Printf("failed: %s, %s\n", string(data), dataPrefix+fmt.Sprintf("%d", msgId))
		panic("error")
	}

	maxMsgId = msgId
	return 0
}

func TestInterface(t *testing.T) {
	if err := agent.Listen(agent.Options{}); err != nil {
		fmt.Printf("failed")
	}
	Init("127.0.0.1", 8001, "82.156.224.174:2379", "unitest")
	queue := CreateQueue("xl_test", false, 3600, ProtoTypeTcp)
	tx := queue.CreateTransmitter("cluster")
	queue.CreateReceiver(TmpRxCallback)
	time.Sleep(time.Millisecond * 100)
	const SendCount uint64 = 1000000
	for i := uint64(0); i < SendCount; i++ {
		for {
			res := tx.Send([]byte(dataPrefix + fmt.Sprintf("%d", i)))
			if res == 1 {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			break
		}

	}

	prvMsgId := uint64(0)
	for {
		if maxMsgId == SendCount-1 {
			break
		}
		if prvMsgId != maxMsgId {
			fmt.Printf("received max msg id: %d\n", maxMsgId)
			prvMsgId = maxMsgId
		}

		time.Sleep(time.Second * 1)
	}
}
