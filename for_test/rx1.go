package main

import (
	"fmt"
	"github.com/google/gops/agent"
	dbus "git.amberainsider.com/metabase/dbus"
)

var (
	prevCountTmMilli int64  = dbus.TimeStampMilli()
	prevCount        uint64 = 0
)

func TmpRxCallback(msgId uint64, data []byte) int {
	nowTm := dbus.TimeStampMilli()
	if nowTm-prevCountTmMilli >= 1000 {
		fmt.Printf("qps: %d\n", (msgId-prevCount)*1000/uint64(nowTm-prevCountTmMilli))
		prevCountTmMilli = nowTm
		prevCount = msgId
	}

	return 0
}

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		fmt.Printf("failed")
	}
	// test etcd server: "82.156.224.174:2379"
	// db_path level db path example: "./rxdata"
	dbus.Init("127.0.0.1", 7892, "82.156.224.174:2379", "./rxdb1")
	// 修改queueName
	queue := dbus.CreateQueue("xl_q4", false, 3600, dbus.ProtoTypeTcp)
	queue.CreateReceiver(TmpRxCallback)
	select {}
}
