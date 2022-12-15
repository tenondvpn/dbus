package main

import (
	"encoding/binary"
	"fmt"
	"time"
	"github.com/google/gops/agent"
	dbus "github.com/tenondvpn/dbus"
)

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		fmt.Printf("failed")
	}

	dbus.Init("127.0.0.1", 8001, "127.0.0.1:2379", "./txdata")
	queue := dbus.CreateQueue("xl_q4", false, 3600, dbus.ProtoTypeTcp)
	tx := queue.CreateTransmitter("")
	time.Sleep(time.Millisecond * 100)
	data := make([]byte, 32)
	for i := 0; i < 1000000; i++ {
		binary.BigEndian.PutUint64(data, uint64(i))
		for {
			res := tx.Send(data)
			if res != 0 {
				fmt.Printf("%d\n", i)
				time.Sleep(time.Millisecond * 1000)
				continue
			}

			break
		}
	//	time.Sleep(time.Millisecond * 1000)
	}

	select {}
}
