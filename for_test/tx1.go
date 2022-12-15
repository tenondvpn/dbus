package main

import (
	"encoding/binary"
	"time"

	dbus "git.amberainsider.com/metabase/dbus"
)

func main() {
	dbus.Init("127.0.0.1", 8002, "82.156.224.174:2379", "./txdata1")
	queue := dbus.CreateQueue("xl_q4", false, 3600, dbus.ProtoTypeTcp)
	tx := queue.CreateTransmitter("")
	time.Sleep(time.Millisecond * 100)
	data := make([]byte, 32)
	for i := 0; i < 1000000; i++ {
		binary.BigEndian.PutUint64(data, uint64(i))
		for {
			res := tx.Send(data)
			if res != 0 {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			break
		}
	//	time.Sleep(time.Millisecond * 1000)
	}

	select {}
}
