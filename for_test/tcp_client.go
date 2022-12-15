package main

import (
	"dbus/dbus"
	"encoding/binary"
	"fmt"
	"time"
)

func main() {
	cli := dbus.NewTcpClient()
	conn := cli.ConnectServer("127.0.0.1:8990")
	if conn == nil {
		fmt.Println("connect error.")
		return
	}

	msg := []byte("testsdadfasdfasdfadfd")
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, uint32(len(msg)))
	val1 := append(val, msg...)
	cli.Send(conn, val1)
	time.Sleep(time.Second * 1)
	cli.Close(conn)
	fmt.Println("close success")
}
