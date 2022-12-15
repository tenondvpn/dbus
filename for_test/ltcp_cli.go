package main

import (
	"encoding/binary"
	"fmt"
	"net"

	"dbus/dbus"
)

var (
	tcpConnection *dbus.EventItem
	index         uint32 = 0
)

func onMsgForTest(data []byte, conn *net.Conn) {
	index := binary.BigEndian.Uint32(data[8:])
	binary.BigEndian.PutUint32(data[4:], 0)
	binary.BigEndian.PutUint32(data[8:], index+1)
	// val = append(val, data...)
	(*conn).Write(data)
	if index%1000 == 0 {
		fmt.Printf("%d\n", index)
	}
}

func main() {
	tcpClient := dbus.NewTcpClient(onMsgForTest)

	tcpConnection = tcpClient.ConnectServer("127.0.0.1:8990")
	if tcpConnection == nil {
		fmt.Println("connect error.")
		return
	}

	data := []byte("testsdadfasdfasdfadfd")
	val := make([]byte, 12)
	binary.BigEndian.PutUint32(val[0:], uint32(12+len(data)))
	binary.BigEndian.PutUint32(val[4:], 0)
	binary.BigEndian.PutUint32(val[8:], index)
	index++
	val = append(val, data...)
	tcpClient.Send(tcpConnection, val)
	select {}
	tcpClient.Close(tcpConnection)
	fmt.Println("close success")
}
