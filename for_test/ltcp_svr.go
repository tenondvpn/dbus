package main

import (
	"encoding/binary"
	"net"

	"dbus/dbus"
)

var (
	index uint32 = 0
)

func onMsgForTest(data []byte, conn *net.Conn) {
	binary.BigEndian.PutUint32(data[4:], uint32(1))
	(*conn).Write(data)
	// fmt.Printf("get data from client: %s\n", string(data[8:]))

}

func main() {
	svr := dbus.NewTcpServer(onMsgForTest)
	go svr.StartServer("127.0.0.1:8990")
	select {}
	// tcpClient := NewTcpClient(onMsgForTest)

	// tcpConnection = tcpClient.ConnectServer("127.0.0.1:8990")
	// if tcpConnection == nil {
	// 	fmt.Println("connect error.")
	// 	return
	// }

	// data := []byte("testsdadfasdfasdfadfd")
	// val := make([]byte, 12)
	// binary.BigEndian.PutUint32(val[0:], uint32(12+len(data)))
	// binary.BigEndian.PutUint32(val[4:], 0)
	// binary.BigEndian.PutUint32(val[8:], index)
	// index++
	// val = append(val, data...)
	// tcpClient.Send(tcpConnection, val)
	// time.Sleep(time.Second * 10000000)
	// tcpClient.Close(tcpConnection)
	// fmt.Println("close success")
}
