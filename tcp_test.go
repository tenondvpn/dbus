package dbus

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

var (
	tcpConnection *EventItem
	index         int64 = 0
	btime         int64 = time.Now().UnixMicro()
	ttime         int64 = time.Now().UnixMicro()
)

func onMsgForTest(data []byte, conn *net.Conn, msgBuf *MsgBuffer) bool {
	t := binary.BigEndian.Uint32(data[4:8])
	if t == 0 { // from client
		binary.BigEndian.PutUint32(data[4:], uint32(1))
		(*conn).Write(data)
	} else {
		index++
		etime := time.Now().UnixMicro()
		if etime-btime >= 3000000 {
			fmt.Printf("all: %d, avg: %d\n", index, index*1000000/(etime-ttime))
			btime = etime
		}

		// index := binary.BigEndian.Uint32(data[8:])
		// binary.BigEndian.PutUint32(data[4:], 0)
		// binary.BigEndian.PutUint32(data[8:], index+1)
		// tcpConnection.conn.Write(data)
	}

	return true
}

func TestTcp(t *testing.T) {
	// Init("127.0.0.1", 8001, "82.156.224.174:2379")
	// svr := NewTcpServer(onMsgForTest)
	// go svr.StartServer("127.0.0.1:8990")
	// time.Sleep(time.Second * 1)
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
	// binary.BigEndian.PutUint32(val[8:], uint32(index))
	// index++
	// val = append(val, data...)
	// for i := 0; i < 10000000; i++ {
	// 	tcpClient.Send(tcpConnection, val)
	// 	if i%50000 == 0 {
	// 		time.Sleep(time.Second)
	// 	}
	// }
	// time.Sleep(time.Second * 10000000)
	// tcpClient.Close(tcpConnection)
	// fmt.Println("close success")
}
