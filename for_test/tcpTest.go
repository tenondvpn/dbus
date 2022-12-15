package main

import (
	"dbus/dbus"
	"encoding/binary"
	"fmt"
	"net"
)

func TestStartServer() {
	dbus.StartServer("localhost:50002")
}

func TestClient() {
	conn, err := net.Dial("tcp", "localhost:50002")
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return // 终止程序
	}
	var headSize int
	var headBytes = make([]byte, 4)
	s := "hello world"
	content := []byte(s)
	headSize = len(content)
	binary.BigEndian.PutUint32(headBytes, uint32(headSize))
	conn.Write(headBytes)
	conn.Write(content)

	s = "hello gohelhello gohello gohehello gohello gohello gohello gohello gohello gollo gohello gohello gohello golo gohehello gohello gohello gohello gohello gohello gollo gohello gohello gohello go"
	content = []byte(s)
	headSize = len(content)
	binary.BigEndian.PutUint32(headBytes, uint32(headSize))
	conn.Write(headBytes)
	conn.Write(content)

	s = "hello tcp"
	content = []byte(s)
	headSize = len(content)
	binary.BigEndian.PutUint32(headBytes, uint32(headSize))
	conn.Write(headBytes)
	conn.Write(content)
}

func main() {
	TestStartServer()
}
