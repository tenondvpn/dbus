// main.go
package main

import (
	"dbus/dbus"
	"fmt"
	"net"
)

func SvrTcpCallback(data []byte, conn *net.Conn) {
	(*conn).Write(data)
}

func main() {
	dbus.Init("127.0.0.1", 8002, "82.156.224.174:2379")
	fmt.Println("new server call")
	svr := dbus.NNewTcpServer(SvrTcpCallback)
	fmt.Println("new server called, now start")
	svr.StartServer("127.0.0.1:8990")
	fmt.Println("new server called, now started")
}
