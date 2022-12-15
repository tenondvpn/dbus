package main

import (
	"dbus/dbus"
	"fmt"
)

func main() {
	fmt.Println("new server call")
	svr := dbus.NewTcpServer()
	fmt.Println("new server called, now start")
	svr.StartServer("127.0.0.1:8990")
	fmt.Println("new server called, now started")

}
