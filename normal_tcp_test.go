package dbus

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func handler(conn net.Conn) {
	recv := make([]byte, 4096)

	for {
		n, err := conn.Read(recv)
		if err != nil {
			if err == io.EOF {
				fmt.Println("connection is closed from client : ", conn.RemoteAddr().String())
			}
			fmt.Println("Failed to receive data : ", err)
			break
		}

		if n > 0 {
			fmt.Println(string(recv[:n]))
			conn.Write(recv[:n])
		}
	}
}

func svrMain() {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("Failed to Listen : ", err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to Accept : ", err)
			continue
		}

		fmt.Printf("new connection coming.")
		go handler(conn)
	}
}

func clientMain() {
	conn, err := net.Dial("tcp", "127.0.0.1:8000")
	if err != nil {
		fmt.Println("Faield to Dial : ", err)
	}
	defer conn.Close()

	go func(c net.Conn) {
		send := "Hello"
		for {
			_, err = c.Write([]byte(send))
			if err != nil {
				fmt.Println("Failed to write data : ", err)
				break
			}

			time.Sleep(1 * time.Second)
		}
	}(conn)

	time.Sleep(time.Second * 2)
	go func(c net.Conn) {
		recv := make([]byte, 4096)

		for {
			n, err := c.Read(recv)
			if err != nil {
				fmt.Println("Failed to Read data : ", err)
				break
			}

			fmt.Println(string(recv[:n]))
		}
	}(conn)
}

func TestNormalTcp(t *testing.T) {
	// go svrMain()
	// time.Sleep(time.Second * 2)
	// clientMain()
	// time.Sleep(time.Second * 20000)
}
