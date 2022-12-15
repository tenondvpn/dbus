// main.go
package main

import (
	"fmt"
	"io"
	"net"
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
			conn.Write(recv[:n])
		}
	}
}

func main() {
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

		go handler(conn)
	}
}
