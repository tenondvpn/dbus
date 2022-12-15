// main.go
package main

import (
	"fmt"
	"net"
	// "time"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8000")
	if err != nil {
		fmt.Println("Faield to Dial : ", err)
	}
	defer conn.Close()

	// go func(c net.Conn) {
	send := "Hello"
	// for {
	_, err = conn.Write([]byte(send))
	if err != nil {
		fmt.Println("Failed to write data : ", err)
		return
		// break
	}

	// time.Sleep(1 * time.Second)
	// }
	// }(conn)

	go func(c net.Conn) {
		recv := make([]byte, 4096)
		receiveCount := int64(0)
		btime := TimeStampMilli()
		for {
			n, err := c.Read(recv)
			if err != nil {
				fmt.Println("Failed to Read data : ", err)
				break
			}

			if n > 0 {
				c.Write(recv[:n])
			}

			receiveCount++
			if receiveCount%10000 == 0 {
				etime := TimeStampMilli()
				fmt.Printf("receive count: %d, avg: %d\n", receiveCount, (receiveCount*1000)/(etime-btime))
			}
			// fmt.Println(string(recv[:n]))
		}
	}(conn)

	fmt.Scanln()
}
