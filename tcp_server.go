package dbus

import (
	"fmt"
	"net"
	"syscall"

	"github.com/sirupsen/logrus"
	queue "github.com/smallnest/queue"
)

type TcpServer struct {
	epoller  *epoll
	cb       TcpCallback
	bufQueue *queue.LKQueue
}

func NewTcpServer(cb TcpCallback) *TcpServer {
	epoller, err := NMkEpoll()
	if err != nil {
		panic(err)
	}

	bufQueue := queue.NewLKQueue()
	for i := 0; i < BuffPoolSize; i++ {
		buf := new(MsgBuffer)
		buf.buf = make([]byte, MaxPackageSize)
		bufQueue.Enqueue(buf)
	}
	svr := &TcpServer{epoller, cb, bufQueue}
	return svr
}

func (svr *TcpServer) StartServer(ipsec string) {
	setLimit()
	ln, err := net.Listen("tcp", ipsec)
	if err != nil {
		panic(err)
	}

	go svr.start()
	for {
		conn, e := ln.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				fmt.Printf("accept temp err: %v\n", ne)
				continue
			}

			logrus.Errorf("accept err: %v\n", e)
			return
		}

		_, err := svr.epoller.Add(conn)
		if err != nil {
			logrus.Errorf("failed to add connection %v\n", err)
			conn.Close()
		}
		logrus.Infof("success add connection %v\n", conn)
	}
}

func (svr *TcpServer) start() {
	for {
		items, err := svr.epoller.Wait()
		if err != nil {
			logrus.Errorf("failed to epoll wait %v\n", err)
			continue
		}

		if len(items) <= 0 {
			continue
		}

		msgHandler.handleMessage(items, svr.epoller, svr.cb, svr.bufQueue)
	}
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
}
