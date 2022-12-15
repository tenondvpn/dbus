package dbus

import (
	"errors"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	queue "github.com/smallnest/queue"
)

type TcpClient struct {
	epoller  *epoll
	cb       TcpCallback
	bufQueue *queue.LKQueue
}

func NewTcpClient(cb TcpCallback) *TcpClient {
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

	cli := TcpClient{epoller, cb, bufQueue}
	go cli.start()
	return &cli
}

func (cli *TcpClient) ConnectServer(ipsec string) *EventItem {
	conn, err := net.DialTimeout("tcp", ipsec, 10*time.Second)
	if err != nil {
		logrus.Infof("failed to connect: %v", err)
		return nil
	}

	item, err := cli.epoller.Add(conn)
	if err != nil {
		logrus.Errorf("failed to add connection %v\n", err)
		conn.Close()
		return nil
	}

	return item
}

func (cli *TcpClient) Send(item *EventItem, data []byte) (int, error) {
	if item == nil {
		return 0, errors.New("invalid connection.")
	}

	item.conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	n, err := item.conn.Write(data)
	return n, err
}

func (cli *TcpClient) Close(item *EventItem) {
	if item == nil {
		return
	}

	if err := cli.epoller.Remove(item); err != nil {
		logrus.Errorf("failed to remove %v\n", err)
	}

	item.conn.Close()
}

func (cli *TcpClient) start() {
	for {
		items, err := cli.epoller.Wait()
		if err != nil {
			logrus.Errorf("failed to epoll wait %v\n", err)
			continue
		}

		if len(items) <= 0 {
			continue
		}

		msgHandler.handleMessage(items, cli.epoller, cli.cb, cli.bufQueue)
	}
}
