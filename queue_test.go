package dbus

import (
	// "context"
	"testing"
	// "github.com/prometheus/client_golang/prometheus/promhttp"
	// "github.com/sirupsen/logrus"
	// etcd "go.etcd.io/etcd/client/v3"
	// "net/http"
	// "time"
)

func RxCallback(msgId uint64, data []byte) int {
	return 0
}

func TestQueue(t *testing.T) {
	// cli, err := etcd.New(etcd.Config{
	// 	Endpoints:   []string{"82.156.224.174:2379"},
	// 	DialTimeout: 5 * time.Second,
	// })
	// if err != nil {
	// 	// handle error!
	// 	logrus.Errorf("connect etcd failed err: %v", err)
	// 	return
	// }
	// defer cli.Close()

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// _, err = cli.Delete(ctx, "/dbus", etcd.WithPrefix())
	// if err != nil {
	// 	logrus.Fatalf("ctx is canceled by another routine: %v", err)
	// }
	// cancel()

	// queue := GetOrCreateQueue(cli, "test_queue", true, -1, ProtoTypeTcp)
	// if queue == nil {
	// 	logrus.Fatalf("create queue failed")
	// 	return
	// }

	// queue.ResetExpireTime()
	// queue.CreateTransmitter("")
	// queue.CreateReceiver(RxCallback)
	// queue.CheckTxLeader()
	// queue.CheckRxLeader()
	// queue.DeleteQueue()
}
