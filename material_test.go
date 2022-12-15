package dbus

// import (
// 	"context"
// 	"testing"

// 	// "github.com/prometheus/client_golang/prometheus/promhttp"
// 	"github.com/sirupsen/logrus"
// 	etcd "go.etcd.io/etcd/client/v3"

// 	// "net/http"
// 	"time"
// )

// func TestMaterial(t *testing.T) {
// 	cli, err := etcd.New(etcd.Config{
// 		Endpoints:   []string{"82.156.224.174:2379"},
// 		DialTimeout: 5 * time.Second,
// 	})
// 	if err != nil {
// 		// handle error!
// 		logrus.Errorf("connect etcd failed err: %v", err)
// 		return
// 	}
// 	defer cli.Close()

// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	_, err = cli.Delete(ctx, "/dbus", etcd.WithPrefix())
// 	if err != nil {
// 		logrus.Fatalf("ctx is canceled by another routine: %v", err)
// 	}
// 	cancel()
// 	mat := NewMaterial(cli)
// 	queueId := mat.GetQueueId()
// 	if queueId != 1 {
// 		t.Fatal("create queue id failed!")
// 	}

// 	rxId := mat.GetTxOrRxId()
// 	if rxId != 1 {
// 		t.Fatal("create rx id failed!")
// 	}
// }
