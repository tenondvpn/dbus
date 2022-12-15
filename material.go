package dbus

import (
	"context"
	"fmt"
	"strconv"

	"time"

	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/client/v3"
)

type Material struct {
	etcdClient   *etcd.Client
	queueIdPath  string
	rxOrTxIdPath string
}

func NewMaterial(etcdClient *etcd.Client) *Material {
	return &Material{etcdClient, QueuePathPrefix + "/" + "material/queueId/", QueuePathPrefix + "/" + "material/rxTxId/"}
}

// 0 is invalid id for all material
func (m *Material) GetQueueId() uint16 {
	return m.getIdWithPath(m.queueIdPath)
}

func (m *Material) RemoveQueueId(id uint16) {
	idPath := m.queueIdPath + fmt.Sprintf("%d", id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	_, err := m.etcdClient.Delete(ctx, idPath)
	cancel()
	if err != nil {
		logrus.Error(err)
	}
}

// transmitters and receivers are global distincted with id
func (m *Material) GetTxOrRxId() uint16 {
	return m.getIdWithPath(m.rxOrTxIdPath)
}

func (m *Material) RemoveTxOrRxId(id uint16) {
	idPath := m.rxOrTxIdPath + fmt.Sprintf("%d", id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	_, err := m.etcdClient.Delete(ctx, idPath)
	cancel()
	if err != nil {
		logrus.Error(err)
	}
}

func (m *Material) getIdWithPath(path string) uint16 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := m.etcdClient.Get(ctx, path, etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend))
	cancel()
	if err != nil {
		logrus.Error(err)
	}

	idMap := make(map[uint16]uint16)
	idMap[0] = 0
	var maxId uint16 = 0
	for _, ev := range resp.Kvs {
		id, err := strconv.Atoi(string(ev.Value))
		if err != nil {
			continue
		}

		tmpId := uint16(id)
		idMap[tmpId] = tmpId
		if tmpId > maxId {
			maxId = tmpId
		}
	}

	maxId++
	for {
		if _, ok := idMap[maxId]; ok {
			maxId++
			continue
		}

		idStrVal := fmt.Sprintf("%d", maxId)
		idMap[maxId] = maxId
		maxId++
		idPath := path + idStrVal
		tresp, err := m.etcdClient.Txn(context.TODO()).
			If(etcd.Compare(etcd.Version(idPath), "=", 0)).
			Then(etcd.OpPut(idPath, idStrVal)).
			Else(etcd.OpGet(idPath)).
			Commit()
		if err != nil {
			logrus.Errorf("%v\n", err)
			continue
		}

		if !tresp.Succeeded {
			logrus.Warnf("path has created: %v, %s", tresp, idPath)
			continue
		}

		logrus.Debugf("response success: %v, %s", tresp, idPath)
		break
	}

	return maxId - 1
}
