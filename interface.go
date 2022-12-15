package dbus

import (
	"fmt"
	"time"

	levigo "github.com/jmhodges/levigo"
	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/client/v3"
)

var (
	etcdClient      *etcd.Client
	queuManager     *QueueManager
	globalLocalIp   string
	globalLocalPort uint16
	tcpServer       *TcpServer
	db              *levigo.DB
	tcpClient       *TcpClient
	msgHandler      *MessageHandler
	dbLruCache      *levigo.Cache
	dbEnv           *levigo.Env
	dbOption        *levigo.Options
	dbWOption       *levigo.WriteOptions
	dbROption       *levigo.ReadOptions
	bloomPolicy     *levigo.FilterPolicy
)

type ConfigInfo struct {
	local_ip   string `yaml:"local_ip"`
	local_port uint16 `yaml:"local_port"`
	etcd       string `yaml:"etcd"`
}

func Init(localIp string, localPort uint16, etcdSpec string, dbPath string) bool {
	globalLocalIp = localIp
	globalLocalPort = localPort
	etcdCli, err := etcd.New(etcd.Config{
		Endpoints:   []string{etcdSpec},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logrus.Errorf("connect etcd failed err: %v", err)
		panic("connect etcd failed!")
		return false
	}

	if !openDb(dbPath) {
		return false
	}

	etcdClient = etcdCli
	msgHandler = NewMessageHandler(IoThreadCount)
	tcpServer = NewTcpServer(msgHandler.onMsg)
	tcpClient = NewTcpClient(msgHandler.onMsg)
	queuManager = NewQueueManger()
	go tcpServer.StartServer(globalLocalIp + ":" + fmt.Sprintf("%d", globalLocalPort))
	return true
}

func Destroy() {
	closeDb()
}

func CreateQueue(queueName string, ha bool, expireSecond int64, protoType int) *Queue {
	return queuManager.CreateQueue(queueName, ha, expireSecond, protoType)
}

func openDb(dbPath string) bool {
	if levigo.GetLevelDBMajorVersion() <= 0 {
		logrus.Errorf("Major version cannot be less than zero")
	}

	dbEnv = levigo.NewDefaultEnv()
	dbLruCache = levigo.NewLRUCache(1 << 20)
	bloomPolicy := levigo.NewBloomFilter(10)

	dbOption = levigo.NewOptions()
	// options.SetComparator(cmp)
	dbOption.SetErrorIfExists(true)
	dbOption.SetCache(dbLruCache)
	dbOption.SetEnv(dbEnv)
	dbOption.SetFilterPolicy(bloomPolicy)
	dbOption.SetInfoLog(nil)
	dbOption.SetWriteBufferSize(1 << 20)
	dbOption.SetParanoidChecks(true)
	dbOption.SetMaxOpenFiles(1024)
	dbOption.SetBlockSize(4096)
	dbOption.SetBlockRestartInterval(8)
	dbOption.SetCompression(levigo.NoCompression)

	dbROption = levigo.NewReadOptions()
	dbROption.SetVerifyChecksums(false)
	dbROption.SetFillCache(false)

	dbWOption = levigo.NewWriteOptions()
	dbWOption.SetSync(false)

	dbOption.SetCreateIfMissing(true)
	dbOption.SetErrorIfExists(false)
	tmpdb, err := levigo.Open(dbPath, dbOption)
	if err != nil {
		logrus.Errorf("Open failed: %v", err)
		err = levigo.RepairDatabase(dbPath, dbOption)
		if err != nil {
			logrus.Errorf("repaire db failed: %v", err)
			return false
		}

		tmpdb, err = levigo.Open(dbPath, dbOption)
		if err != nil {
			logrus.Errorf("reopen failed: %v", err)
			return false
		}
	}

	db = tmpdb
	return true
}

func closeDb() {
	db.Close()
	dbOption.Close()
	dbROption.Close()
	dbWOption.Close()
	dbLruCache.Close()
	bloomPolicy.Close()
	dbEnv.Close()
}
