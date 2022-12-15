# dbus

## 高可用
- 基于ETCD实现集群管理和配置服务，服务注册发现
- 支持发送端集群，支持多活模式：同时发送根据消息序号自动去重，支持主备模式：主备自动切换
- 支持接收端高可用集群，保证集群所有节点状态一致

## 可靠性
-  消息不重不丢不乱
-  保证ExactlyOnce语意
-  节点宕机重启自动同步
-  消息实时持久化

## 一致性
-  支持强一致性，支持2段提交可靠性
-  支持自定义可靠比率，即自定义m/n个节点提交成功则成功

## 性能
-  当前采用磁盘实时持久化，tcp协议，256字节的消息报文，一个发送端，两个接收端，QPS：20000 左右
-  后期如果业务需要，可升级为SHM实时持久化，并支持UDP组播

## 易使用

## 可扩展
- 所有节点均可以水平自动扩容



# Usage

## 1. 安装依赖项

  下载dbus项目，并运行命令

```
  source dep.sh
```

## 2. 创建消息订阅

```
package main

import (
	"fmt"

	dbus "github.com/tenondvpn/dbus"
)

var (
	prevCountTmMilli int64  = dbus.TimeStampMilli()
	prevCount        uint64 = 0
)

func TmpRxCallback(msgId uint64, data []byte) int {
	nowTm := dbus.TimeStampMilli()
	if nowTm-prevCountTmMilli >= 1000 {
		fmt.Printf("qps: %d\n", (msgId-prevCount)*1000/uint64(nowTm-prevCountTmMilli))
		prevCountTmMilli = nowTm
		prevCount = msgId
	}

	return 0
}

func main() {
	// test etcd server: "82.156.224.174:2379"
	// db_path level db path example: "./rxdata"
	dbus.Init("127.0.0.1", 7891, "82.156.224.174:2379", "./rxdb")
	// 修改queueName
	queue := dbus.CreateQueue(queueName, true, 3600, dbus.ProtoTypeTcp)
	queue.CreateReceiver(TmpRxCallback)
	select {}
}


```

## 3. 创建消息发布

```
package main

import (
	"encoding/binary"
	"time"

	dbus "github.com/tenondvpn/dbus"
)

func main() {
	dbus.Init("127.0.0.1", 8001, "82.156.224.174:2379", "./txdata")
	queue := dbus.CreateQueue(queueName, true, 3600, dbus.ProtoTypeTcp)
	tx := queue.CreateTransmitter("")
	time.Sleep(time.Millisecond * 100)
	data := make([]byte, 32)
	for i := 0; i < 1000000; i++ {
		binary.BigEndian.PutUint64(data, uint64(i))
		tx.Send(data)
	}

	select {}
}
```
