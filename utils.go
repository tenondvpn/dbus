package dbus

import (
	"net"
	"reflect"
	"time"
	"unsafe"

	queue "github.com/smallnest/queue"
)

// can change with config
const (
	MaxReceivers               = 65536
	MaxTransmitters            = 65536
	MaxQueues                  = 65536
	SingleQueueMaxReceivers    = 16
	SingleQueueMaxTransmitters = 4
	SingleProcMaxQueues        = 32
	MaxUint64                  = ^uint64(0)
)

var (
	TxHeadLen                        = 20
	TxSaveHeadLen                    = 32
	TxSaveHeadOffsetLen              = 12
	ConnectionTimeoutMilli    int64  = 5000
	MaxPackageSize                   = 1024 * 1024 * 2
	PackageHeadLenSize               = 4
	QueuePathPrefix                  = "/dbus"
	QueuesPath                       = QueuePathPrefix + "/queues/"
	AllQueuePath                     = QueuePathPrefix + "/all_queues"
	RxTimeoutSeconds                 = 15
	AckTimeoutMilli           int64  = 3000
	IoTimerMilli              int64  = 100
	TxHeartbeatPeriod         int64  = 3000
	DefaultTxWindowSize       int64  = 1024
	MaxNakCount                      = 1024
	NakTimeoutMilli           int64  = 3000
	MaxNakBufferSize                 = 10 * 1024 * 1024
	CheckLeaderTimeout        int64  = 3000
	IoThreadCount                    = 4
	IoPoolSize                       = 32
	BuffPoolSize                     = IoThreadCount * (IoPoolSize + 1)
	StatusTimePeriodMilli     int64  = 3000
	SyncToleranceRange        uint64 = 1024 // follower节点与leader同步，最小容忍范围，即必须与leader同步到这个范围内，才能竞选leader，以及响应发送端ack
	TxMaxDiffWithSequenceId   uint64 = 102400
	DefaultSequenceWindowSize        = DefaultTxWindowSize
)

const (
	TypeQueueIndex       = 0
	TypeAck              = 1
	TypeNak              = 2
	TypeHearbeatResponse = 3

	TypeTxMsg               = 101
	TypeNakMore             = 102
	TypeHearbeatRequest     = 103
	TypeSeqId               = 104
	TypeSeqIdAck            = 105
	TypeSeqIdNak            = 106
	TypeSeqIdNakRes         = 107
	TypeSeqIdCommit         = 108
	TypeSeqIdNakMore        = 109
	TypeSeqHearbeatRequest  = 110
	TypeSeqHearbeatResponse = 111

	TypeCfgNewRx          = 1000
	TypeCfgNewTx          = 1001
	TypeCfgRxLeaderChange = 1002
	TypeCfgTxLeaderChange = 1003
)

const (
	ProtoTypeTcp       = 0
	ProtoTypeUdp       = 1
	ProtoTypeMulticast = 2
)

// message proto
const (
	TxMsgHeadLen  = 20
	AckMsgHeadLen = 20
)

type QueueMsg struct {
	queueId  uint16
	msgType  uint16
	data     []byte
	conn     *net.Conn
	msgBuf   *MsgBuffer
	bufQueue *queue.LKQueue
}

type ConfigMsg struct {
	queueId   uint16
	msgType   uint16
	txCluster string
	data      []byte
}

type MsgHead struct {
	timeout    int64
	directSent int32
	len        int32
	t          uint16
	qid        uint16
	txId       uint16
	reserve    uint16
	msgId      uint64
}

type AckMsg struct {
	len     int32
	t       uint16
	qid     uint16
	rxId    uint16
	reserve uint16
	msgId   uint64
}

type Heartbeat struct {
	len     int32
	t       uint16
	qid     uint16
	rxId    uint16
	reserve uint16
	msgId   uint64
}

type Nak struct {
	len    int32
	t      uint16
	qid    uint16
	rxId   uint16
	count  uint16
	nakIds []uint64
}

type NakMore struct {
	len  int32
	t    uint16
	qid  uint16
	rxId uint16
}

type EndPoint struct {
	ip   string
	port uint16
}

type EventItem struct {
	conn       net.Conn
	fd         int
	msgBuf     *MsgBuffer
	removed    bool
	packageLen int
	parentConn *Connection
}

type QueueMessageInfo struct {
	queueIndex     uint64
	syncQueueIndex uint64
	calledIndex    uint64
	dirty          bool
}

type QueueInfo struct {
	Id            uint16  `json:"Id"`
	QueueName     string  `json:"QueueName"`
	ProtoType     int     `json:"ProtoType"`
	ExpireSeconds int64   `json:"ExpireSeconds"` // default 24hour
	Ha            bool    `json:"Ha"`            // default false
	WindowSize    int64   `json:"WindowSize"`    // default 256
	AckRatio      int16   `json:"AckRatio"`      // -1: direct, not -1: 1/4 windowSize
	SeqAckRatio   float32 `json:"SeqAckRatio"`   // -1: direct, not -1: 1/4 windowSize
}

type TxAckedInfo struct {
	receivedMaxIndex     uint64
	ackedIndex           uint64
	receivedIndex        uint64
	increasedIndex       uint64
	prevAckedTime        int64
	conn                 *net.Conn
	prevNakedTime        int64
	txId                 uint16
	nakCount             int64
	ackCount             int64
	hbCount              int64
	queueLimitTxIndex    map[uint64]uint64
	queueLimitStartIndex uint64
}

type ReceiverInfo struct {
	QueueId    uint16 `json:"queueid"`
	Id         uint16 `json:"id"`
	Ip         string `json:"ip"`
	Port       uint16 `json:"port"`
	AckedIndex uint64 `json:"ackedindex"`
}

type TransmitterInfo struct {
	QueueId           uint16  `json:"QueueId"`
	Id                uint16  `json:"Id"`
	Ip                string  `json:"Ip"`
	Port              uint16  `json:"Port"`
	SentIndex         uint64  `json:"SentIndex"`
	AckedIndex        uint64  `json:"AckedIndex"`
	IsLeader          bool    `json:"IsLeader"`
	MinIndex          uint64  `json:"MinIndex"`
	MaxIndex          uint64  `json:"MaxIndex"`
	ValidBeginIndex   uint64  `json:"ValidBeginIndex"`
	MinAckedCount     float32 `json:"MinAckedCount"`
	MaxCachedMsgCount uint64  `json:"MaxCachedMsgCount"`
	dirty             bool
	directSendCount   uint64
	resendCount       uint64
	resendMaxIndex    uint64
}

type SequenceIdInfo struct {
	MaxSeqId         uint64 `json:"MaxSeqId"`
	CalledSeqId      uint64 `json:"CalledSeqId"`
	AckedSeqId       uint64 `json:"AckedSeqId"`
	SentSeqId        uint64 `json:"SentSeqId"`
	prevSeqAckedTime int64
	ackedCount       int64
}

type Connection struct {
	endpoint          *EndPoint
	cliEv             *EventItem
	timeout           int64
	rxId              uint16
	ackedIndex        uint64
	prevHeatbeatTimer int64
	connectValid      bool
	seqAckedIndex     uint64
	conn              *net.Conn
	syncedValid       bool
	seqIdSyncedValid  bool
	// txDirectCliEv     *EventItem
	// txResendCliEv     *EventItem
}

type TcpCallback func(qMsg *QueueMsg) bool
type ReceiverCallback func(msgId uint64, data []byte) int

func TimeStampMilli() int64 {
	return time.Now().UnixNano() / (1000 * 1000)
}

func Bytes2Str(slice []byte) string {
	return *(*string)(unsafe.Pointer(&slice))
}

func Str2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}
