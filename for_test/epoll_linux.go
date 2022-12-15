package dbus

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"net"
	"reflect"
	"syscall"
	"unsafe"
)

type epoll struct {
	fd          int
	connections map[int]*EventItem
}

func MkEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoll{
		fd:          fd,
		connections: make(map[int]*EventItem),
	}, nil
}

func (e *epoll) Add(conn net.Conn) (*EventItem, error) {
	// Extract file descriptor associated with the connection
	fd := e.socketFD(conn)
	msgBuf := newBuffer(conn, MaxPackageSize)
	eventItem := &EventItem{conn, fd, &msgBuf, false, 0}
	u64Val := uintptr(unsafe.Pointer(eventItem))
	first := uint32(u64Val & 0x00000000FFFFFFFF)
	second := uint32(u64Val >> 32 & 0x00000000FFFFFFFF)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.POLLIN | unix.POLLHUP,
		Fd:     int32(first),
		Pad:    int32(second)})
	if err != nil {
		return nil, err
	}

	for tmpFd, item := range e.connections {
		if item.removed {
			delete(e.connections, tmpFd)
		}
	}

	e.connections[fd] = eventItem
	return eventItem, nil
}

func (e *epoll) Remove(item *EventItem) error {
	fd := e.socketFD(item.conn)
	if fd == -1 {
		return nil
	}

	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		logrus.Errorf("remove fd failed: %d, %v", fd, err)
		return err
	}

	item.removed = true
	return nil
}

func (e *epoll) Wait() ([]*EventItem, error) {
	events := make([]unix.EpollEvent, 100)
retry:
	n, err := unix.EpollWait(e.fd, events, 100)
	if err != nil {
		if err == unix.EINTR {
			goto retry
		}
		return nil, err
	}
	var res_connections []*EventItem
	for i := 0; i < n; i++ {
		u64Adr := uint64(events[i].Pad)<<32 | uint64(events[i].Fd)
		eventAddr := (*EventItem)(unsafe.Pointer(uintptr(u64Adr)))
		res_connections = append(res_connections, eventAddr)
	}
	return res_connections, nil
}

func (e *epoll) socketFD(conn net.Conn) int {
	//tls := reflect.TypeOf(conn.UnderlyingConn()) == reflect.TypeOf(&tls.Conn{})
	// Extract the file descriptor associated with the connection
	//connVal := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn").Elem()
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	//if tls {
	//	tcpConn = reflect.Indirect(tcpConn.Elem())
	//}
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")

	return int(pfdVal.FieldByName("Sysfd").Int())
}
