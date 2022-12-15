package dbus

import (
	"encoding/binary"
	"errors"
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

type MsgBuffer struct {
	reader net.Conn
	buf    []byte
	start  int // fix with callback
	maxLen int
	pkgLen int // must has len and read all msg, each time just read one pkg
}

func newBuffer(conn net.Conn, len int) MsgBuffer {
	buf := make([]byte, len)
	return MsgBuffer{conn, buf, 0, len, 0}
}

func (b *MsgBuffer) readFromReader() (int, []byte, error) {
	if b.pkgLen == 0 {
		if b.start >= 4 {
			logrus.Errorf("b.pkgLen == 0 and b.start >= 4: %d", b.start)
			return 0, nil, errors.New("invalid package len.")
		}

		b.reader.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
		n, err := b.reader.Read(b.buf[b.start:4])
		if err != nil {
			return 0, nil, err
		}

		b.start += n
		if b.start == 4 {
			b.pkgLen = int(binary.BigEndian.Uint32(b.buf[0:4]))
		} else {
			return 0, nil, nil
		}
	}

	if b.pkgLen < b.start || b.pkgLen >= MaxPackageSize {
		logrus.Errorf("b.pkgLen < b.start %d, %d", b.pkgLen, b.start)
		return 0, nil, errors.New("invalid package len.")
	}

	b.reader.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
	n, err := b.reader.Read(b.buf[b.start:b.pkgLen])
	if err != nil {
		return 0, nil, err
	}

	b.start += n
	if b.pkgLen == b.start {
		tmpLen := b.pkgLen
		b.start = 0
		b.pkgLen = 0
		return tmpLen, b.buf[0:tmpLen], nil
	}

	return 0, nil, nil
}
