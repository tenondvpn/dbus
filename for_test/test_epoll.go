package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"golang.org/x/sys/unix"
)

const (
	MaxEpollEvents = 32
)

type epollEvent struct {
	Events uint32
	Ptr    unsafe.Pointer // Event struct
}

type Event struct {
	Fd     int
	send   bool
	Cookie interface{}
}

func echo(ev Event) error {
	spew.Dump(ev)

	var buf [32 * 1024]byte

	nbytes, err := unix.Read(ev.Fd, buf[:])
	if nbytes > 0 {
		//fmt.Printf(">>> %s", buf)
		//unix.Write(ev.Fd, buf[:nbytes])
		//fmt.Printf("<<< %s", buf)
	}
	if nbytes <= 0 {
		if nbytes == 0 || err != unix.EAGAIN {
			return fmt.Errorf("closed: %v", err)
		}

		// return stuff to configure epoll in EPOLLIN on this fd.
		return nil
	}

	return nil
}

func netWorker() {
	var event epollEvent
	var events [MaxEpollEvents]epollEvent

	fd, err := newSocket(net.ParseIP("::0"), 7000)
	if err != nil {
		log.Fatal(err)
	}

	epfd, e := unix.EpollCreate1(0)
	if e != nil {
		fmt.Println("epoll_create1: ", e)
		os.Exit(1)
	}
	defer unix.Close(epfd)

	event.Events = unix.EPOLLIN

	ptr := C.malloc(C.ulong(unsafe.Sizeof(Event{})))
	(*Event)(ptr).Fd = fd
	(*Event)(ptr).send = false
	(*Event)(ptr).Cookie = nil
	event.Ptr = ptr

	if e = epollCtl(epfd, unix.EPOLL_CTL_ADD, fd, &event); e != nil {
		fmt.Println("epoll_ctl: ", e)
		os.Exit(1)
	}

	for {
		nevents, e := epollWait(epfd, events[:], -1)
		if e == unix.EINTR {
			continue
		} else if e != nil {
			fmt.Println("epoll_wait: ", e)
			break
		}

		for ev := 0; ev < nevents; ev++ {
			if (*(*Event)(events[ev].Ptr)).Fd == fd {
				connFd, _, err := unix.Accept(fd)
				if err != nil {
					fmt.Println("accept: ", err)
					continue
				}
				unix.SetNonblock(fd, true)

				event.Events = unix.EPOLLIN

				p2 := C.malloc(C.ulong(unsafe.Sizeof(Event{})))
				(*Event)(p2).Fd = connFd
				(*Event)(p2).send = false
				(*Event)(p2).Cookie = "COOKIE about my connection"
				event.Ptr = p2

				if err := epollCtl(epfd, unix.EPOLL_CTL_ADD, connFd, &event); err != nil {
					fmt.Print("epoll_ctl: ", connFd, err)
					os.Exit(1)
				}
			} else {
				if err := echo(*(*Event)(events[ev].Ptr)); err != nil {
					if e = epollCtl(epfd, unix.EPOLL_CTL_DEL, (*(*Event)(events[ev].Ptr)).Fd, &events[ev]); e != nil {
						log.Fatal("epoll_ctl del: ", e)
					}

					unix.Close((*(*Event)(events[ev].Ptr)).Fd)
					C.free(events[ev].Ptr)
				}
			}
		}
	}
}

func main() {
	for i := 0; i < 10; i++ {
		go netWorker()
	}

	netWorker()
}
