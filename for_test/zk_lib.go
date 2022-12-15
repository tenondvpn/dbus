package dbus

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func zk_main() {
	callbackChan := make(chan zk.Event)
	f := func(event zk.Event) {
		callbackChan <- event
	}

	zk_hanler, eventChan, err := zk.Connect([]string{"82.156.224.174:2181"}, 15*time.Second, zk.WithEventCallback(f))
	if err != nil {
		fmt.Printf("Connect returned error: %+v\n", err)
	}
	fmt.Printf("0 1\n")

	verifyEventOrder := func(c <-chan zk.Event, expectedStates []zk.State, source string) {
		fmt.Printf("0 1 2\n")
		for _, state := range expectedStates {
			for {
				event, ok := <-c
				if !ok {
					fmt.Printf("unexpected channel close for %s\n", source)
				}

				if event.Type != zk.EventSession {
					continue
				}

				if event.State != state {
					fmt.Printf("mismatched state order from %s, expected %v, received %v\n", source, state, event.State)
				}
				break
			}
		}
	}

	fmt.Printf("2\n")

	states := []zk.State{zk.StateConnecting, zk.StateConnected, zk.StateHasSession}
	verifyEventOrder(callbackChan, states, "callback")
	verifyEventOrder(eventChan, states, "event channel")
	fmt.Printf("0 000000 2 \n")
	{
		path := "/gozk-test"

		if err := zk_hanler.Delete(path, -1); err != nil && err != zk.ErrNoNode {
			fmt.Printf("Delete returned error: %+v\n", err)
		} else {
			fmt.Printf("Delete returned success\n")
		}
		if p, err := zk_hanler.Create(path, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			fmt.Printf("Create returned error: %+v\n", err)
		} else if p != path {
			fmt.Printf("Create returned different path '%s' != '%s'\n", p, path)
		} else {
			fmt.Printf("Create returned success\n")
		}
		if data, stat, err := zk_hanler.Get(path); err != nil {
			fmt.Printf("Get returned error: %+v\n", err)
		} else if stat == nil {
			fmt.Printf("Get returned nil stat\n")
		} else if len(data) < 4 {
			fmt.Printf("Get returned wrong size data\n")
		} else {
			fmt.Printf("Get returned data: %d,%d,%d,%d\n", data[0], data[1], data[2], data[3])
		}
	}
	fmt.Printf("0000000 1 1\n")

	{
		path := "/gozk-test"

		if err := zk_hanler.Delete(path, -1); err != nil && err != zk.ErrNoNode {
			fmt.Printf("Delete returned error: %+v\n", err)
		} else {
			fmt.Printf("Delete returned success 0\n")
		}
		ops := []interface{}{
			&zk.CreateRequest{Path: path, Data: []byte{1, 2, 3, 4}, Acl: zk.WorldACL(zk.PermAll)},
			&zk.SetDataRequest{Path: path, Data: []byte{3, 2, 3, 4}, Version: -1},
		}
		if res, err := zk_hanler.Multi(ops...); err != nil {
			fmt.Printf("Multi returned error: %+v\n", err)
		} else if len(res) != 2 {
			fmt.Printf("Expected 2 responses got %d\n", len(res))
		} else {
			fmt.Printf("%+v", res)
		}
		if data, stat, err := zk_hanler.Get(path); err != nil {
			fmt.Printf("Get returned error: %+v\n", err)
		} else if stat == nil {
			fmt.Printf("Get returned nil stat\n")
		} else if len(data) < 4 {
			fmt.Printf("Get returned wrong size data\n")
		} else {
			fmt.Printf("Get success\n")
		}
	}

	{
		deleteNode := func(node string) {
			if err := zk_hanler.Delete(node, -1); err != nil && err != zk.ErrNoNode {
				fmt.Printf("Delete returned error: %+v", err)
			} else {
				fmt.Printf("child delete zk path success.\n")
			}
		}

		deleteNode("/gozk-test-big")

		if path, err := zk_hanler.Create("/gozk-test-big", []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			fmt.Printf("Create returned error: %+v", err)
		} else if path != "/gozk-test-big" {
			fmt.Printf("Create returned different path '%s' != '/gozk-test-big'\n", path)
		} else {
			fmt.Printf("child create zk path success.\n")
		}

		rb := make([]byte, 1000)
		hb := make([]byte, 2000)
		prefix := []byte("/gozk-test-big/")
		for i := 0; i < 3; i++ {
			_, err := rand.Read(rb)
			if err != nil {
				fmt.Printf("Cannot create random znode name\n")
			}
			hex.Encode(hb, rb)

			expect := string(append(prefix, hb...))
			if path, err := zk_hanler.Create(expect, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
				fmt.Printf("Create returned error: %+v\n", err)
			} else if path != expect {
				fmt.Printf("Create returned different path '%s' != '%s'\n", path, expect)
			} else {
				fmt.Printf("childs create zk path success.\n")
			}
			deleteNode(string(expect))
		}

		children, _, err := zk_hanler.Children("/gozk-test-big")
		if err != nil {
			fmt.Printf("Children returned error: %+v\n", err)
		} else if len(children) != 3 {
			fmt.Printf("Children returned wrong number of nodes\n")
		} else {
			fmt.Printf("get childs create zk path success.\n")
		}

	}

	{
		zk_hanler, _, err := zk.Connect([]string{"82.156.224.174:2181"}, 15*time.Second)
		if err != nil {
			fmt.Printf("Connect returned error: %+v\n", err)
		}
		if err := zk_hanler.Delete("/gozk-test", -1); err != nil && err != zk.ErrNoNode {
			fmt.Printf("Delete returned error: %+v\n", err)
		} else {
			fmt.Printf("Delete returned success 1\n")
		}

		watch1 := func() {
			for true {
				children, stat, childCh, err := zk_hanler.ChildrenW("/")
				if err != nil {
					fmt.Printf("Children returned error: %+v\n", err)
				} else if stat == nil {
					fmt.Printf("Children returned nil stat\n")
				} else if len(children) < 1 {
					fmt.Printf("Children should return at least 1 child\n")
				} else {
					fmt.Printf("ChildrenW returned success1\n")
				}

				select {
				case ev := <-childCh:
					if ev.Err != nil {
						fmt.Printf("Child watcher error %+v\n", ev.Err)
					}
					if ev.Path != "/" {
						fmt.Printf("Child watcher wrong path %s instead of %s\n", ev.Path, "/")
					}

					fmt.Printf("Child watcher get path: %s\n", ev.Path)
					// case <-time.After(time.Second * 3):
					// 	fmt.Printf("Child watcher timed out\n")
				}
			}
		}

		go watch1()

		time.Sleep(time.Second * 1)
		fmt.Printf("now create new path1\n")
		if path, err := zk_hanler.Create("/gozk-test", []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			fmt.Printf("Create returned error: %+v\n", err)
		} else if path != "/gozk-test" {
			fmt.Printf("Create returned different path '%s' != '/gozk-test'\n", path)
		} else {
			fmt.Printf("create path success 1\n")
		}

		// Delete of the watched node should trigger the watch

		// watch2 := func() {
		// 	children, stat, childCh, err := zk_hanler.ChildrenW("/gozk-test")
		// 	if err != nil {
		// 		fmt.Printf("Children returned error: %+v\n", err)
		// 	} else if stat == nil {
		// 		fmt.Printf("Children returned nil stat\n")
		// 	} else if len(children) != 0 {
		// 		fmt.Printf("Children should return 0 children\n")
		// 	} else {
		// 		fmt.Printf("ChildrenW path success 2\n")
		// 	}

		// 	select {
		// 	case ev := <-childCh:
		// 		if ev.Err != nil {
		// 			fmt.Printf("Child watcher error %+v\n", ev.Err)
		// 		}
		// 		if ev.Path != "/gozk-test" {
		// 			fmt.Printf("Child watcher wrong path %s instead of %s\n", ev.Path, "/")
		// 		}

		// 		fmt.Printf("Child watcher get path: %s\n", ev.Path)
		// 	case <-time.After(time.Second * 200):
		// 		fmt.Printf("Child watcher timed out\n")
		// 	}
		// }

		// go watch2()
		time.Sleep(time.Second * 1)

		if err := zk_hanler.Delete("/gozk-test", -1); err != nil && err != zk.ErrNoNode {
			fmt.Printf("Delete returned error: %+v\n", err)
		} else {
			fmt.Printf("Delete path success 2\n")
		}

		time.Sleep(time.Second * 1)
		if path, err := zk_hanler.Create("/gozk-test", []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			fmt.Printf("Create returned error: %+v\n", err)
		} else if path != "/gozk-test" {
			fmt.Printf("Create returned different path '%s' != '/gozk-test'\n", path)
		} else {
			fmt.Printf("create path success 1\n")
		}
	}

	{
		zk_hanler, _, err := zk.Connect([]string{"82.156.224.174:2181"}, 15*time.Second)
		if err != nil {
			fmt.Printf("Connect returned error: %+v\n", err)
		}

		zk2, _, err := zk.Connect([]string{"82.156.224.174:2181"}, 15*time.Second)
		if err != nil {
			fmt.Printf("Connect returned error: %+v", err)
		}
		defer zk2.Close()

		if err := zk_hanler.Delete("/gozk-test", -1); err != nil && err != zk.ErrNoNode {
			fmt.Printf("Delete returned error: %+v", err)
		}

		testPaths := map[string]<-chan zk.Event{}
		defer func() {
			// clean up all of the test paths we create
			for p := range testPaths {
				zk2.Delete(p, -1)
			}
		}()

		// we create lots of paths to watch, to make sure a "set watches" request
		// on re-create will be too big and be required to span multiple packets
		for i := 0; i < 10; i++ {
			testPath, err := zk_hanler.Create(fmt.Sprintf("/gozk-test-%d", i), []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				fmt.Printf("Create returned: %+v", err)
			}
			testPaths[testPath] = nil
			_, _, testEvCh, err := zk_hanler.GetW(testPath)
			if err != nil {
				fmt.Printf("GetW returned: %+v", err)
			}
			testPaths[testPath] = testEvCh
		}

		children, stat, childCh, err := zk_hanler.ChildrenW("/")
		if err != nil {
			fmt.Printf("Children returned error: %+v", err)
		} else if stat == nil {
			fmt.Printf("Children returned nil stat")
		} else if len(children) < 1 {
			fmt.Printf("Children should return at least 1 child")
		}

		// Simulate network error by brutally closing the network connection.
		zk_hanler.Close()
		for p := range testPaths {
			if err := zk2.Delete(p, -1); err != nil && err != zk.ErrNoNode {
				fmt.Printf("Delete returned error: %+v", err)
			}
		}
		if path, err := zk2.Create("/gozk-test", []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			fmt.Printf("Create returned error: %+v", err)
		} else if path != "/gozk-test" {
			fmt.Printf("Create returned different path '%s' != '/gozk-test'", path)
		}

		time.Sleep(100 * time.Millisecond)

		// zk should still be waiting to reconnect, so none of the watches should have been triggered
		for p, ch := range testPaths {
			select {
			case <-ch:
				fmt.Printf("GetW watcher for %q should not have triggered yet", p)
			default:
			}
		}
		select {
		case <-childCh:
			fmt.Printf("ChildrenW watcher should not have triggered yet")
		default:
		}

		// now we let the reconnect occur and make sure it resets watches
		for p, ch := range testPaths {
			select {
			case ev := <-ch:
				if ev.Err != nil {
					fmt.Printf("GetW watcher error %+v", ev.Err)
				}
				if ev.Path != p {
					fmt.Printf("GetW watcher wrong path %s instead of %s", ev.Path, p)
				}
			case <-time.After(2 * time.Second):
				fmt.Printf("GetW watcher timed out")
			}
		}

		select {
		case ev := <-childCh:
			if ev.Err != nil {
				fmt.Printf("Child watcher error %+v", ev.Err)
			}
			if ev.Path != "/" {
				fmt.Printf("Child watcher wrong path %s instead of %s", ev.Path, "/")
			}
		case <-time.After(2 * time.Second):
			fmt.Printf("Child watcher timed out")
		}
	}

	time.Sleep(time.Second * 1)

	zk_hanler.Close()
	verifyEventOrder(callbackChan, []zk.State{zk.StateDisconnected}, "callback")
	verifyEventOrder(eventChan, []zk.State{zk.StateDisconnected}, "event channel")

	// c, _, err := zk.Connect([]string{"82.156.224.174:2181"}, time.Second) //*10)
	// if err != nil {
	// 	panic(err)
	// }
	// children, stat, ch, err := c.ChildrenW("/")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("%+v %+v\n", children, stat)
	// e := <-ch
	// fmt.Printf("%+v\n", e)
}
