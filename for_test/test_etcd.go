package main

import (
	"context"
	"fmt"
	"sync"

	// "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	// "io"
	// "net"
	// "net/http"
	"strings"
	"time"
)

func main() {
	cli, err := etcd.New(etcd.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		logrus.Errorf("connect etcd failed err: %v", err)
		return
	}
	defer cli.Close()

	{
		fooResp, err := cli.Put(context.TODO(), "foo/", "bar")
		if err != nil {
			logrus.Fatal(err)
		}
		if _, err = cli.Put(context.TODO(), "foo/a", "baz"); err != nil {
			logrus.Fatal(err)
		}

		tresp, terr := cli.Txn(context.TODO()).If(
			etcd.Compare(
				etcd.CreateRevision("foo/"), "=", fooResp.Header.Revision).
				WithPrefix(),
		).Commit()
		if terr != nil {
			logrus.Fatal(terr)
		}
		if tresp.Succeeded {
			logrus.Fatal("expected prefix compare to false, got compares as true")
		}
	}

	{
		fmt.Printf("etcd value: %v\n", etcd.Value("foo"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		_, err := cli.Delete(ctx, "foo")
		if err != nil {
			logrus.Fatalf("ctx is canceled by another routine: %v", err)
		}
		cancel()

		fmt.Printf("etcd value: %v\n", etcd.Value("foo"))
	}
	{
		tresp, err := cli.Txn(context.TODO()).
			If(etcd.Compare(etcd.Version("foo"), "=", 0)).
			Then(
				etcd.OpPut("foo", "bar"),
				etcd.OpTxn(nil, []etcd.Op{etcd.OpPut("abc", "123")}, nil)).
			Else(etcd.OpPut("foo", "baz")).Commit()
		if err != nil {
			logrus.Fatal(err)
		}
		if len(tresp.Responses) != 2 {
			logrus.Errorf("expected 2 top-level txn responses, got %+v", tresp.Responses)
		}

		// check txn writes were applied
		resp, err := cli.Get(context.TODO(), "foo")
		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Printf("resp 0: %v\n", resp)
		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "bar" {
			logrus.Errorf("unexpected Get response %+v", resp)
		}
		resp, err = cli.Get(context.TODO(), "abc")
		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Printf("resp 1: %v\n", resp)

		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "123" {
			logrus.Errorf("unexpected Get response %+v", resp)
		}
	}

	return
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		_, err := cli.Delete(ctx, "sample_key")
		if err != nil {
			logrus.Fatalf("ctx is canceled by another routine: %v", err)
		}
		cancel()
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		resp, err := cli.Put(ctx, "sample_key", "sample_value")
		if err != nil {
			switch err {
			case context.Canceled:
				logrus.Fatalf("ctx is canceled by another routine: %v", err)
			case context.DeadlineExceeded:
				logrus.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
			case rpctypes.ErrEmptyKey:
				logrus.Fatalf("client-side error: %v", err)
			default:
				logrus.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
			}

			return
		}
		cancel()
		logrus.Infof("etcd put success: %v", resp)
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		gresp, err := cli.Get(ctx, "sample_key", etcd.WithPrefix())
		cancel()
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Infof("etcd get success: %v", gresp)
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		resp, err := cli.Put(ctx, "sample_key", "sample_value0", etcd.WithKeysOnly())
		if err != nil {
			switch err {
			case context.Canceled:
				logrus.Fatalf("ctx is canceled by another routine: %v", err)
			case context.DeadlineExceeded:
				logrus.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
			case rpctypes.ErrEmptyKey:
				logrus.Fatalf("client-side error: %v", err)
			default:
				logrus.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
			}

			return
		}
		cancel()
		logrus.Infof("etcd put success: %v", resp)
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		gresp, err := cli.Get(ctx, "sample_key", etcd.WithPrefix())
		if err != nil {
			logrus.Fatal(err)
		}
		cancel()
		logrus.Infof("etcd get success: %v", gresp)
	}
	return

	// {

	// 	for i := range make([]int, 3) {
	// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 		_, err = cli.Put(ctx, fmt.Sprintf("key_%d", i), "value")
	// 		cancel()
	// 		if err != nil {
	// 			logrus.Fatal(err)
	// 		}
	// 	}

	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	resp, err := cli.Get(ctx, "key", etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend))
	// 	cancel()
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	for _, ev := range resp.Kvs {
	// 		logrus.Infof("%s : %s\n", ev.Key, ev.Value)
	// 	}
	// }

	// {
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	defer cancel()

	// 	// count keys about to be deleted
	// 	gresp, err := cli.Get(ctx, "key", etcd.WithPrefix())
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	// delete the keys
	// 	dresp, err := cli.Delete(ctx, "key", etcd.WithPrefix())
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	fmt.Println("Deleted all keys:", int64(len(gresp.Kvs)) == dresp.Deleted)
	// }

	// {
	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	resp, err := cli.Get(ctx, "foo")
	// 	cancel()
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	compRev := resp.Header.Revision // specify compact revision of your choice

	// 	ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	// 	_, err = cli.Compact(ctx, compRev)
	// 	cancel()
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }

	// {
	// 	kvc := etcd.NewKV(cli)

	// 	_, err = kvc.Put(context.TODO(), "key", "xyz")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// 	_, err = kvc.Txn(ctx).
	// 		// txn value comparisons are lexical
	// 		If(etcd.Compare(etcd.Value("key"), ">", "abc")).
	// 		// the "Then" runs, since "xyz" > "abc"
	// 		Then(etcd.OpPut("key", "XYZ")).
	// 		// the "Else" does not run
	// 		Else(etcd.OpPut("key", "ABC")).
	// 		Commit()
	// 	cancel()
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	gresp, err := kvc.Get(context.TODO(), "key")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	for _, ev := range gresp.Kvs {
	// 		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	// 	}
	// }

	// {
	// 	ops := []etcd.Op{
	// 		etcd.OpPut("put-key", "123"),
	// 		etcd.OpGet("put-key"),
	// 		etcd.OpPut("put-key", "456")}

	// 	for _, op := range ops {
	// 		if _, err := cli.Do(context.TODO(), op); err != nil {
	// 			logrus.Fatal(err)
	// 		}
	// 	}
	// }

	// {
	// 	// minimum lease TTL is 5-second
	// 	resp, err := cli.Grant(context.TODO(), 2)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	// after 5 seconds, the key 'foo' will be removed
	// 	_, err = cli.Put(context.TODO(), "foo", "bar", etcd.WithLease(resp.ID))
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	gresp, err := cli.Get(context.TODO(), "foo")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	fmt.Printf("get res: %v\n", gresp)
	// 	time.Sleep(time.Second * 3)
	// 	gresp, err = cli.Get(context.TODO(), "foo")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	fmt.Printf("get res: %v\n", gresp)
	// }

	// {
	// 	resp, err := cli.Grant(context.TODO(), 2)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	_, err = cli.Put(context.TODO(), "foo", "bar", etcd.WithLease(resp.ID))
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	// revoking lease expires the key attached to its lease ID
	// 	_, err = cli.Revoke(context.TODO(), resp.ID)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	gresp, err := cli.Get(context.TODO(), "foo")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	fmt.Println("number of keys:", len(gresp.Kvs))

	// 	gresp, err = cli.Get(context.TODO(), "foo")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	fmt.Printf("get res 0: %v\n", gresp)

	// 	time.Sleep(time.Second * 3)
	// 	gresp, err = cli.Get(context.TODO(), "foo")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	fmt.Printf("get res 1: %v\n", gresp)
	// }

	// {
	// 	resp, err := cli.Grant(context.TODO(), 5)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	_, err = cli.Put(context.TODO(), "foo", "bar", etcd.WithLease(resp.ID))
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	// the key 'foo' will be kept forever
	// 	ch, kaerr := cli.KeepAlive(context.TODO(), resp.ID)
	// 	if kaerr != nil {
	// 		logrus.Fatal(kaerr)
	// 	}

	// 	ka := <-ch
	// 	if ka != nil {
	// 		fmt.Println("ttl:", ka.TTL)
	// 	} else {
	// 		fmt.Println("Unexpected NULL")
	// 	}
	// }

	// {
	// 	resp, err := cli.Grant(context.TODO(), 5)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	_, err = cli.Put(context.TODO(), "foo", "bar", etcd.WithLease(resp.ID))
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}

	// 	// to renew the lease only once
	// 	ka, kaerr := cli.KeepAliveOnce(context.TODO(), resp.ID)
	// 	if kaerr != nil {
	// 		logrus.Fatal(kaerr)
	// 	}

	// 	fmt.Println("ttl:", ka.TTL)
	// }

	// {
	// 	cli.Get(context.TODO(), "test_key")
	// 	ln, err := net.Listen("tcp", ":0")
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	donec := make(chan struct{})
	// 	go func() {
	// 		defer close(donec)
	// 		http.Serve(ln, promhttp.Handler())
	// 	}()
	// 	defer func() {
	// 		ln.Close()
	// 		<-donec
	// 	}()

	// 	// make an http request to fetch all Prometheus metrics
	// 	url := "http://" + ln.Addr().String() + "/metrics"
	// 	resp, err := http.Get(url)
	// 	if err != nil {
	// 		logrus.Fatalf("fetch error: %v", err)
	// 	}
	// 	b, err := io.ReadAll(resp.Body)
	// 	resp.Body.Close()
	// 	if err != nil {
	// 		logrus.Fatalf("fetch error: reading %s: %v", url, err)
	// 	}

	// 	// confirm range request in metrics
	// 	for _, l := range strings.Split(string(b), "\n") {
	// 		if strings.Contains(l, `grpc_client_started_total{grpc_method="Range"`) {
	// 			fmt.Println(l)
	// 			break
	// 		}
	// 	}
	// }

	// watch
	{
		putFunc := func() {
			for i := 0; i < 2; i++ {
				{
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					_, err := cli.Put(ctx, "testfoo", "sample_value")
					if err != nil {
						switch err {
						case context.Canceled:
							logrus.Fatalf("ctx is canceled by another routine: %v", err)
						case context.DeadlineExceeded:
							logrus.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
						case rpctypes.ErrEmptyKey:
							logrus.Fatalf("client-side error: %v", err)
						default:
							logrus.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
						}

						return
					}
					cancel()
				}
				time.Sleep(time.Second * 1)

				{
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					cli.Delete(ctx, "testfoo")
					cancel()
				}

				time.Sleep(time.Second * 1)
			}
		}

		go putFunc()
		rch := cli.Watch(context.Background(), "testfoo")
		eventFunc := func() {
			for wresp := range rch {
				for _, ev := range wresp.Events {
					fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				}
			}
		}

		go eventFunc()
		time.Sleep(time.Second * 5)
	}

	{
		// watches within ['foo1', 'foo4'), in lexicographical order
		rch := cli.Watch(context.Background(), "foo1", etcd.WithRange("foo4"))

		go func() {
			cli.Put(context.Background(), "foo1", "bar1")
			cli.Put(context.Background(), "foo5", "bar5")
			cli.Put(context.Background(), "foo2", "bar2")
			cli.Put(context.Background(), "foo3", "bar3")
		}()

		i := 0
		for wresp := range rch {
			for _, ev := range wresp.Events {
				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				i++
				if i == 3 {
					// After 3 messages we are done.
					cli.Delete(context.Background(), "foo", etcd.WithPrefix())
					//cli.Close()
					break
				}
			}

			if i == 3 {
				break
			}
		}
	}

	// lock
	{
		// create two separate sessions for lock competition
		s1, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s1.Close()
		m1 := concurrency.NewMutex(s1, "/my-lock")

		s2, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s2.Close()
		m2 := concurrency.NewMutex(s2, "/my-lock")

		// acquire lock for s1
		if err = m1.Lock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("acquired lock for s1")

		if err = m2.TryLock(context.TODO()); err == nil {
			logrus.Fatal("should not acquire lock")
		}
		if err == concurrency.ErrLocked {
			fmt.Println("cannot acquire lock for s2, as already locked in another session")
		}

		if err = m1.Unlock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("released lock for s1")
		if err = m2.TryLock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("acquired lock for s2")
		if err = m2.Unlock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("release lock for s2")
	}

	{
		// create two separate sessions for lock competition
		s1, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s1.Close()
		m1 := concurrency.NewMutex(s1, "/my-lock/")

		s2, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s2.Close()
		m2 := concurrency.NewMutex(s2, "/my-lock/")

		// acquire lock for s1
		if err := m1.Lock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("acquired lock for s1")

		m2Locked := make(chan struct{})
		go func() {
			defer close(m2Locked)
			// wait until s1 is locks /my-lock/
			if err := m2.Lock(context.TODO()); err != nil {
				logrus.Fatal(err)
			} else {
				fmt.Println("acquired lock for s2")
			}
		}()

		if err := m1.Unlock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("released lock for s1")

		<-m2Locked
		if err = m2.Unlock(context.TODO()); err != nil {
			logrus.Fatal(err)
		}
		fmt.Println("release lock for s2")
	}

	{
		s1, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s1.Close()
		e1 := concurrency.NewElection(s1, "/my-election/")

		s2, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s2.Close()
		e2 := concurrency.NewElection(s2, "/my-election/")

		// create competing candidates, with e1 initially losing to e2
		var wg sync.WaitGroup
		wg.Add(2)
		electc := make(chan *concurrency.Election, 2)
		go func() {
			defer wg.Done()
			// delay candidacy so e2 wins first
			time.Sleep(3 * time.Second)
			if err := e1.Campaign(context.Background(), "e1"); err != nil {
				logrus.Fatal(err)
			}
			electc <- e1
		}()
		go func() {
			defer wg.Done()
			if err := e2.Campaign(context.Background(), "e2"); err != nil {
				logrus.Fatal(err)
			}
			electc <- e2
		}()

		cctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		e := <-electc
		fmt.Println("completed first election with", string((<-e.Observe(cctx)).Kvs[0].Value))

		// resign so next candidate can be elected
		if err := e.Resign(context.TODO()); err != nil {
			logrus.Fatal(err)
		}

		e = <-electc
		fmt.Println("completed second election with", string((<-e.Observe(cctx)).Kvs[0].Value))

		wg.Wait()
	}

	{
		const prefix = "/resume-election/"
		var s *concurrency.Session
		s, err = concurrency.NewSession(cli)
		if err != nil {
			logrus.Fatal(err)
		}
		defer s.Close()

		e := concurrency.NewElection(s, prefix)

		// entire test should never take more than 10 seconds
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		// become leader
		if err = e.Campaign(ctx, "candidate1"); err != nil {
			logrus.Fatalf("Campaign() returned non nil err: %s", err)
		}

		// get the leadership details of the current election
		var leader *etcd.GetResponse
		leader, err = e.Leader(ctx)
		if err != nil {
			logrus.Fatalf("Leader() returned non nil err: %s", err)
		}

		// Recreate the election
		e = concurrency.ResumeElection(s, prefix,
			string(leader.Kvs[0].Key), leader.Kvs[0].CreateRevision)

		respChan := make(chan *etcd.GetResponse)
		go func() {
			defer close(respChan)
			o := e.Observe(ctx)
			respChan <- nil
			for resp := range o {
				// Ignore any observations that candidate1 was elected
				if string(resp.Kvs[0].Value) == "candidate1" {
					continue
				}
				respChan <- &resp
				return
			}
			logrus.Error("Observe() channel closed prematurely")
		}()

		// wait until observe goroutine is running
		<-respChan

		// put some random data to generate a change event, this put should be
		// ignored by Observe() because it is not under the election prefix.
		_, err = cli.Put(ctx, "foo", "bar")
		if err != nil {
			logrus.Fatalf("Put('foo') returned non nil err: %s", err)
		}

		// resign as leader
		if err := e.Resign(ctx); err != nil {
			logrus.Fatalf("Resign() returned non nil err: %s", err)
		}

		// elect a different candidate
		if err := e.Campaign(ctx, "candidate2"); err != nil {
			logrus.Fatalf("Campaign() returned non nil err: %s", err)
		}

		// wait for observed leader change
		resp := <-respChan

		kv := resp.Kvs[0]
		if !strings.HasPrefix(string(kv.Key), prefix) {
			logrus.Errorf("expected observed election to have prefix '%s' got %q", prefix, string(kv.Key))
		}
		if string(kv.Value) != "candidate2" {
			logrus.Errorf("expected new leader to be 'candidate1' got %q", string(kv.Value))
		}

		fmt.Println("GOOD")
	}

	{
		rch := cli.Watch(context.Background(), "ddfoo", etcd.WithProgressNotify())
		closedch := make(chan bool)
		go func() {
			// This assumes that cluster is configured with frequent WatchProgressNotifyInterval
			// e.g. WatchProgressNotifyInterval: 200 * time.Millisecond.
			time.Sleep(time.Second)
			err := cli.Close()
			if err != nil {
				logrus.Fatal(err)
			}
			close(closedch)
		}()
		wresp := <-rch
		fmt.Println("wresp.IsProgressNotify:", wresp.IsProgressNotify())
		<-closedch
	}

}
