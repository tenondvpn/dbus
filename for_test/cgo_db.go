package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	levigo "github.com/jmhodges/levigo"
	"github.com/sirupsen/logrus"
)

// export CGO_CFLAGS="-I/opt/homebrew/Cellar/leveldb/1.23/include -I/opt/homebrew/Cellar/snappy/1.1.9/include"

// export CGO_LDFLAGS="-L/opt/homebrew/Cellar/leveldb/1.23/lib -L/opt/homebrew/Cellar/snappy/1.1.9/lib -lsnappy"
// export LD_LIBRARY_PATH=/opt/homebrew/Cellar/leveldb/1.23/lib:/opt/homebrew/Cellar/snappy/1.1.9/lib:$LD_LIBRARY_PATH

func CheckGet(where string, db *levigo.DB, roptions *levigo.ReadOptions, key, expected []byte) {
	getValue, err := db.Get(roptions, key)

	if err != nil {
		logrus.Errorf("%s, Get failed: %v", where, err)
	}
	if !bytes.Equal(getValue, expected) {
		logrus.Errorf("%s, expected Get value %v, got %v", where, expected, getValue)
	}
	logrus.Infof("%s, expected Get value %v, got %v", where, expected, getValue)
}

func WBIterCheckEqual(where string, which string, pos int, expected, given []byte) {
	if !bytes.Equal(expected, given) {
		logrus.Errorf("%s at pos %d, %s expected: %v, got: %v", where, pos, which, expected, given)
	}
}

func CheckIter(it *levigo.Iterator, key, value []byte) {
	if !bytes.Equal(key, it.Key()) {
		logrus.Errorf("Iterator: expected key %v, got %v", key, it.Key())
	}
	if !bytes.Equal(value, it.Value()) {
		logrus.Errorf("Iterator: expected value %v, got %v", value, it.Value())
	}
}

func deleteDBDirectory(dirPath string) {
	err := os.RemoveAll(dirPath)
	if err != nil {
		logrus.Errorf("Unable to remove database directory: %s", dirPath)
	}
}

func tempDir() string {
	bottom := fmt.Sprintf("levigo-test-%d", rand.Int())
	path := filepath.Join(os.TempDir(), bottom)
	deleteDBDirectory(path)
	return path
}

func main() {
	if levigo.GetLevelDBMajorVersion() <= 0 {
		logrus.Errorf("Major version cannot be less than zero")
	}

	dbname := tempDir()
	defer deleteDBDirectory(dbname)
	env := levigo.NewDefaultEnv()
	cache := levigo.NewLRUCache(1 << 20)

	options := levigo.NewOptions()
	// options.SetComparator(cmp)
	policy := levigo.NewBloomFilter(10)
	options.SetFilterPolicy(policy)
	options.SetErrorIfExists(true)
	options.SetCache(cache)
	options.SetEnv(env)
	options.SetInfoLog(nil)
	options.SetWriteBufferSize(1 << 20)
	options.SetParanoidChecks(true)
	options.SetMaxOpenFiles(10)
	options.SetBlockSize(1024)
	options.SetBlockRestartInterval(8)
	options.SetCompression(levigo.NoCompression)

	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(false)
	roptions.SetFillCache(false)

	woptions := levigo.NewWriteOptions()
	woptions.SetSync(false)

	_ = levigo.DestroyDatabase(dbname, options)

	fmt.Println(0)
	db, err := levigo.Open(dbname, options)
	if err == nil {
		logrus.Errorf("Open on missing db should have failed")
	}

	fmt.Println(1)
	options.SetCreateIfMissing(true)
	db, err = levigo.Open(dbname, options)
	if err != nil {
		logrus.Errorf("Open failed: %v", err)
	}

	btime := TimeStampMilli()
	prevIdx := 0
	data := make([]byte, 1024*10)
	for i := 0; i < 10000000; i++ {
		putKey := []byte("foo_" + fmt.Sprintf("%d", i))
		err = db.Put(woptions, putKey, data)
		if err != nil {
			logrus.Errorf("Put failed: %v", err)
		}

		// _, err = db.Get(roptions, putKey)
		// if err != nil {
		// 	logrus.Errorf("Put failed: %v", err)
		// }

		etime := TimeStampMilli()
		if etime-btime >= 1000 {
			btime = etime
			fmt.Printf("qps: %d\n", i-prevIdx)
			prevIdx = i
		}
	}

	for i := 0; i < 10000000; i++ {
		putKey := []byte("foo_" + fmt.Sprintf("%d", i))
		_, err = db.Get(roptions, putKey)
		if err != nil {
			logrus.Errorf("Put failed: %v", err)
		}

		etime := TimeStampMilli()
		if etime-btime >= 1000 {
			btime = etime
			fmt.Printf("qps: %d\n", i-prevIdx)
			prevIdx = i
		}
	}

	// fmt.Println(3)
	// CheckGet("after Put", db, roptions, putKey, putValue)

	// wb := levigo.NewWriteBatch()
	// wb.Put([]byte("foo"), []byte("a"))
	// wb.Clear()
	// wb.Put([]byte("bar"), []byte("b"))
	// wb.Put([]byte("box"), []byte("c"))
	// wb.Delete([]byte("bar"))
	// err = db.Write(woptions, wb)
	// if err != nil {
	// 	logrus.Errorf("Write batch failed: %v", err)
	// }
	// CheckGet("after WriteBatch", db, roptions, []byte("foo"), []byte("hello"))
	// CheckGet("after WriteBatch", db, roptions, []byte("bar"), nil)
	// CheckGet("after WriteBatch", db, roptions, []byte("box"), []byte("c"))
	// // TODO: WriteBatch iteration isn't easy. Suffers same problems as
	// // Comparator.
	// // wbiter := &TestWBIter{t: t}
	// // wb.Iterate(wbiter)
	// // if wbiter.pos != 3 {
	// // 	logrus.Errorf("After Iterate, on the wrong pos: %d", wbiter.pos)
	// // }
	// wb.Close()

	// iter := db.NewIterator(roptions)
	// if iter.Valid() {
	// 	logrus.Errorf("Read iterator should not be valid, yet")
	// }
	// iter.SeekToFirst()
	// if !iter.Valid() {
	// 	logrus.Errorf("Read iterator should be valid after seeking to first record")
	// }
	// CheckIter(iter, []byte("box"), []byte("c"))
	// iter.Next()
	// CheckIter(iter, []byte("foo"), []byte("hello"))
	// iter.Prev()
	// CheckIter(iter, []byte("box"), []byte("c"))
	// iter.Prev()
	// if iter.Valid() {
	// 	logrus.Errorf("Read iterator should not be valid after go back past the first record")
	// }
	// iter.SeekToLast()
	// CheckIter(iter, []byte("foo"), []byte("hello"))
	// iter.Seek([]byte("b"))
	// CheckIter(iter, []byte("box"), []byte("c"))
	// if iter.GetError() != nil {
	// 	logrus.Errorf("Read iterator has an error we didn't expect: %v", iter.GetError())
	// }
	// iter.Close()

	// // approximate sizes
	// n := 20000
	// for i := 0; i < n; i++ {
	// 	keybuf := []byte(fmt.Sprintf("k%020d", i))
	// 	valbuf := []byte(fmt.Sprintf("v%020d", i))
	// 	err := db.Put(woptions, keybuf, valbuf)
	// 	if err != nil {
	// 		logrus.Errorf("Put error in approximate size test: %v", err)
	// 	}
	// }

	// ranges := []levigo.Range{
	// 	{[]byte("a"), []byte("k00000000000000010000")},
	// 	{[]byte("k00000000000000010000"), []byte("z")},
	// }
	// sizes := db.GetApproximateSizes(ranges)
	// if len(sizes) == 2 {
	// 	if sizes[0] <= 0 {
	// 		logrus.Errorf("First size range was %d", sizes[0])
	// 	}
	// 	if sizes[1] <= 0 {
	// 		logrus.Errorf("Second size range was %d", sizes[1])
	// 	}
	// } else {
	// 	logrus.Errorf("Expected 2 approx. sizes back, got %d", len(sizes))
	// }

	// // property
	// prop := db.PropertyValue("nosuchprop")
	// if prop != "" {
	// 	logrus.Errorf("property nosuchprop should not have a value")
	// }
	// prop = db.PropertyValue("leveldb.stats")
	// if prop == "" {
	// 	logrus.Errorf("property leveldb.stats should have a value")
	// }

	// // snapshot
	// snap := db.NewSnapshot()
	// err = db.Delete(woptions, []byte("foo"))
	// if err != nil {
	// 	logrus.Errorf("Delete during snapshot test errored: %v", err)
	// }
	// roptions.SetSnapshot(snap)
	// CheckGet("from snapshot", db, roptions, []byte("foo"), []byte("hello"))
	// roptions.SetSnapshot(nil)
	// CheckGet("from snapshot", db, roptions, []byte("foo"), nil)
	// db.ReleaseSnapshot(snap)

	// // repair
	// db.Close()
	// options.SetCreateIfMissing(false)
	// options.SetErrorIfExists(false)
	// err = levigo.RepairDatabase(dbname, options)
	// if err != nil {
	// 	logrus.Errorf("Repairing db failed: %v", err)
	// }
	// db, err = levigo.Open(dbname, options)
	// if err != nil {
	// 	logrus.Errorf("Unable to open repaired db: %v", err)
	// }
	// CheckGet("repair", db, roptions, []byte("foo"), nil)
	// CheckGet("repair", db, roptions, []byte("bar"), nil)
	// CheckGet("repair", db, roptions, []byte("box"), []byte("c"))
	// options.SetCreateIfMissing(true)
	// options.SetErrorIfExists(true)

	// // filter
	// policy := levigo.NewBloomFilter(10)
	// db.Close()
	// levigo.DestroyDatabase(dbname, options)
	// options.SetFilterPolicy(policy)
	// db, err = levigo.Open(dbname, options)
	// if err != nil {
	// 	logrus.Fatalf("Unable to recreate db for filter tests: %v", err)
	// }
	// err = db.Put(woptions, []byte("foo"), []byte("foovalue"))
	// if err != nil {
	// 	logrus.Errorf("Unable to put 'foo' with filter: %v", err)
	// }
	// err = db.Put(woptions, []byte("bar"), []byte("barvalue"))
	// if err != nil {
	// 	logrus.Errorf("Unable to put 'bar' with filter: %v", err)
	// }
	// db.CompactRange(levigo.Range{nil, nil})
	// CheckGet("filter", db, roptions, []byte("foo"), []byte("foovalue"))
	// CheckGet("filter", db, roptions, []byte("bar"), []byte("barvalue"))
	// options.SetFilterPolicy(nil)
	// policy.Close()

	// cleanup
	db.Close()
	options.Close()
	roptions.Close()
	woptions.Close()
	cache.Close()
	// DestroyComparator(cmp)
	env.Close()
}
