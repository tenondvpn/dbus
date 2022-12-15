package dbus

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func test_main() {
	db, err := leveldb.OpenFile("./db", nil)
	defer db.Close()

	data, err := db.Get([]byte("key"), nil)
	if err != nil {
		fmt.Println("get data failed!")
	} else {
		fmt.Printf("get data success: %s!\n", data)
	}

	err = db.Put([]byte("key"), []byte("value"), nil)
	if err != nil {
		fmt.Println("Put data failed!")
	}

	data, err = db.Get([]byte("key"), nil)
	if err != nil {
		fmt.Println("get data failed!")
	} else {
		fmt.Printf("get data success: %s!\n", string(data))
	}

	err = db.Delete([]byte("key"), nil)
}
