package pitbase

import (
	"fmt"
	"io/ioutil"
	"os"
)

// Db is a key-value database
type Db struct {
	Dir string
}

// Open takes a directory name as input and returns a db object
func Open(dir string) (db Db, err error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return db, err
		}
	}
	db.Dir = dir
	return
}

func (db Db) Put(key []byte, val []byte) (err error) {
	fn := fmt.Sprintf("%s/%s", db.Dir, string(key))
	err = ioutil.WriteFile(fn, val, 0644)
	if err != nil {
		return err
	}
	return
}

func (db Db) Get(key []byte) (val []byte, err error) {
	fn := fmt.Sprintf("%s/%s", db.Dir, string(key))
	val, err = ioutil.ReadFile(fn)
	if err != nil {
		return val, err
	}
	return
}
