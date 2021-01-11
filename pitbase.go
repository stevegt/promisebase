package pitbase

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
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

// ExLock uses syscall.Flock to get an exclusive lock (LOCK_EX) on the
// file referenced by `key`.
func (db *Db) ExLock(key []byte) (fd uintptr, err error) {
	fh, err := os.Open(fmt.Sprintf("%s/%s", db.Dir, string(key)))
	if err != nil {
		return
	}
	fd = fh.Fd()
	syscall.Flock(int(fd), syscall.LOCK_EX)
	return
}

// Unlock uses syscall.Flock to unlock (LOCK_UN) the file referenced
// by `key`.
func (db *Db) Unlock(fd uintptr) (err error) {
	syscall.Flock(int(fd), syscall.LOCK_UN)
	return
}
