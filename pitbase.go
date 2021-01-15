package pitbase

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"syscall"

	log "github.com/sirupsen/logrus"
)

// Db is a key-value database
type Db struct {
	Dir   string
	inode Inode
}

func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
}

// Open creates a db object and its directory (if one doesn't already exist)
func Open(dir string) (db *Db, err error) {
	db = &Db{}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return db, err
		}
	}
	db.Dir = dir
	db.inode, err = db.openKey([]byte(""), os.O_RDONLY)
	if err != nil {
		return
	}
	db.inode.fd = db.inode.fh.Fd()
	return
}

// Put creates a file for each key and assigns a value in each file.
func (db *Db) Put(key []byte, val []byte) (err error) {
	// enter critical section: lock entire db
	err = db.ExLock()
	if err != nil {
		return err
	}
	defer db.Unlock()
	// get inode
	inode, err := db.openKey(key, os.O_WRONLY|os.O_CREATE)
	if err != nil {
		return err
	}
	defer inode.close()
	// lock inode
	err = inode.ExLock()
	if err != nil {
		return err
	}
	defer inode.Unlock()
	err = syscall.Ftruncate(int(inode.fd), 0)
	if err != nil {
		return err
	}
	_, err = inode.fh.Write(val)
	if err != nil {
		return err
	}

	return
}

// Get retrieves the value of a specific key by reading its file contents.
func (db *Db) Get(key []byte) (val []byte, err error) {
	// lock db
	err = db.ShLock()
	if err != nil {
		return
	}
	defer db.Unlock()
	inode, err := db.openKey(key, os.O_RDONLY)
	if err != nil {
		return
	}
	defer inode.close()
	err = inode.ShLock()
	if err != nil {
		return
	}
	defer inode.Unlock()
	val, err = ioutil.ReadAll(inode.fh)
	if err != nil {
		return
	}
	return
}

// Rm deletes the entry associated with the key and returns an error if the key doesn't exist.
func (db *Db) Rm(key []byte) (err error) {
	inode, err := db.openKey(key, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer inode.close()
	err = inode.ExLock()
	if err != nil {
		return err
	}
	defer inode.Unlock()
	err = os.Remove(inode.path)
	if err != nil {
		return err
	}
	return
}

func (db *Db) openKey(key []byte, flag int) (inode Inode, err error) {
	inode.key = key
	inode.path = fmt.Sprintf("%s/%s", db.Dir, string(key))
	inode.fh, err = os.OpenFile(inode.path, flag, 0644)
	if err != nil {
		return
	}
	inode.fd = inode.fh.Fd()
	return
}

func (inode *Inode) close() (err error) {
	return inode.fh.Close()
}

func (inode *Inode) ilock(locktype int) (err error) {
	log.Debugf("inode.ilock starting %+v:%d", inode, locktype)
	err = syscall.Flock(int(inode.fd), locktype)
	log.Debug("inode.ilock finished")
	return
}

func (db *Db) lock(locktype int) (err error) {
	log.Debugf("db.lock starting %+v:%d", db, locktype)
	err = syscall.Flock(int(db.inode.fd), locktype)
	log.Debug("db.lock finishing")
	log.Debug(string(debug.Stack()))
	return
}

// ExLock uses syscall.Flock to get an exclusive lock (LOCK_EX) on the
// file referenced by `key`.
func (inode *Inode) ExLock() (err error) {
	return inode.ilock(syscall.LOCK_EX)
}

// ShLock uses syscall.Flock to get a shared lock (LOCK_SH) on the
// file referenced by `key`.
func (inode *Inode) ShLock() (err error) {
	return inode.ilock(syscall.LOCK_SH)
}

// Unlock uses syscall.Flock to unlock (LOCK_UN) the file referenced
// by `key`.
func (inode *Inode) Unlock() (err error) {
	log.Debug("inode.Unlock starting")
	err = syscall.Flock(int(inode.fd), syscall.LOCK_UN)
	log.Debug("inode.Unlock finishing")
	return
}

// ExLock uses syscall.Flock to get an exclusive lock (LOCK_EX)
// on the database
func (db *Db) ExLock() (err error) {
	return db.lock(syscall.LOCK_EX)
}

// ShLock uses syscall.Flock to get a shared lock (LOCK_SH) on
// the database
func (db *Db) ShLock() (err error) {
	return db.lock(syscall.LOCK_SH)
}

// Unlock uses syscall.Flock to unlock (LOCK_UN) the database
func (db *Db) Unlock() (err error) {
	log.Debug("db.Unlock starting")
	err = syscall.Flock(int(db.inode.fd), syscall.LOCK_UN)
	log.Debug("db.Unlock finishing")
	return
}
