package pitbase

import (
	"fmt"
	"os"
	"syscall"
)

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
