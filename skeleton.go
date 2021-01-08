package pitbase

// ExLock uses syscall.Flock to get an exclusive lock (LOCK_EX) on the
// file referenced by `key`.
func (db *Db) ExLock(key []byte) (err error) {
	return
}

// Unlock uses syscall.Flock to unlock (LOCK_UN) the file referenced
// by `key`.
func (db *Db) Unlock(key []byte) (err error) {
	return
}
