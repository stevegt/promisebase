package pitbase

// ShLock uses syscall.Flock to get a shared lock (LOCK_SH) on the
// file referenced by `key`.
func (db *Db) ShLock(key []byte) (fd uintptr, err error) {
	return
}
