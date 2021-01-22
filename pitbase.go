package pitbase

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

// Db is a key-value database
type Db struct {
	Dir   string
	inode Inode
}

// Inode contains various file-related items such as file descriptor,
// file handle, maybe some methods, etc.
type Inode struct {
	fd   uintptr
	fh   *os.File
	path string
	key  []byte
}

func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
}

func mkdir(dir string) (err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.Mkdir(dir, 0755) // XXX perms too open?
		if err != nil {
			return
		}
	}
	return
}

// Open creates a db object and its directory (if one doesn't already exist)
func Open(dir string) (db *Db, err error) {
	db = &Db{}
	err = mkdir(dir)
	if err != nil {
		return
	}

	// XXX use filepath.Join() for any Sprintf that's doing something like this
	// The objects dir is where we store hashed objects
	err = mkdir(fmt.Sprintf("%s/objects", dir))
	if err != nil {
		return
	}

	// we store references to hashed objects in refs
	err = mkdir(fmt.Sprintf("%s/refs", dir))
	if err != nil {
		return
	}

	// we store transactions (temporary copy on write copies of the refs dir) in tx
	err = mkdir(fmt.Sprintf("%s/tx", dir))
	if err != nil {
		return
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
	defer inode.Close()
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
	defer inode.Close()
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
	defer inode.Close()
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
	inode.path = db.Path(key)
	inode.fh, err = os.OpenFile(inode.path, flag, 0644)
	if err != nil {
		return
	}
	inode.fd = inode.fh.Fd()
	return
}

// Close closes an inode
func (inode *Inode) Close() (err error) {
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

func (db *Db) tmpFile() (inode Inode, err error) {
	inode.fh, err = ioutil.TempFile(db.Dir, "*")
	if err != nil {
		return
	}
	inode.path = inode.fh.Name()
	inode.fd = inode.fh.Fd()
	return
}

// PutNoLock creates a temporary file for a key and then atomically renames to the permanent path.
func (db *Db) PutNoLock(key []byte, val []byte) (err error) {

	// get temporary file
	inode, err := db.tmpFile()
	defer inode.Close()

	// write to temp file
	_, err = inode.fh.Write(val)
	if err != nil {
		return err
	}

	// get permanent pathname for key
	path := db.Path(key)

	// rename temp file to key file
	err = os.Rename(inode.path, path)
	if err != nil {
		return
	}

	return
}

// GetNoLock retrieves the value of a key by reading its file contents.
func (db *Db) GetNoLock(key []byte) (val []byte, err error) {
	inode, err := db.openKey(key, os.O_RDONLY)
	if err != nil {
		return
	}
	defer inode.Close()
	val, err = ioutil.ReadAll(inode.fh)
	if err != nil {
		return
	}
	return
}

//RmNoLock removes... without a lock..
func (db *Db) RmNoLock(key []byte) (err error) {
	inode, err := db.openKey(key, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer inode.Close()
	err = os.Remove(inode.path)
	if err != nil {
		return err
	}
	return
}

// PutBlob hashes the blob if needed, stores the blob in a file named after the hash,
// and returns the hash.
func (db *Db) PutBlob(algo string, blob []byte) (key []byte, err error) {
	key = Hash(algo, blob)

	// check if it's already stored
	// XXX

	// store it
	err = db.PutNoLock(key, blob)
	if err != nil {
		return
	}
	return
}

// GetBlob returns the content of the file referenced by key
func (db *Db) GetBlob(algo string, key []byte) (val []byte, err error) {
	val, err = db.GetNoLock(key)
	return
}

// Hash takes a blob and returns a hash of it using a given algorithm
func Hash(algo string, blob []byte) (key []byte) {

	// hash blob using algo
	switch algo {
	case "sha256":
		k := sha256.Sum256(blob)
		key = make([]byte, len(k))
		log.Debugf("k type: %T, k length: %d, k value: %x", k, k, k)
		copy(key[:], k[0:len(k)])
		log.Debugf("finished sha256 case, key %v, k %v", key, k)
	case "sha512":
		k := sha512.Sum512(blob)
		copy(key, k[:])
	default:
		fmt.Errorf("not implemented: %s", algo)
	}
	return
}

// PutRef creates a file, named ref, that contains the given key.
// XXX deprecate in favor of tx.PutRef
func (db *Db) PutRef(algo string, key []byte, ref string) (err error) {
	// get temporary file
	inode, err := db.tmpFile()
	defer inode.Close()

	// write to temp file
	_, err = inode.fh.Write([]byte(fmt.Sprintf("%s:%x", algo, key)))
	if err != nil {
		return err
	}

	// get permanent pathname for ref
	path := db.RefPath(ref)

	// make a directory from pathname
	dirpath, _ := filepath.Split(path)
	err = os.MkdirAll(dirpath, 0755)
	if err != nil {
		return
	}

	// rename temp file to key file
	err = os.Rename(inode.path, path)
	if err != nil {
		return
	}

	return
}

// GetRef takes a reference, parses the ref file, and returns the algorithm and key.
func (db *Db) GetRef(ref string) (algo string, key []byte, err error) {
	// use RefPath to get path to file
	path := db.RefPath(ref)
	// read file XXX see last half of TestPutRef for ideas
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	gotfullref := string(buf)
	// parse out algo and key
	refparts := strings.Split(gotfullref, ":")
	algo = refparts[0]
	hexkey := refparts[1]
	// convert ascii hex string to binary bytes
	decodedlen := hex.DecodedLen(len(hexkey))
	key = make([]byte, decodedlen)
	n, err := hex.Decode(key, []byte(hexkey))
	if err != nil {
		return
	}
	if n != decodedlen {
		err = fmt.Errorf(
			"expected %d, got %d when decoding", decodedlen, n)
	}
	return
}

// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) Path(key []byte) (path string) {
	path = fmt.Sprintf("%s/objects/%x", db.Dir, key)
	return
}

// RefPath takes a reference name and returns the pathname of the file
// containing the reference.
func (db *Db) RefPath(ref string) (path string) {
	path = fmt.Sprintf("%s/refs/%s", db.Dir, ref)
	return
}

type Transaction struct {
	Db  *Db
	dir string
}

// StartTransaction atomically creates a copy-on-write copy of the ref directory.
func (db *Db) StartTransaction() (tx *Transaction, err error) {

	// make atomic by getting a shared lock
	defer db.Unlock()
	err = db.ShLock()
	if err != nil {
		return
	}

	// clone the refs/ directory by creating a new temporary directory as a subdirectory of tx/
	// https://golang.org/pkg/io/ioutil/#TempDir
	tmpdir, err := ioutil.TempDir(filepath.Join(db.Dir, "tx"), "")
	if err != nil {
		return
	}
	tx = &Transaction{Db: db, dir: tmpdir}
	// hard-link all of the contents of refs into tmpdir, including any subdirs
	// https://golang.org/pkg/path/filepath/#Walk
	refdir := filepath.Join(db.Dir, "refs")
	if err != nil {
		return
	}

	hardlink := func(path string, info os.FileInfo, err error) {
		// newpath := XXX use tmpdir here
		// err := os.Link(path, newpath)
	}

	err = filepath.Walk(".", hardlink)

	return
}

// PutRef creates a file in tx.Dir that contains the given key.
func (tx *Transaction) PutRef(algo string, key []byte, ref string) (err error) {
	// XXX move most of db.PutRef into func putref(dir, algo, key, ref) and
	// call it from db.PutRef and tx.PutRef
	return
}

// Commit atomically renames the content of tx.Dir into db.Dir.
// XXX last commit wins
func (tx *Transaction) Commit() (err error) {

	// make atomic by getting an exclusive lock
	defer tx.Db.Unlock()
	err = tx.Db.ExLock()
	if err != nil {
		return
	}

	// rename all of the contents, including any subdirs
	// https://golang.org/pkg/path/filepath/#Walk
	// err = os.Rename(inode.path, path)

	return

}

type Key struct {
	Algo string
	Bin  []byte
	Hex  string
}

// KeyFromBlob takes an algo and blob and returns a populated Key object
func KeyFromBlob(algo string, val []byte) (key *Key) {
	return
}
