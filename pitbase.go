package pitbase

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// Db is a key-value database
type Db struct {
	Dir string
	// inode Inode
	locknode Inode
}

// Inode contains various file-related items such as file descriptor,
// file handle, maybe some methods, etc.
type Inode struct {
	fd   uintptr
	fh   *os.File
	path string
	key  *Key
}

func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
	logrus.SetReportCaller(true)
	formatter := &logrus.TextFormatter{
		CallerPrettyfier: caller(),
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyFile: "caller",
		},
	}
	formatter.TimestampFormat = "15:04:05.999999999"
	logrus.SetFormatter(formatter)
}

// caller returns string presentation of log caller which is formatted as
// `/path/to/file.go:line_number`. e.g. `/internal/app/api.go:25`
// https://stackoverflow.com/questions/63658002/is-it-possible-to-wrap-logrus-logger-functions-without-losing-the-line-number-pr
func caller() func(*runtime.Frame) (function string, file string) {
	return func(f *runtime.Frame) (function string, file string) {
		p, _ := os.Getwd()
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, getGID())
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
	// The blob dir is where we store hashed blobs
	err = mkdir(fmt.Sprintf("%s/blob", dir))
	if err != nil {
		return
	}

	// we store references to hashed blobs in refs
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

	// create a lock file
	// XXX move openKey() guts into an inode constructor and
	// call that here
	db.locknode = Inode{
		path: filepath.Join(dir, ".lock"),
	}
	err = touch(db.locknode.path)
	if err != nil {
		return
	}
	db.locknode.fh, err = os.OpenFile(db.locknode.path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	db.locknode.fd = db.locknode.fh.Fd()

	return
}

func touch(path string) error {
	return ioutil.WriteFile(path, []byte(""), 0644)
}

/*
// Put creates a file for each key and assigns a value in each file.
func (db *Db) XXXPut(key []byte, val []byte) (err error) {
	// enter critical section: lock entire db
	locknode, err = db.ExLock()
	if err != nil {
		return err
	}
	defer locknode.Unlock()
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
func (db *Db) XXXGet(key []byte) (val []byte, err error) {
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
*/

func (db *Db) XXXopenKey(key *Key, flag int) (inode Inode, err error) {
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

func (db *Db) lock(locktype int) (inode *Inode, err error) {
	log.Debugf("db.lock starting db %+v fd %v locktype %v", db, db.locknode.fd, locktype)
	fh, err := os.OpenFile(db.locknode.path, os.O_RDONLY, 0644)
	inode = &Inode{fd: fh.Fd()}
	err = syscall.Flock(int(inode.fd), locktype)
	log.Debugf("db.lock finishing, err=%#v", err)
	// log.Debug(string(debug.Stack()))
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
func (db *Db) ExLock() (inode *Inode, err error) {
	return db.lock(syscall.LOCK_EX)
}

// ShLock uses syscall.Flock to get a shared lock (LOCK_SH) on
// the database
func (db *Db) ShLock() (inode *Inode, err error) {
	return db.lock(syscall.LOCK_SH)
}

func (db *Db) tmpFile() (inode Inode, err error) {
	return tmpFile(db.Dir)
}

func tmpFile(dir string) (inode Inode, err error) {
	inode.fh, err = ioutil.TempFile(dir, "*")
	if err != nil {
		return
	}
	inode.path = inode.fh.Name()
	inode.fd = inode.fh.Fd()
	return
}

// PutNoLock creates a temporary file for a key and then atomically renames to the permanent path.
func (db *Db) PutNoLock(key *Key, val *[]byte) (err error) {

	// get temporary file
	inode, err := db.tmpFile()
	defer inode.Close()

	// write to temp file
	_, err = inode.fh.Write(*val)
	if err != nil {
		return err
	}

	// get permanent pathname for key
	path := db.Path(key)

	dir, _ := filepath.Split(path)
	err = os.MkdirAll(dir, 0755)
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

// GetNoLock retrieves the value of a key by reading its file contents.
func (db *Db) GetNoLock(key *Key) (val []byte, err error) {
	val, err = ioutil.ReadFile(db.Path(key))
	if err != nil {
		return
	}
	return
}

//RmNoLock removes... without a lock..
func (db *Db) RmNoLock(key *Key) (err error) {
	err = os.Remove(db.Path(key))
	if err != nil {
		return err
	}
	return
}

// PutBlob hashes the blob if needed, stores the blob in a file named after the hash,
// and returns the hash.
func (db *Db) PutBlob(algo string, blob *[]byte) (key *Key, err error) {
	key, err = KeyFromBlob(algo, blob)
	if err != nil {
		return
	}

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
func (db *Db) GetBlob(algo string, key *Key) (val []byte, err error) {
	val, err = db.GetNoLock(key)
	return
}

// PutRef creates a file, named ref, that contains the given key.
// XXX deprecate in favor of tx.PutRef
func (db *Db) PutRef(key *Key, ref string) (err error) {
	dir := filepath.Join(db.Dir, "refs")
	return putref(dir, key, ref)
}

// GetRef takes a reference, parses the ref file, and returns the algorithm and key.
func (db *Db) GetRef(ref string) (key *Key, err error) {
	dir := filepath.Join(db.Dir, "refs")
	return getref(dir, ref)
}

// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) Path(key *Key) (path string) {
	path = filepath.Join(db.Dir, key.String())
	return
}

// RefPath takes a reference name and returns the pathname of the file
// containing the reference.
func (db *Db) RefPath(ref string) (path string) {
	dir := fmt.Sprintf("%s/refs", db.Dir)
	return refPath(dir, ref)
}

func refPath(dir, ref string) (path string) {
	path = fmt.Sprintf("%s/%s", dir, ref)
	return
}

type Transaction struct {
	Db  *Db
	dir string
}

// StartTransaction atomically creates a copy-on-write copy of the ref directory.
func (db *Db) StartTransaction() (tx *Transaction, err error) {

	// make atomic by getting a shared lock
	locknode, err := db.ExLock()
	if err != nil {
		return
	}
	defer locknode.Unlock()

	// clone the refs/ directory by creating a new temporary directory as a subdirectory of tx/
	// https://golang.org/pkg/io/ioutil/#TempDir
	tmpdir, err := ioutil.TempDir(filepath.Join(db.Dir, "tx"), "")
	if err != nil {
		return
	}
	tx = &Transaction{Db: db, dir: tmpdir}
	log.Debug("transaction started in ", tmpdir)

	refdir := filepath.Join(db.Dir, "refs")

	// hard-link all of the contents of refs into tmpdir, including any subdirs
	// https://golang.org/pkg/path/filepath/#Walk
	hardlink := func(path string, info os.FileInfo, inerr error) (err error) {
		if inerr != nil {
			log.Debug("inerr ", inerr)
			return inerr
		}
		// make sure that path is in refdir
		index := strings.Index(path, refdir)
		if index != 0 {
			err = fmt.Errorf("index: expected 0, got %d", index)
			return
		}
		// we need to replace the first part of path with tmpdir
		// for example, if path is var/refs/foo and tmpdir is var/tx/123
		// then newpath needs to be var/tx/123/foo
		newpath := strings.Replace(path, refdir, tmpdir, 1)

		if info.IsDir() {
			log.Debug("mkdir ", newpath)
			err = os.MkdirAll(newpath, 0755)
			if err != nil {
				return
			}
		} else {
			log.Debug("linking path ", path, " newpath ", newpath)
			err = os.Link(path, newpath)
			if err != nil {
				if !exists(path) {
					panic("path missing")
				}
				if !exists(tmpdir) {
					panic("tmpdir missing")
				}
				return
			}
		}
		return
	}

	err = filepath.Walk(refdir, hardlink)

	return
}

func exists(parts ...string) (found bool) {
	path := filepath.Join(parts...)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// PutRef creates a file in tx.Dir that contains the given key.
func (tx *Transaction) PutRef(key *Key, ref string) (err error) {
	return putref(tx.dir, key, ref)
}

// GetRef takes a reference, parses the ref file, and returns the key.
func (tx *Transaction) GetRef(ref string) (key *Key, err error) {
	return getref(tx.dir, ref)
}

func putref(dir string, key *Key, ref string) (err error) {
	// get temporary file
	inode, err := tmpFile(dir)
	defer inode.Close()

	// write to temp file
	_, err = inode.fh.Write([]byte(fmt.Sprintf("%s", key)))
	if err != nil {
		return err
	}

	// get permanent pathname for ref
	path := refPath(dir, ref)

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

func getref(dir string, ref string) (key *Key, err error) {
	path := refPath(dir, ref)
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	key, err = KeyFromPath(string(buf))
	if err != nil {
		return
	}
	return
}

// Commit atomically renames the content of tx.Dir into db.Dir.
// XXX last commit wins
func (tx *Transaction) Commit() (err error) {

	// make atomic by getting an exclusive lock
	locknode, err := tx.Db.ExLock()
	if err != nil {
		return
	}
	defer locknode.Unlock()

	refdir := filepath.Join(tx.Db.Dir, "refs")

	// rename all of the contents, including any subdirs
	// https://golang.org/pkg/path/filepath/#Walk
	rename := func(path string, info os.FileInfo, inerr error) (err error) {

		log.Debug(path)
		// ensure path is in tx.dir
		index := strings.Index(path, tx.dir)
		if index != 0 {
			err = fmt.Errorf("index: expected 0, got %d", index)
			return
		}
		// to generate newpath, we need to rename the first part of path with refdir.
		// for example, if path is var/tx/123/foo, tx.dir is var/tx/123 and refdir is var/refs/
		// then newpath needs to be var/refs/foo
		newpath := strings.Replace(path, tx.dir, refdir, 1)

		if info.IsDir() {
			err = os.MkdirAll(newpath, 0755)
			if err != nil {
				return
			}
		} else {
			log.Debug("start renaming path ", path, " newpath ", newpath)
			err = os.Rename(path, newpath)
			log.Debug("finish renaming path ", path, " newpath ", newpath)
			if err != nil {
				return
			}
		}
		return
	}

	err = filepath.Walk(tx.dir, rename)
	//XXX remove files in tx after rename

	return

}

// Key is a relative path to an object.  An object is a blob, tree, or
// ref.
type Key struct {
	Class string
	Algo  string
	Hash  string
}

func (k Key) String() string {
	return filepath.Join(k.Class, k.Algo, k.Hash)
}

func KeyFromPath(path string) (key *Key, err error) {
	parts := strings.Split(path, "/")
	key = &Key{
		Class: parts[0],
		Algo:  parts[1],
		Hash:  parts[2],
	}
	/*
		// convert ascii hex string to binary bytes
		decodedlen := hex.DecodedLen(len(hexkey))
		binhash = make([]byte, decodedlen)
		n, err := hex.Decode(binhash, []byte(hexkey))
		if err != nil {
			return
		}
		if n != decodedlen {
			err = fmt.Errorf(
				"expected %d, got %d when decoding", decodedlen, n)
			if err != nil {
				return
			}
		}
	*/
	return
}

func KeyFromString(algo string, s string) (key *Key, err error) {
	blob := []byte(s)
	return KeyFromBlob(algo, &blob)
}

// KeyFromBlob takes a class, algo, and blob and returns a populated Key object
func KeyFromBlob(algo string, blob *[]byte) (key *Key, err error) {
	var binhash []byte
	switch algo {
	case "sha256":
		d := sha256.Sum256(*blob)
		binhash = make([]byte, len(d))
		copy(binhash[:], d[0:len(d)])
	case "sha512":
		d := sha512.Sum512(*blob)
		binhash = make([]byte, len(d))
		copy(binhash[:], d[0:len(d)])
	default:
		err = fmt.Errorf("not implemented: %s", algo)
		return
	}
	key = &Key{
		Class: "blob",
		Algo:  algo,
		Hash:  fmt.Sprintf("%x", binhash),
	}
	return
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
