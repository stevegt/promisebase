package pitbase

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
)

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

	// rename temp file to key file
	dir := fmt.Sprintf("%s/refs", db.Dir)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.Mkdir(dir, 0755)
		if err != nil {
			return
		}
	}
	err = os.Rename(inode.path, path)
	if err != nil {
		return
	}

	return
}

// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) Path(key []byte) (path string) {
	path = fmt.Sprintf("%s/%x", db.Dir, key)
	return
}

// RefPath takes a reference name and returns the pathname of the file
// containing the reference.
func (db *Db) RefPath(ref string) (path string) {
	path = fmt.Sprintf("%s/refs/%s", db.Dir, ref)
	return
}
