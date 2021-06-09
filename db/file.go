package pitbase

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	. "github.com/stevegt/goadapt"
)

// file modes
const (
	NEW   = 0
	READ  = 0444
	WRITE = 0644
)

type WORM struct {
	Db *Db
	*Path
	_mode os.FileMode
	fh    *os.File
	hash  hash.Hash
}

func CreateWORM(db *Db, class string, algo string) (file *WORM, err error) {
	defer Return(&err)
	file = &WORM{}
	file.Db = db
	// we don't call Path.New() here 'cause we don't want it to
	// try to parse the empty Raw field
	file.Path = &Path{Class: class, Algo: algo}
	file.Mode(WRITE)
	// Set file.hash so file.Write() can feed new data blocks into the
	// hash algorithm.
	switch file.Path.Algo {
	case "sha256":
		file.hash = sha256.New()
	case "sha512":
		file.hash = sha512.New()
	default:
		err := fmt.Errorf("%w: %s", syscall.ENOSYS, file.Path.Algo)
		return nil, err
	}
	return
}

func OpenWORM(db *Db, path *Path) (file *WORM, err error) {
	defer Return(&err)
	file = &WORM{}
	file.Db = db
	file.Path = path
	ErrnoIf(len(file.Path.Abs) == 0, syscall.EINVAL, "empty path")
	ErrnoIf(!exists(file.Path.Abs), syscall.ENOENT, "not found: %s", file.Path.Abs)
	file.Mode(READ)
	return
}

// gets called by Read(), Write(), etc.
func (file *WORM) ckopen() (err error) {
	defer Return(&err)

	if file.IsOpen() {
		return
	}
	switch file.Mode() {
	case WRITE:
		// open temporary file
		file.fh, err = file.Db.tmpFile()
		Ck(err)
		// write file header
		header := []byte(file.header())
		n, err := file.fh.Write(header)
		Ck(err)
		Assert(n == len(header))
		// add header to hash data to help keep us from accidentally
		// writing a cyrtographic hash reverser
		n, err = file.hash.Write(header)
		Ck(err)
		Assert(n == len(header))
	case READ:
		// open existing file
		file.fh, err = os.Open(file.Path.Abs)
		Ck(err)
		// strip file header
		header := file.header()
		buf := make([]byte, len(header))
		n, err := file.fh.Read(buf)
		Ck(err)
		if n != len(header) || string(buf) != header {
			return fmt.Errorf("malformed header: %s file: %s", string(buf), file.Path.Abs)
		}
	default:
		Assert(false)
	}
	return
}

func (file *WORM) Close() (err error) {
	defer Return(&err)
	switch file.Mode() {
	case NEW, READ:
		if file.fh == nil {
			return
		}
		// no err check needed because readonly
		file.fh.Close()
		// log.Debugf("file Close() returning %v for %#v", err, file)
		file.fh = nil
		return
	case WRITE:
		Assert(file.fh != nil, "writeable file handle is nil: %#v %#v\n", file, file.Path)

		// this one was writeable, so check err
		err = file.fh.Close()
		Ck(err)

		// finish computing hash
		binhash := file.hash.Sum(nil)
		hexhash := bin2hex(binhash)

		// now that we know what the data's hash is, we can replace tmp
		// Path with permanent Path
		Assert(file.Path.Class != "")
		Assert(file.Path.Algo != "")
		canpath := fmt.Sprintf("%s/%s/%s", file.Path.Class, file.Path.Algo, hexhash)
		file.Path = Path{}.New(file.Db, canpath)

		// make sure subdirs exist
		dir, _ := filepath.Split(file.Path.Abs)
		err = os.MkdirAll(dir, 0755)
		Ck(err)

		// rename temp file to permanent blob file
		err = os.Rename(file.fh.Name(), file.Path.Abs)
		Ck(err)

		file.Mode(READ)

		log.Debugf("file Close() returning %v for %v", err, file.fh.Name())
		file.fh = nil
		return
	}
	return
}

func (file *WORM) IsOpen() (ok bool) {
	if file.fh == nil {
		return false
	}
	_, err := file.fh.Seek(0, io.SeekCurrent)
	return err == nil
}

func (file *WORM) Mode(newmode ...os.FileMode) (oldmode os.FileMode) {
	Assert(len(newmode) < 2)
	oldmode = file._mode
	if len(newmode) > 0 {
		file._mode = newmode[0]
		if exists(file.Path.Abs) {
			err := os.Chmod(file.Path.Abs, file._mode)
			Ck(err)
		}
	}
	return
}

// Read reads from the file and puts the data into `buf`, returning n
// as the number of bytes read.  Supports the io.Reader interface.
func (file *WORM) Read(buf []byte) (n int, err error) {
	defer Return(&err)
	file.Mode(READ)
	err = file.ckopen()
	Ck(err)
	return file.fh.Read(buf)
}

// XXX deprecate
func (file *WORM) ReadAll() (buf []byte, err error) {
	defer Return(&err)
	err = file.ckopen()
	Ck(err)
	for {
		b := make([]byte, 4096)
		n, err := file.fh.Read(b)
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		buf = append(buf, b[:n]...)
	}
	return
}

func (file *WORM) Rewind() error {
	_, err := file.Seek(0, 0)
	return err
}

// Seek moves the cursor position `b.pos` to `n`, using
// os.File.Seek():  Seek sets the offset for the next Read
// or Write on file to offset, interpreted according to `whence`: 0
// means relative to the origin of the file, 1 means relative to the
// current offset, and 2 means relative to the end.  It returns the
// new offset and an error, if any.  Supports the io.Seeker interface.
//
// Size(), Seek(), etc. act as if the file content doesn't include the
// header.  In  other words, a caller of Seek(), Size(), or Tell()
// doesn't need to know the size of the file header, and doesn't need
// to know that the file header exists at all -- these functions
// operate on the file body data only.
func (file *WORM) Seek(n int64, whence int) (nout int64, err error) {
	defer Return(&err)

	file.Mode(READ)
	err = file.ckopen()
	Ck(err)

	// add header length offset to n to get file seek position
	hl := int64(len(file.header()))
	var pos int64
	switch whence {
	case io.SeekStart:
		pos = n + hl
	case io.SeekCurrent:
		tellpos, err := file.fh.Seek(0, io.SeekCurrent)
		Ck(err)
		pos = n + tellpos
		whence = io.SeekStart
	case io.SeekEnd:
		pos = n
	default:
		Assert(false)
	}

	// do the seek
	nout, err = file.fh.Seek(pos, whence)
	Ck(err)
	// don't let callers seek backwards into header
	Assert(nout >= 0)
	// subtract the header length to get blob seek position
	nout -= hl

	return
}

func (file *WORM) Size() (n int64, err error) {
	file.Mode(READ)
	info, err := os.Stat(file.Path.Abs)
	if err != nil {
		return
	}
	hl := int64(len(file.header()))
	n = info.Size() - hl
	return
}

// Tell returns the current seek position (the current value of
// `b.pos`) in the file.
func (file *WORM) Tell() (n int64, err error) {
	// call Seek(0, 1)
	return file.Seek(0, io.SeekCurrent)
}

// Write takes data from `data` and puts it into the file named
// file.Path.Abs.  Large blobs can be written using multiple Write()
// calls.  Supports the io.Writer interface.
func (file *WORM) Write(data []byte) (n int, err error) {

	if file.Mode() == READ {
		err = fmt.Errorf("cannot write to existing object: %s", file.Path.Abs)
		return
	}

	err = file.ckopen()
	if err != nil {
		return
	}

	// add data to hash digest
	n, err = file.hash.Write(data)
	if err != nil {
		return
	}

	// write data to disk file
	n, err = file.fh.Write(data)
	if err != nil {
		// panic(fmt.Sprintf("fh: %#v\n", file.fh))
		return
	}

	return
}
