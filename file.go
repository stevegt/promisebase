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

type File struct {
	Db *Db
	*Path
	Readonly bool
	fh       *os.File
	hash     hash.Hash
}

func (file File) New(db *Db, path *Path) (*File, error) {
	file.Db = db
	file.Path = path
	if file.Path == nil {
		// we don't call Path.New() here 'cause we don't want it to
		// try to parse the empty Raw field
		file.Path = &Path{}
	}
	if file.Path.Algo == "" {
		// we default to "sha256" here, but callers can e.g. specify algo
		// for a new blob via something like Blob{File{Path{Algo: "sha512"}}}
		// XXX default should come from a DefaultAlgo field in Db config
		file.Path.Algo = "sha256"
	}

	// Detect whether this invocation of New is for an existing disk
	// file, or for a new one that hasn't been written yet.  In the
	// latter case, we need to set file.hash so file.Write() can feed
	// new data blocks into the hash algorithm.
	if len(file.Path.Abs) > 0 && exists(file.Path.Abs) {
		// use existing file
		file.Readonly = true
	} else {
		// we're creating a new file -- initialize hash engine
		switch file.Path.Algo {
		case "sha256":
			file.hash = sha256.New()
		case "sha512":
			file.hash = sha512.New()
		default:
			err := fmt.Errorf("%w: %s", syscall.ENOSYS, file.Path.Algo)
			return nil, err
		}
	}

	return &file, nil
}

// gets called by Read(), Write(), etc.
func (file *File) ckopen() (err error) {
	defer Return(&err)

	if file.IsOpen() {
		return
	}
	if !file.Readonly {
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
	} else {
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
	}
	return
}

func (file *File) Close() (err error) {
	if file.Readonly {
		err = file.fh.Close()
		log.Debugf("file Close() returning %v for %#v", err, file)
		file.fh = nil
		return
	}

	// move tmpfile to perm

	// close disk file
	file.fh.Close()

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
	abspath := file.Path.Abs
	dir, _ := filepath.Split(abspath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		log.Debugf("file Close() returning %v for mkdir dir %v", err, dir)
		return
	}

	// rename temp file to permanent blob file
	err = os.Rename(file.fh.Name(), abspath)
	if err != nil {
		log.Debugf("file Close() returning %v rename %v to %v", err, abspath, file.fh.Name())
		return
	}

	log.Debugf("file Close() returning %v for %v", err, file.fh.Name())
	file.fh = nil
	return
}

func (file *File) IsOpen() (ok bool) {
	if file.fh == nil {
		return false
	}
	_, err := file.fh.Seek(0, io.SeekCurrent)
	// _, nok := err.(*fs.PathError)
	return err == nil
}

// Read reads from the file and puts the data into `buf`, returning n
// as the number of bytes read.  Supports the io.Reader interface.
func (file *File) Read(buf []byte) (n int, err error) {
	defer Return(&err)
	file.Readonly = true
	err = file.ckopen()
	Ck(err)
	return file.fh.Read(buf)
}

// XXX deprecate
func (file *File) ReadAll() (buf []byte, err error) {
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

func (file *File) Rewind() error {
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
func (file *File) Seek(n int64, whence int) (nout int64, err error) {
	file.Readonly = true
	err = file.ckopen()
	if err != nil {
		return
	}
	hl := int64(len(file.header()))
	var pos int64
	switch whence {
	case 0:
		pos = n + hl
	case 1:
		pos = n
	case 2:
		pos = n
	default:
		Assert(false)
	}
	nout, err = file.fh.Seek(pos, whence)
	nout -= hl
	return
}

func (file *File) Size() (n int64, err error) {
	file.Readonly = true
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
func (file *File) Tell() (n int64, err error) {
	// call Seek(0, 1)
	return file.Seek(0, io.SeekCurrent)
}

// Write takes data from `data` and puts it into the file named
// file.Path.Abs.  Large blobs can be written using multiple Write()
// calls.  Supports the io.Writer interface.
func (file *File) Write(data []byte) (n int, err error) {

	if file.Readonly {
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
