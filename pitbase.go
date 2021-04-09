package pitbase

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	. "github.com/stevegt/goadapt"

	"github.com/pkg/errors"
	resticRabin "github.com/restic/chunker"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

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
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, GetGID())
	}
}

// Db is a key-value database. Dir is the base directory. Depth is the
// number of subdirectory levels in the blob and tree dirs.  We use
// three-character hexadecimal names for the subdirectories, giving us
// a maximum of 4096 subdirs in a parent dir -- that's a sweet spot.
// Two-character names (such as what git uses under .git/objects) only
// allow for 256 subdirs, which is unnecessarily small.
// Four-character names would give us 65,536 subdirs, which would
// cause performance issues on e.g. ext4.
type Db struct {
	Dir     string          // base of tree
	Depth   int             // number of subdir levels in blob and tree dirs
	Poly    resticRabin.Pol // rabin polynomial for chunking
	MinSize uint            // minimum chunk size
	MaxSize uint            // maximum chunk size
}

// Object is a data item stored in a Db; includes blob, tree, and
// stream.
type Object interface {
	Read(buf []byte) (n int, err error)
	Write(data []byte) (n int, err error)
	Seek(n int64, whence int) (nout int64, err error)
	Tell() (n int64, err error)
	Close() (err error)
	Size() (n int64, err error)
	GetPath() (path *Path)
}

func (db *Db) ObjectFromPath(path *Path) (obj Object, err error) {
	defer Return(&err)

	class := path.Class
	switch class {
	case "blob":
		file, err := File{}.New(db, path)
		Ck(err)
		return Blob{}.New(db, file), nil
	case "tree":
		file, err := File{}.New(db, path)
		Ck(err)
		return Tree{}.New(db, file), nil
	default:
		Assert(false, "unhandled class %s", class)
	}
	return
}

func mkdir(dir string) (err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755) // XXX perms too open?
		if err != nil {
			return
		}
	}
	return
}

type ExistsError struct {
	Dir string
}

func (e *ExistsError) Error() string {
	return fmt.Sprintf("directory not empty: %s", e.Dir)
}

// Create initializes a db directory and its contents
func (db Db) Create() (out *Db, err error) {
	defer Return(&err)

	dir := db.Dir

	// if directory exists, make sure it's empty
	if canstat(dir) {
		var files []os.FileInfo
		files, err = ioutil.ReadDir(dir)
		if len(files) > 0 {
			return nil, &ExistsError{Dir: dir}
		}
		Ck(err)
	}

	// set nesting depth
	if db.Depth < 1 {
		db.Depth = 2
	}

	err = mkdir(dir)
	Ck(err)

	// The blob dir is where we store hashed blobs
	err = mkdir(filepath.Join(dir, "blob"))
	Ck(err)

	// we store references to trees as stream symlinks
	err = mkdir(filepath.Join(dir, "stream"))
	Ck(err)

	// we store merkle tree nodes in tree
	err = mkdir(filepath.Join(dir, "tree"))
	Ck(err)

	if db.Poly == 0 {
		db.Poly, err = resticRabin.RandomPolynomial()
		Ck(err)
	}

	buf, err := json.Marshal(db)
	Ck(err)
	err = ioutil.WriteFile(filepath.Join(dir, "config.json"), buf, 0644)
	Ck(err)

	return &db, nil
}

type NotDbError struct {
	Dir string
}

func (e *NotDbError) Error() string {
	return fmt.Sprintf("not a database: %s", e.Dir)
}

// Open loads an existing db object from dir.
func Open(dir string) (db *Db, err error) {
	dir = filepath.Clean(dir)

	if !canstat(dir) {
		return nil, fmt.Errorf("cannot open: %s", dir)
	} else if err != nil {
		return
	}

	// load config
	buf, err := ioutil.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil, &NotDbError{Dir: dir}
	}
	db = &Db{}
	err = json.Unmarshal(buf, db)
	if err != nil {
		return
	}

	return
}

func touch(path string) error {
	return ioutil.WriteFile(path, []byte(""), 0644)
}

func (db *Db) tmpFile() (fh *os.File, err error) {
	dir := db.Dir
	fh, err = ioutil.TempFile(dir, "*")
	if err != nil {
		return
	}
	return
}

// GetBlob retrieves an entire blob into buf by reading its file contents.
func (db *Db) GetBlob(path *Path) (buf []byte, err error) {
	file, err := File{}.New(db, path)
	if err != nil {
		return nil, err
	}
	file.Readonly = true
	return Blob{}.New(db, file).ReadAll()
}

// Rm deletes the file associated with a path of any format and returns an error
// if the file doesn't exist.
func (db *Db) Rm(path *Path) (err error) {
	err = os.Remove(path.Abs)
	if err != nil {
		return err
	}
	return
}

// PutStream reads blobs from stream, creates a merkle tree with those
// blobs as leaf nodes, and returns the root node of the new tree.
// XXX needs to accept label arg
func (db *Db) PutStream(algo string, rd io.Reader) (rootnode *Tree, err error) {
	// set chunker parameters
	chunker, err := Rabin{Poly: db.Poly, MinSize: db.MinSize, MaxSize: db.MaxSize}.Init()
	if err != nil {
		return
	}

	// create a chunker
	// XXX should be called e.g. New()
	chunker.Start(rd)

	// feed rd into chunker until rd hits EOF
	// XXX hardcoded buffer size of 1 MB, might want to make this configurable
	// XXX buffer size really only needs to be slightly larger than the max chunk size,
	// XXX which we should be able to get out of the rabin struct
	buf := make([]byte, chunker.MaxSize+1) // this might be wrong
	var oldtree *Tree
	for {
		chunk, err := chunker.Next(buf)
		if errors.Cause(err) == io.EOF {
			log.Debugf("EOF")
			break
		}
		if err != nil {
			return nil, err
		}

		newblob, err := db.PutBlob(algo, chunk.Data)
		if err != nil {
			return nil, err
		}

		log.Debugf("newblob %v", newblob)
		if oldtree == nil {
			// we're just starting the tree
			rootnode, err = db.PutTree(algo, newblob)
			if err != nil {
				return nil, err
			}
		} else {
			// add the next node
			rootnode, err = db.PutTree(algo, oldtree, newblob)
			if err != nil {
				return nil, err
			}
		}
		log.Debugf("rootnode %v", rootnode)
		oldtree = rootnode
	}
	log.Debugf("oldtree %v", oldtree)

	return
}

// PutBlob hashes the blob, stores the blob in a file named after the hash,
// and returns the blob object.
func (db *Db) PutBlob(algo string, buf []byte) (b *Blob, err error) {
	defer Return(&err)

	Assert(db != nil, "db is nil")

	path := &Path{Algo: algo, Class: "blob"}
	file, err := File{}.New(db, path)
	Ck(err)
	b = Blob{}.New(db, file)

	var n int
	n, err = b.Write(buf)
	Ck(err)
	Assert(n == len(buf), "short write")
	err = b.Close()
	Ck(err)

	return
}

// OpenStream returns an existing Stream object given a label
// XXX figure out how to collapse OpenStream and Stream.New
// into one function, probably by deferring any disk I/O in OpenStream
// until we hit a Read() or Write().
// XXX likewise for MkBlob and MkTree
func (db *Db) OpenStream(label string) (stream *Stream, err error) {
	// XXX sanitize label
	linkabspath := filepath.Join(db.Dir, "stream", label)
	treeabspath, err := filepath.EvalSymlinks(linkabspath)
	if err != nil {
		return
	}
	treepath := Path{}.New(db, treeabspath)
	log.Debugf("treeabspath %#v treepath %#v", treeabspath, treepath)
	rootnode, err := db.GetTree(treepath)
	if err != nil {
		return
	}
	if rootnode == nil {
		panic("rootnode is nil")
	}
	log.Debugf("OpenStream rootnode %#v", rootnode)
	stream = Stream{}.New(db, label, rootnode)
	return
}
func exists(path string) (found bool) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

/*
func hex2bin (hexkey string) (binhash []byte) {
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
}
*/

// Hash returns the hash of a blob using a given algorithm
// XXX rework to support streaming
func Hash(algo string, buf []byte) (hash []byte, err error) {
	var binhash []byte
	switch algo {
	case "sha256":
		d := sha256.Sum256(buf)
		binhash = make([]byte, len(d))
		copy(binhash[:], d[0:len(d)])
	case "sha512":
		d := sha512.Sum512(buf)
		binhash = make([]byte, len(d))
		copy(binhash[:], d[0:len(d)])
	default:
		err = fmt.Errorf("not implemented: %s", algo)
		return
	}
	return binhash, nil
}

// GetGID returns the goroutine ID of its calling function, for logging purposes.
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// bin2hex converts byte slice into hex string
func bin2hex(bin []byte) (hex string) {
	hex = fmt.Sprintf("%x", bin)
	return
}

// PutTree takes one or more child nodes, stores relpaths in a file
// under tree/,
// and returns a pointer to a Tree object.
func (db *Db) PutTree(algo string, children ...Object) (tree *Tree, err error) {
	defer Return(&err)

	Assert(db != nil, "db is nil")

	path := &Path{Class: "tree", Algo: algo}
	file, err := File{}.New(db, path)
	Ck(err)
	tree = Tree{}.New(db, file)

	// populate the entries field
	tree.entries = &children
	// concatenate all relpaths together (include the full canpath with
	// the 'blob/' or 'tree/' prefix to help protect against preimage
	// attacks)
	// XXX refactor for streaming
	buf := []byte(tree.Txt())

	var n int
	n, err = tree.Write(buf)
	Ck(err)
	Assert(n == len(buf), "short write")
	err = tree.Close()
	Ck(err)

	return
}

// GetTree takes a tree path and returns a Tree struct
func (db *Db) GetTree(path *Path) (tree *Tree, err error) {
	return db.getTree(path, true)
}

// XXX do we ever take advantage of verify == false?  where should we?
// XXX reconcile with OpenTree
func (db *Db) getTree(path *Path, verify bool) (tree *Tree, err error) {
	defer Return(&err)

	file, err := File{}.New(db, path)
	Ck(err)
	defer file.Close()

	tree = Tree{}.New(db, file)

	err = tree.loadEntries()
	Ck(err)

	return
}

func pretty(x interface{}) string {
	b, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

func canstat(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

/*
func (s Stream) Init() *Stream {

	return &s
}

// XXX this is not the right signature
func (s Stream) XXX() (rootnode *Tree, err error) {

	// XXX  We will need an io.Pipe() somewhere near here to solve the
	// mismatch between us wanting to be an io.Writer, and restic's
	// chunker wanting to be an io.Reader
	//
	// i.e. We need to write a wrapper around restic's chunker
	// library.  The wrapper acts as an adapter.  The purpose of the
	// adapter is to provide an io.Writer interface (a Write() method)
	// to callers, so callers can simply write to the chunker.  There
	// may already be a library on github that does this; there may
	// already be something somewhere in the restic user's repos.  If
	// not, we may want to write it in a standalone repo, and publish
	// it ourselves.
	//
	// example pseudo-code for what a test case for this might look
	// like:
	//
	// chunker := ChunkerWrapper{}.Init()
	// io.Copy(chunker, os.Stdin) // copy bytes from stdin to the chunker
	//

	// chunk it
	// XXX not sure where this should go
	// s.chunker.Start(rd) // XXX 's' is not the right stream -- maybe what goes here is the other side of the io.Pipe()

	db := s.Db
	algo := s.Algo
	chunker := s.chunker
	// XXX hardcoded buffer size of 1 MB, might want to make this configurable
	// XXX buffer size really only needs to be slightly larger than the max chunk size,
	// XXX which we should be able to get out of the rabin struct
	buf := make([]byte, chunker.MaxSize+1) // this might be wrong
	var oldnode *Node
	for {
		chunk, err := chunker.Next(buf)
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		key, err := db.PutBlob(algo, &chunk.Data)
		if err != nil {
			return nil, err
		}
		newblobnode := &Node{Db: db, Key: key}

		if oldnode == nil {
			// we're just starting the tree
			rootnode, err = db.PutNode(algo, newblobnode)
			if err != nil {
				return nil, err
			}
		} else {
			// add the next node
			rootnode, err = db.PutNode(algo, oldnode, newblobnode)
			if err != nil {
				return nil, err
			}
		}
		oldnode = rootnode
	}

	return
}

// Write writes up to len(buf) bytes from buf to the database.  It
// returns the number of bytes written.  (Write is guaranteed to return
// only after writing all bytes from buf or after encountering an
// error, so `n` can be safely ignored.
func (s *Stream) Write(buf []byte) (n int, err error) {
	n = len(buf)
	// if RootNode is null, then call db.PutBlob and db.PutNode
	if s.RootNode == nil {

//		// set up chunker
//		db := s.Db
//		s.chunker, err = Rabin{Poly: db.Poly, MinSize: db.MinSize, MaxSize: db.MaxSize}.Init()
//		if err != nil {
//			return
//		}
//
//		// XXX stuff missing here, and the rest of this function is
//		// probably wrong still
//		if true {
//			return 0, io.EOF
//		}

		key, err := s.Db.PutBlob(s.Algo, &buf)
		if err != nil {
			return n, err
		}
		blobnode := &Node{Db: s.Db, Key: key}
		s.RootNode, err = s.Db.PutNode(s.Algo, blobnode)
		if err != nil {
			return n, err
		}
	} else {
		// else call RootNode.AppendBlob() and update RootNode
		s.RootNode, err = s.RootNode.AppendBlob(s.Algo, &buf)
	}
	return
}

// Read reads up to len(p) bytes from the database into buf.  It
// returns the number of bytes read.
func (s *Stream) Read(buf []byte) (n int, err error) {

	// read the next chunk from currentBlob and update posInBlob

	// XXX
	err = io.EOF

	return
}

func (s *Stream) Seek(offset int64, whence int) (newOffset int64, err error) {
	// XXX
	return
}

func (s *Stream) Close() (err error) {
	// do we need to do anything here?  flush?

	return
}
*/
