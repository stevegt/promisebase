package pitbase

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	. "github.com/stevegt/goadapt"

	"github.com/google/renameio"
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
// number of subdirectory levels in the blob and node trees.  We use
// three-character hexadecimal names for the subdirectories, giving us
// a maximum of 4096 subdirs in a parent dir -- that's a sweet spot.
// Two-character names (such as what git uses under .git/objects) only
// allow for 256 subdirs, which is unnecessarily small.
// Four-character names would give us 65,536 subdirs, which would
// cause performance issues on e.g. ext4.
type Db struct {
	Dir     string          // base of tree
	Depth   int             // number of subdir levels in blob and node trees
	Poly    resticRabin.Pol // rabin polynomial for chunking
	MinSize uint            // minimum chunk size
	MaxSize uint            // maximum chunk size
}

// Object is a data item stored in a Db; includes blob, node, and
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
	case "node":
		file, err := File{}.New(db, path)
		Ck(err)
		return Node{}.New(db, file), nil
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

func (db *Db) Stat(path string) (info os.FileInfo, err error) {
	fullpath := filepath.Join(db.Dir, path)
	return os.Stat(fullpath)
}

func (db *Db) Size(path string) (size int64, err error) {
	info, err := db.Stat(path)
	if err != nil {
		return
	}
	size = info.Size()
	return
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

	// we store references to nodes as stream symlinks
	err = mkdir(filepath.Join(dir, "stream"))
	Ck(err)

	// we store merkle tree nodes in node
	err = mkdir(filepath.Join(dir, "node"))
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
			err := fmt.Errorf("not implemented: %s", file.Path.Algo)
			return nil, err
		}
	}

	return &file, nil
}

// gets called by Read(), Write(), etc.
func (file *File) ckopen() (err error) {
	if file.fh != nil {
		return
	}
	if !file.Readonly {
		// open temporary file
		file.fh, err = file.Db.tmpFile()
		if err != nil {
			return
		}
	} else {
		// open existing file
		file.fh, err = os.Open(file.Path.Abs)
		if err != nil {
			return
		}
	}
	return
}

func (file *File) Close() (err error) {
	if file.Readonly {
		err = file.fh.Close()
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
		return
	}

	// rename temp file to permanent blob file
	err = os.Rename(file.fh.Name(), abspath)
	if err != nil {
		return
	}

	return
}

// Read reads from the file and puts the data into `buf`, returning n
// as the number of bytes read.  If `buf` is too small to fit all of
// the data, we update b.pos so the next Read() can continue where we
// left off.  Returns io.EOF err when all data has already been
// returned by previous Read() calls.  Supports the io.Reader
// interface.
func (file *File) Read(buf []byte) (n int, err error) {
	err = file.ckopen()
	if err != nil {
		return
	}
	return file.fh.Read(buf)
}

func (file *File) ReadAll() (buf []byte, err error) {
	buf, err = ioutil.ReadFile(file.Path.Abs)
	if err != nil {
		return
	}
	return
}

// Seek moves the cursor position `b.pos` to `n`, using
// os.File.Seek():  Seek sets the offset for the next Read
// or Write on file to offset, interpreted according to `whence`: 0
// means relative to the origin of the file, 1 means relative to the
// current offset, and 2 means relative to the end.  It returns the
// new offset and an error, if any.  Supports the io.Seeker interface.
func (file *File) Seek(n int64, whence int) (nout int64, err error) {
	err = file.ckopen()
	if err != nil {
		return
	}
	return file.fh.Seek(n, whence)
}

func (file *File) Size() (n int64, err error) {
	info, err := os.Stat(file.Path.Abs)
	if err != nil {
		return
	}
	n = info.Size()
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

type Blob struct {
	Db *Db
	*File
}

func (blob *Blob) GetPath() *Path {
	return blob.Path
}

func (blob Blob) New(db *Db, file *File) *Blob {
	blob.Db = db
	blob.File = file
	return &blob
}

// GetBlob retrieves an entire blob into buf by reading its file contents.
func (db *Db) GetBlob(path *Path) (buf []byte, err error) {
	file, err := File{}.New(db, path)
	if err != nil {
		return nil, err
	}
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

// AppendBlob puts a blob in the database, appends it to the Merkle
// tree as a new leaf node, and then rewrites the stream label's symlink
// to point at the new tree root.
func (stream *Stream) AppendBlob(algo string, buf []byte) (newstream *Stream, err error) {
	oldrootnode := stream.RootNode
	newrootnode, err := oldrootnode.AppendBlob(algo, buf)
	if err != nil {
		return
	}

	// rewrite symlink
	noderel := filepath.Join("..", newrootnode.Path.Rel)
	linkabs := filepath.Join(stream.Db.Dir, stream.Path.Canon)
	err = renameio.Symlink(noderel, linkabs)
	if err != nil {
		return
	}
	newstream = Stream{}.New(stream.Db, stream.Label, newrootnode)
	return

}

// AppendBlob puts a blob in the database, appends it to the node's
// Merkle tree as a new leaf node, and returns the new root node.
// This function can be used to append new records or blocks to journals
// or files in accounting, trading, version control, blockchain, and file
// storage applications.
// XXX refactor for streaming, or add an AppendBlobStream
func (node *Node) AppendBlob(algo string, buf []byte) (newrootnode *Node, err error) {
	oldrootnode := node

	// put blob
	blob, err := node.Db.PutBlob(algo, buf)

	// put node for new root of merkle tree
	newrootnode, err = node.Db.PutNode(algo, oldrootnode, blob)
	if err != nil {
		return
	}
	return
}

// PutStream reads blobs from stream, creates a merkle tree with those
// blobs as leaf nodes, and returns the root node of the new tree.
// XXX needs to accept label arg
func (db *Db) PutStream(algo string, rd io.Reader) (rootnode *Node, err error) {
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
	var oldnode *Node
	for {
		chunk, err := chunker.Next(buf)
		if errors.Cause(err) == io.EOF {
			log.Debugf("EOF")
			break
		}
		if err != nil {
			return nil, err
		}

		newblobnode, err := db.PutBlob(algo, chunk.Data)
		if err != nil {
			return nil, err
		}

		log.Debugf("newblobnode %v", newblobnode)
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
		log.Debugf("rootnode %v", rootnode)
		oldnode = rootnode
	}
	log.Debugf("oldnode %v", oldnode)

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

// LinkStream makes a symlink named label pointing at node, and returns
// the resulting stream object.
// XXX do we need this?  creating the stream with rootnode == nil is risky
func (node *Node) LinkStream(label string) (stream *Stream, err error) {
	stream = Stream{}.New(node.Db, label, node)
	src := filepath.Join("..", node.Path.Rel)
	// XXX sanitize label
	linkabspath := filepath.Join(node.Db.Dir, "stream", label)
	log.Debugf("linkabspath %#v", linkabspath)
	err = renameio.Symlink(src, linkabspath)
	if err != nil {
		return
	}
	return
}

// Stream is an ordered set of bytes of arbitrary (but not infinite)
// length.  It implements the io.ReadWriteCloser interface so a
// Stream acts like a file from the perspective of a caller.
// XXX Either (A) stop exporting Node and Blob, and have callers only
// see Stream, or (B) be prepared to expose nodes and blobs to open
// market operations, and redefine `address` to include blobs as well
// as nodes.
type Stream struct {
	Db          *Db
	RootNode    *Node
	Label       string
	Path        *Path
	chunker     *Rabin
	currentBlob *Blob
	posInBlob   int64
}

func (stream Stream) New(db *Db, label string, rootnode *Node) *Stream {
	stream.Db = db
	stream.Label = label
	stream.RootNode = rootnode
	linkrelpath := filepath.Join("stream", label)
	stream.Path = Path{}.New(db, linkrelpath)
	return &stream
}

// OpenStream returns an existing Stream object given a label
// XXX figure out how to collapse OpenStream and Stream.New
// into one function, probably by deferring any disk I/O in OpenStream
// until we hit a Read() or Write().
// XXX likewise for MkBlob and MkNode
func (db *Db) OpenStream(label string) (stream *Stream, err error) {
	// XXX sanitize label
	linkabspath := filepath.Join(db.Dir, "stream", label)
	nodeabspath, err := filepath.EvalSymlinks(linkabspath)
	if err != nil {
		return
	}
	nodepath := Path{}.New(db, nodeabspath)
	log.Debugf("nodeabspath %#v nodepath %#v", nodeabspath, nodepath)
	rootnode, err := db.GetNode(nodepath)
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

// Ls lists all of the leaf nodes in a stream and optionally both
// leaf and inner
func (stream *Stream) Ls(all bool) (objects []Object, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	return stream.RootNode.traverse(all)
}

// Cat concatenates all of the leaf node content in World and returns
// it as a pointer to a byte slice.
func (stream *Stream) Cat() (buf []byte, err error) {
	return stream.RootNode.Cat()
}

// Cat concatenates all of the leaf node content in node's tree and returns
// it all as a pointer to a byte slice.
// XXX rework for streaming
func (node *Node) Cat() (buf []byte, err error) {
	defer Return(&err)

	// db := node.Db

	// get leaf nodes
	objects, err := node.traverse(false)
	Ck(err)

	// append leaf node content to buf
	buf = []byte{}
	for _, obj := range objects {
		var content []byte
		blob, ok := obj.(*Blob)
		if !ok {
			panic("assertion failure: blob type")
		}
		content, err = blob.ReadAll()
		Ck(err)
		buf = append(buf, content...)
	}
	return
}

// Verify hashes the node content and compares it to its address
// XXX move to File
// XXX refactor to take advantage of streaming
// XXX right now we only verify nodes by default -- what about blobs?
func (node *Node) Verify() (ok bool, err error) {
	objects, err := node.traverse(true)
	if err != nil {
		return
	}
	for _, obj := range objects {
		switch child := obj.(type) {
		case *Blob:
			// XXX add a verify flag to GetBlob and do this there
			path := child.Path
			content, err := child.Db.GetBlob(path)
			if err != nil {
				return false, err
			}
			// hash content
			binhash, err := Hash(path.Algo, content)
			if err != nil {
				return false, err
			}
			// compare hash with path.Hash
			hex := bin2hex(binhash)
			if path.Hash != hex {
				log.Debugf("verify failure path %v content '%s'", path.Abs, content)
				return false, fmt.Errorf("expected %v, calculated %v", path.Hash, hex)
			}
		case *Node:
			path := child.Path
			log.Debugf("child %#v", child)
			_, err := node.Db.getNode(path, true)
			if err != nil {
				return false, err
			}
		default:
			panic(fmt.Sprintf("unhandled type %T", child))
		}
	}
	return true, nil
}

// traverse recurses down the tree of nodes returning leaves or optionally all nodes
func (node *Node) traverse(all bool) (objects []Object, err error) {
	defer Return(&err)

	if node.File == nil {
		file, err := File{}.New(node.Db, node.Path)
		Ck(err)
		node.File = file
	}

	if node.entries == nil {
		err = node.loadEntries()
		Ck(err)
	}

	if all {
		objects = append(objects, node)
	}

	log.Debugf("traverse node %#v", node)
	for _, obj := range *node.entries {
		log.Debugf("traverse obj %#v", obj)
		switch child := obj.(type) {
		case *Node:
			childobjs, err := child.traverse(all)
			if err != nil {
				return nil, err
			}
			objects = append(objects, childobjs...)
		case *Blob:
			objects = append(objects, obj)
		default:
			panic(fmt.Sprintf("unhandled type %T", child))
		}
	}

	return
}

func exists(path string) (found bool) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

type Path struct {
	Db    *Db
	Raw   string
	Abs   string // absolute
	Rel   string // relative
	Canon string // canonical
	Class string
	Algo  string
	Hash  string
	Addr  string
	Label string // stream label
}

func (path Path) New(db *Db, raw string) (res *Path) {
	path.Db = db
	path.Raw = raw

	// XXX need to also or instead call some sort of realpath function
	// here to deal with symlinks that might exist in the db.Dir path
	clean := filepath.Clean(raw)

	// remove db.Dir
	index := strings.Index(clean, path.Db.Dir)
	if index == 0 {
		clean = strings.Replace(clean, path.Db.Dir+"/", "", 1)
	}

	// split into parts
	parts := strings.Split(clean, "/")
	if len(parts) < 2 {
		panic(fmt.Errorf("malformed path: %s", raw))
	}
	path.Class = parts[0]
	if path.Class == "stream" {
		path.Label = filepath.Join(parts[1:]...)
		path.Rel = filepath.Join(path.Class, path.Label)
		path.Abs = filepath.Join(path.Db.Dir, path.Rel)
		path.Canon = path.Rel
	} else {
		if len(parts) < 3 {
			panic(fmt.Errorf("malformed path: %s", raw))
		}
		path.Algo = parts[1]
		// the last part of the path should always be the full hash,
		// regardless of whether we were given the full or canonical
		// path
		path.Hash = parts[len(parts)-1]
		// log.Debugf("anypath %#v class %#v algo %#v hash %#v", anypath, class, algo, hash)

		// Rel is the relative path of any type of input path.  We
		// use the nesting depth described in the Db comments.  We use the
		// full hash value in the last component of the path in order to make
		// troubleshooting using UNIX tools slightly easier (in contrast to
		// the way git truncates the leading subdir parts of the hash).
		var subpath string
		for i := 0; i < path.Db.Depth; i++ {
			subdir := path.Hash[(3 * i):((3 * i) + 3)]
			subpath = filepath.Join(subpath, subdir)
		}
		path.Rel = filepath.Join(path.Class, path.Algo, subpath, path.Hash)
		path.Abs = filepath.Join(path.Db.Dir, path.Rel)
		path.Canon = filepath.Join(path.Class, path.Algo, path.Hash)
		// Addr is a universally-unique address for the data stored at path.
		path.Addr = filepath.Join(path.Algo, path.Hash)
	}

	return &path
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

// Node is a vertex in a Merkle tree. Entries point at leafs or other nodes.
type Node struct {
	Db *Db
	*File
	entries *[]Object
}

func (node Node) New(db *Db, file *File) *Node {
	node.Db = db
	node.File = file
	return &node
}

func (node *Node) GetPath() *Path {
	return node.Path
}

// Txt returns the concatenated node entries
func (node *Node) Txt() (out string) {
	for _, entry := range *node.entries {
		out += strings.TrimSpace(entry.GetPath().Canon) + "\n"
	}
	return
}

// bin2hex converts byte slice into hex string
func bin2hex(bin []byte) (hex string) {
	hex = fmt.Sprintf("%x", bin)
	return
}

// PutNode takes one or more child nodes, stores relpaths in a file under node/,
// and returns a pointer to a Node object.
func (db *Db) PutNode(algo string, children ...Object) (node *Node, err error) {
	defer Return(&err)

	Assert(db != nil, "db is nil")

	path := &Path{Class: "node", Algo: algo}
	file, err := File{}.New(db, path)
	Ck(err)
	node = Node{}.New(db, file)

	// populate the entries field
	node.entries = &children
	// concatenate all relpaths together (include the full canpath with
	// the 'blob/' or 'node/' prefix to help protect against preimage
	// attacks)
	// XXX refactor for streaming
	buf := []byte(node.Txt())

	var n int
	n, err = node.Write(buf)
	Ck(err)
	Assert(n == len(buf), "short write")
	err = node.Close()
	Ck(err)

	return
}

// GetNode takes a node path and returns a Node struct
func (db *Db) GetNode(path *Path) (node *Node, err error) {
	return db.getNode(path, true)
}

// XXX do we ever take advantage of verify == false?  where should we?
// XXX reconcile with OpenNode
func (db *Db) getNode(path *Path, verify bool) (node *Node, err error) {
	defer Return(&err)

	file, err := File{}.New(db, path)
	Ck(err)
	defer file.Close()

	node = Node{}.New(db, file)

	err = node.loadEntries()
	Ck(err)

	return
}

func (node *Node) loadEntries() (err error) {
	defer Return(&err)

	Assert(node.File != nil)
	file := node.File
	scanner := bufio.NewScanner(file)
	var content []byte
	var entries []Object
	for scanner.Scan() {
		buf := scanner.Bytes()
		line := string(buf)
		line = strings.TrimSpace(line)
		path := Path{}.New(node.Db, line)
		entry, err := node.Db.ObjectFromPath(path)
		Ck(err)
		log.Debugf("entry %#v", entry)
		entries = append(entries, entry)

		content = append(content, buf...)
		content = append(content, '\n')
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	node.entries = &entries

	/*
		// XXX merge this with Verify
		if verify {
			// hash content
			binhash, err := Hash(path.Algo, content)
			if err != nil {
				return node, err
			}
			// compare hash with path.Hash
			hex := bin2hex(binhash)
			if path.Hash != hex {
				log.Debugf("getNode verify failure path %v content '%s'", path.Abs, content)
				err = fmt.Errorf("expected %v, calculated %v", path.Hash, hex)
				return node, err
			}
		}
	*/

	log.Debugf("getNode node.entries %#v", node.entries)
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
func (s Stream) XXX() (rootnode *Node, err error) {

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
