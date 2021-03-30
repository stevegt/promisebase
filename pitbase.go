package pitbase

import (
	"bufio"
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

type canPath string

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
}

func (db Db) ObjectFromCanPath(canpath string) (obj Object) {
	panic("not implemented")
	return
}

// Inode contains various file-related items such as file descriptor,
// file handle, maybe some methods, etc.
// XXX deprecate in favor of Object
type Inode struct {
	fd      uintptr
	fh      *os.File
	abspath string
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
	dir := db.Dir

	// if directory exists, make sure it's empty
	if canstat(dir) {
		var files []os.FileInfo
		files, err = ioutil.ReadDir(dir)
		if len(files) > 0 {
			return nil, &ExistsError{Dir: dir}
		} else if err != nil {
			return
		}
	}

	// set nesting depth
	if db.Depth < 1 {
		db.Depth = 2
	}

	err = mkdir(dir)
	if err != nil {
		return
	}

	// The blob dir is where we store hashed blobs
	err = mkdir(filepath.Join(dir, "blob"))
	if err != nil {
		return
	}

	// we store references to nodes as stream symlinks
	err = mkdir(filepath.Join(dir, "stream"))
	if err != nil {
		return
	}

	// we store transactions (temporary copy on write copies of the refs dir) in tx
	// err = mkdir(filepath.Join(dir, "tx"))
	if err != nil {
		return
	}

	// we store merkle tree nodes in node
	err = mkdir(filepath.Join(dir, "node"))
	if err != nil {
		return
	}

	if db.Poly == 0 {
		db.Poly, err = resticRabin.RandomPolynomial()
		if err != nil {
			return
		}
	}

	buf, err := json.Marshal(db)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(filepath.Join(dir, "config.json"), buf, 0644)
	if err != nil {
		return
	}

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

// Close closes an inode
func (inode *Inode) Close() (err error) {
	return inode.fh.Close()
}

func (db *Db) tmpFile() (inode Inode, err error) {
	return tmpFile(db.Dir)
}

func tmpFile(dir string) (inode Inode, err error) {
	inode.fh, err = ioutil.TempFile(dir, "*")
	if err != nil {
		return
	}
	inode.abspath = inode.fh.Name()
	inode.fd = inode.fh.Fd()
	return
}

// Put creates a temporary file for a buf and then atomically renames to the permanent path.
// XXX refactor for streaming
func (db *Db) put(path *Path, buf []byte) (err error) {

	// get temporary file
	inode, err := db.tmpFile()
	defer inode.Close()

	// write to temp file
	_, err = inode.fh.Write(buf)
	if err != nil {
		return err
	}

	dir, _ := filepath.Split(path.Abs())
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}
	// rename temp file to permanent file
	err = os.Rename(inode.abspath, path.Abs())
	if err != nil {
		return
	}

	return
}

type Blob struct {
	Db       *Db
	Path     *Path
	fh       *os.File
	Readonly bool
	inode    Inode // XXX get rid of inode dependency so we can deprecate inode?
}

func (b *Blob) Size() (n int64, err error) {
	info, err := os.Stat(b.Path.Abs())
	if err != nil {
		return
	}
	n = info.Size()
	return
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

func (db *Db) OpenBlob(path *Path) (b *Blob, err error) {
	b = &Blob{Db: db, Path: path}
	if exists(path.Abs()) {
		// open existing file
		b.fh, err = os.Open(path.Abs())
		if err != nil {
			return
		}
		b.Readonly = true
	} else {
		// open temporary file
		b.inode, err = db.tmpFile()
		if err != nil {
			return
		}
	}
	return
}

func (b *Blob) Close() (err error) {
	defer b.inode.Close()
	path := b.Path.Rel()
	// mkdir
	dir, _ := filepath.Split(path)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}

	// rename temp file to permanent blob file
	err = os.Rename(b.inode.abspath, path)
	if err != nil {
		return
	}

	return
}

// Write takes data from `data` and puts it into the file named
// b.Path.  Updates pos after each write.  Large blobs might be
// written using multiple Write() calls.  Supports the io.Writer
// interface.

func (b *Blob) Write(data []byte) (n int, err error) {
	// XXX this works for a file that fits in a single write; for
	// larger blobs we need to do the tmpFile() stuff in Init() and we
	// might need a b.Close() function to do the Rename when we're done
	// writing

	// write to temp file
	n, err = b.inode.fh.Write(data)
	if err != nil {
		return
	}

	return
}

// Read reads from the file named b.Path and puts the data into `buf`,
// returning n as the number of bytes read.  If `buf` is too small to
// fit all of the data, we update b.pos so the next Read() can
// continue where we left off.  Returns io.EOF err when all data has
// already been returned by previous Read() calls.  Supports the
// io.Reader interface.
func (b *Blob) Read(buf []byte) (n int, err error) {
	return b.fh.Read(buf)
}

func (b *Blob) ReadAll() (buf []byte, err error) {
	buf, err = ioutil.ReadFile(b.Path.Abs())
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
func (b *Blob) Seek(n int64, whence int) (nout int64, err error) {
	return b.fh.Seek(n, whence)
}

// Tell returns the current seek position (the current value of
// `b.pos`) in the file.
func (b *Blob) Tell() (n int64, err error) {
	// we do this by calling b.Seek(0, 1)
	return b.Seek(0, io.SeekCurrent)
}

// GetBlob retrieves a blob by reading its file contents.
// XXX deprecate
func (db *Db) GetBlob(path *Path) (buf []byte, err error) {
	// XXX streaming: call OpenBlob(), b.Read(), and b.Close()
	buf, err = ioutil.ReadFile(path.Abs())
	if err != nil {
		return
	}
	return
}

// Rm deletes the file associated with a path of any format and returns an error
// if the file doesn't exist.
func (db *Db) Rm(path *Path) (err error) {
	err = os.Remove(path.Abs())
	if err != nil {
		return err
	}
	return
}

// AppendBlob puts a blob in the database, appends it to the Merkle
// tree as a new leaf node, and then rewrites the stream label's symlink
// to point at the new tree root.
func (stream *Stream) AppendBlob(algo string, buf []byte) (newrootnode *Node, err error) {
	oldrootnode := stream.RootNode
	newrootnode, err = oldrootnode.AppendBlob(algo, buf)
	if err != nil {
		return
	}

	// rewrite symlink
	src := filepath.Join("..", stream.Path.Rel())
	err = renameio.Symlink(src, stream.CanPath())
	if err != nil {
		return
	}
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
func (db *Db) PutStream(algo string, stream io.Reader) (rootnode *Node, err error) {
	// setup
	chunker, err := Rabin{Poly: db.Poly, MinSize: db.MinSize, MaxSize: db.MaxSize}.Init()
	if err != nil {
		return
	}

	// chunk it
	chunker.Start(stream)

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

		newblobnode, err := db.PutBlob(algo, chunk.Data)
		if err != nil {
			return nil, err
		}

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

// PutBlob hashes the blob, stores the blob in a file named after the hash,
// and returns the hash.
func (db *Db) PutBlob(algo string, buf []byte) (b *Blob, err error) {

	path, err := db.PathFromBuf(algo, buf)
	if err != nil {
		return
	}

	// check if it's already stored
	_, err = os.Stat(path.Abs())
	if err == nil {
		// noop
	} else if os.IsNotExist(err) {
		// store it
		err = nil // clear IsNotExist err
		var n int
		b, err := db.OpenBlob(path)
		if err != nil {
			return b, err
		}
		n, err = b.Write(buf)
		if err != nil {
			return b, err
		}
		if n != len(buf) {
			// XXX
			panic("short write")
		}
		err = b.Close()
		if err != nil {
			return b, err
		}

	}
	return
}

/*
// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) XXXPath(key *Key) (path string) {
	log.Debugf("db: %v, key: %v", db, key)
	path = filepath.Join(db.Dir, key.Path())
	return
}
*/

/*
// World is a reference to a timeline
type World struct {
	Db   *Db
	Name string
	Key  string
}
*/

/*
// String returns the path for a world
func (world *World) String() (path string) {
	return filepath.Join(world.Db.Dir, "world", world.Name)
}
*/

// LabelStream makes a symlink named label pointing at node, and returns a stream
func (db *Db) LabelStream(node *Node, label string) (stream *Stream, err error) {
	world = &World{Db: db, Label: label}
	src := filepath.Join("..", node.Path.Rel())
	err = renameio.Symlink(src, world.String())
	if err != nil {
		return
	}
	world.Key = key
	return
}

// Stream is an ordered set of bytes of arbitrary (but not infinite)
// length.  It implements the io.ReadWriteCloser interface so a
// Stream acts like a file from the perspective of a caller.
type Stream struct {
	Db          *Db
	Algo        string
	RootNode    *Node
	Label       string
	Path        *Path
	chunker     *Rabin
	currentBlob *Blob
	posInBlob   int64
}

// GetStream returns a Stream object given a label
func (db *Db) OpenStream(label string) (stream *Stream, err error) {
	stream = &Stream{Db: db, Label: label}
	// XXX deal with non-'/' path seperators
	linkabspath := filepath.Join(db.Dir, "stream", label)
	noderelpath, err := os.Readlink(linkabspath)
	if err != nil {
		return
	}
	nodepath := &Path{db, noderelpath}
	stream.Algo = nodepath.Algo()
	stream.RootNode, err = db.GetNode(nodepath)
	if err != nil {
		return
	}

	return
}

func (stream *Stream) CanPath() (canpath string) {
	return filepath.Join("stream", stream.Label)
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
// XXX replace with node.Read()
func (node *Node) Cat() (buf []byte, err error) {

	// db := node.Db

	// get leaf nodes
	objects, err := node.traverse(false)
	if err != nil {
		return
	}

	// append leaf node content to buf
	buf = []byte{}
	for _, obj := range objects {
		var content []byte
		blob, ok := obj.(*Blob)
		if !ok {
			panic("assertion failure: blob type")
		}
		content, err = blob.ReadAll()
		if err != nil {
			return
		}
		buf = append(buf, content...)
	}
	return
}

// Verify hashes the node content and compares it to its address
// XXX refactor to take advantage of streaming
func (node *Node) Verify() (ok bool, err error) {
	objects, err := node.traverse(true)
	if err != nil {
		return
	}
	for _, child := range objects {
		path := child.Path
		switch path.Class() {
		case "blob":
			// XXX add a verify flag to GetBlob and do this there
			content, err := child.Db.GetBlob(path)
			if err != nil {
				return false, err
			}
			// hash content
			binhash, err := Hash(path.Algo(), content)
			if err != nil {
				return false, err
			}
			// compare hash with path.Hash
			hex := bin2hex(binhash)
			if path.Hash() != hex {
				log.Debugf("node %v path %v content '%s'", node, path, content)
				return false, fmt.Errorf("expected %v, calculated %v", path.Hash(), hex)
			}
		case "node":
			_, err := node.Db.getNode(path.Canon(), true)
			if err != nil {
				return false, err
			}
		default:
			err = fmt.Errorf("invalid path.Class %v", path.Class())
			return false, err
		}
	}
	return true, nil
}

// traverse recurses down the tree of nodes returning leaves or optionally all nodes
func (node *Node) traverse(all bool) (objects []Object, err error) {

	if all {
		nodes = append(nodes, node)
		return
	}

	switch node.Path.Class() {
	case "blob":
		nodes = append(nodes, node)
	case Node:
		for _, child := range obj.entries {
			var childnodes []*Node
			childnodes, err = obj.traverse(all)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, childnodes...)
		}
	default:
		panic("unhandled Object type in traverse()")
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

// Key is a unique identifier for an object. An object is a Merkle
// tree inner or leaf node (blob), world, or ref.
// XXX deprecate in favor of Object
/*
type Key struct {
	Db    *Db
	Class string
	// World string
	Algo string
	Hash string
}
*/

const pathsep = string(os.PathSeparator)

type Path struct {
	Db  *Db
	Any string
}

// Abs returns the absolute path given any type of path as input.
func (path *Path) Abs() string {
	relpath := path.Rel(path.Any)
	return filepath.Join(path.Db.Dir, relpath)
}

// Canon returns the canonical path given any type of path as input.
func (path *Path) Canon() string {
	class, algo, hash := path.Parts()
	return filepath.Join(class, algo, hash)
}

// Rel returns the relative path of any type of input path.  We
// use the nesting depth described in the Db comments.  We use the
// full hash value in the last component of the path in order to make
// troubleshooting using UNIX tools slightly easier (in contrast to
// the way git truncates the leading subdir parts of the hash).
func (path *Path) Rel() string {
	class, algo, hash := path.Parts()
	var subpath string
	for i := 0; i < path.Db.Depth; i++ {
		subdir := hash[(3 * i):((3 * i) + 3)]
		subpath = filepath.Join(subpath, subdir)
	}
	return filepath.Join(class, algo, subpath, hash)
}

func (path *Path) Addr() (addr string) {
	class, algo, hash := path.Parts()
	// an addr always uses forward slashes, so filepath.Join() would be
	// the wrong thing here
	return algo + "/" + hash
}

func (path *Path) Parts() (class, algo, hash string) {

	anypath := path.Any
	index := strings.Index(anypath, path.Db.Dir)
	if index == 0 {
		// remove db.Dir
		anypath = strings.Replace(anypath, db.Dir, "", 1)
	}

	// split into parts
	// XXX detect and handle malformed path
	parts := strings.Split(anypath, pathsep)
	class = parts[0]
	algo = parts[1]
	// the last part of the path should always be the full hash,
	// regardless of whether we were given the full or canonical
	// path
	hash = parts[len(parts)-1]

	return
}

func (path *Path) Class() (name string) {
	class, _, _ := path.Parts()
	return class
}

func (path *Path) Algo() (name string) {
	_, algo, _ := path.Parts()
	return algo
}

func (path *Path) Hash() (name string) {
	_, _, hash := path.Parts()
	return hash
}

/*
func (k Key) String() string {
	return k.Canon()
}

// Canon returns the canonical path of a key, without the intermediate
// subdirectory levels.
func (k Key) Canon() string {
	return k.path(false)
}

// KeyFromPath takes either a canonical path or a path relative to db
// root dir and returns a populated Key object
// XXX replace with ObjectFromCanPath()
func (db *Db) KeyFromPath(path string) (key *Key) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		panic(fmt.Errorf("path not found: %q", path))
	}
	key = &Key{
		Db:    db,
		Class: parts[0],
		Algo:  parts[1],
		// the last part of the path should always be the full hash,
		// regardless of whether we were given the full or canonical
		// path
		Hash: parts[len(parts)-1],
	}
	return
}
*/

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

func (db *Db) PathFromString(algo string, s string) (path *Path, err error) {
	buf := []byte(s)
	return db.PathFromBuf(algo, &buf)
}

// XXX deprecate in favor of Blob.Write(), Close(), then Hash()
func (db *Db) PathFromBuf(algo string, buf []byte) (path *Path, err error) {
	binhash, err := Hash(algo, buf)
	if err != nil {
		return
	}
	hash := bin2hex(binhash)
	path := &Path{Db: db, Any: filepath.Join("blob", algo, hash)}
	return
}

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
	return &binhash, nil
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
	Db      *Db
	entries []Object
	Path    *Path
	fh      *os.File
}

func (db *Db) OpenNode(relpath string) (node *Node, err error) {
	// XXX see OpenBlob
	return
}

func (node *Node) Read(buf []byte) (n int, err error) {
	return node.fh.Read(buf)
}

func (node *Node) Write(data []byte) (n int, err error) {
	return node.fh.Write(data)
}

func (node *Node) Seek(n int64, whence int) (nout int64, err error) {
	return node.fh.Seek(n, whence)
}

func (node *Node) Tell() (n int64, err error) {
	return
}

func (node *Node) Close() (err error) {
	// XXX see Blob.Close
	return
}

func (node *Node) Size() (n int64, err error) {
	// should this return size of node file or size of all distant child blobs?
	return
}

// String returns the concatenated node entries
func (node *Node) String() (out string) {
	for _, entry := range node.entries {
		out += strings.TrimSpace(entry.CanPath()) + "\n"
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

	node = &Node{Db: db}

	// populate the entries field
	node.entries = children

	// concatenate all relpaths together (include the full canpath with
	// the 'blob/' or 'node/' prefix to help protect against preimage
	// attacks)
	content := []byte(node.String())

	binhash, err := Hash(algo, &content)
	if err != nil {
		return
	}
	hash := bin2hex(binhash)
	relpath := filepath.Join("node", algo, hash)

	err = db.put(relpath, &content)
	if err != nil {
		return
	}

	return
}

// GetNode takes a node path and returns a Node struct
func (db *Db) GetNode(path *Path) (node *Node, err error) {
	return db.getNode(path, true)
}

func (db *Db) getNode(path string, verify bool) (node *Node, err error) {

	// XXX refactor to use Object methods

	abspath := path.Abs()
	file, err := os.Open(abspath)
	if err != nil {
		return
	}
	defer file.Close()

	node = &Node{Db: db, Path: path}
	scanner := bufio.NewScanner(file)
	var content []byte
	var entries []Object
	for scanner.Scan() {
		buf := scanner.Bytes()
		line := string(buf)

		parts := strings.Split(line, " ")
		canon := parts[0]
		entry := db.ObjectFromCanPath(canon)
		entries = append(entries, entry)

		content = append(content, buf...)
		content = append(content, '\n')
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	node.entries = entries

	if verify {
		// hash content
		binhash, err := Hash(path.Algo(), &content)
		if err != nil {
			return node, err
		}
		// compare hash with path.Hash()
		hex := bin2hex(binhash)
		if path.Hash() != hex {
			log.Debugf("node %v path %v content '%s'", node, path, content)
			err = fmt.Errorf("expected %v, calculated %v", path.Hash(), hex)
			return node, err
		}
	}

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
