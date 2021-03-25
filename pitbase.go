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
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, GetGID())
	}
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

	// we store references to hashed blobs in refs
	err = mkdir(filepath.Join(dir, "world"))
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
	inode.path = inode.fh.Name()
	inode.fd = inode.fh.Fd()
	return
}

// Put creates a temporary file for a key and then atomically renames to the permanent path.
func (db *Db) put(key *Key, val *[]byte) (err error) {

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

type Blob struct {
	Db       *Db
	Path     string // relative path from db root dir
	fh       *os.File
	Readonly bool
	inode    Inode // XXX get rid of inode dependency so we can deprecate inode?
}

func (db *Db) BlobStat(path string) (info os.FileInfo, err error) {
<<<<<<< HEAD
	// XXX passthrough to os.Stat()
	blob, err := db.OpenBlob(path)
	if err != nil {
		return
	}
	info, err = blob.fh.Stat()
	return
}

func (db *Db) BlobSize(path string) (size int64, err error) {
	// XXX call BlobStat()
	info, err := db.BlobStat(path)
=======
	fullpath := filepath.Join(db.Dir, path)
	return os.Stat(fullpath)
}

func (db *Db) BlobSize(path string) (size int64, err error) {
	fullpath := filepath.Join(db.Dir, path)
	info, err := db.BlobStat(fullpath)
	if err != nil {
		return
	}
>>>>>>> remotes/origin/stevegt/streaming
	size = info.Size()
	return
}

func (db *Db) OpenBlob(path string) (b *Blob, err error) {
	fullpath := filepath.Join(db.Dir, path)
	b = &Blob{Db: db, Path: path}
	if exists(fullpath) {
		// open existing file
		b.fh, err = os.Open(fullpath)
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

	db := b.Db
	path := filepath.Join(db.Dir, b.Path)

	// mkdir
	dir, _ := filepath.Split(path)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}

	// rename temp file to permanent blob file
	err = os.Rename(b.inode.path, path)
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

// GetBlob retrieves the blob of a key by reading its file contents.
func (db *Db) GetBlob(key *Key) (blob *[]byte, err error) {
	buf, err := ioutil.ReadFile(db.Path(key))
	if err != nil {
		return
	}
	blob = &buf
	return
}

// Rm deletes the entry associated with the key and returns an error if the key doesn't exist.
func (db *Db) Rm(key *Key) (err error) {
	err = os.Remove(db.Path(key))
	if err != nil {
		return err
	}
	return
}

// PutBlob performs db.PutBlob on world.Db
// XXX can't do this, 'cause then it's no longer the same world
/*
func (world *World) PutBlob(algo string, blob *[]byte) (key *Key, err error) {
	return world.Db.PutBlob(algo, blob)
}
*/

// AppendBlob puts a blob in the database, appends it to the world's
// Merkle tree as a new leaf node, and then rewrites the world's symlink
// to point at the new tree root.  This function can be used to append
// new records or blocks to journals or files in accounting, trading,
// version control, blockchain, and file storage applications.
func (world *World) AppendBlob(algo string, blob *[]byte) (newworld *World, err error) {
	// get node for root of merkle tree
	oldkey := world.Db.KeyFromPath(world.Src)
	oldrootnode, err := world.Db.GetNode(oldkey)
	if err != nil {
		log.Debugf("oldkey: %v, oldrootnode: %v", oldkey, oldrootnode)
		return
	}
	newrootnode, err := oldrootnode.AppendBlob(algo, blob)

	// rewrite symlink
	newworld, err = world.Db.PutWorld(newrootnode.Key, world.Name)
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
func (node *Node) AppendBlob(algo string, blob *[]byte) (newrootnode *Node, err error) {
	oldrootnode := node

	// put blob
	key, err := node.Db.PutBlob(algo, blob)
	newblobnode := &Node{Db: node.Db, Key: key, Label: ""}

	// put node for new root of merkle tree
	newrootnode, err = node.Db.PutNode(algo, oldrootnode, newblobnode)
	if err != nil {
		return
	}
	return
}

/*
func (blob *Blob) AppendBlob(algo string, newblob *[]byte) (node *Node, err error) {

	// put blob
	key, err := node.Db.PutBlob(algo, newblob)

	// put node to start new merkle tree
	node, err = node.Db.PutNode(algo, oldrootnode, newblobnode)
	if err != nil {
		return
	}
	return
}
*/

/*
func (db *Db) PutStream(algo string, instream io.Reader) (rootnode *Node, err error) {
	outstream := Stream{Db: db, Algo: algo}.Init()
	_, err = io.Copy(outstream, instream)
	if err != nil {
		return
	}
	rootnode = outstream.RootNode
}
*/

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

		key, err := db.PutBlob(algo, &chunk.Data)
		if err != nil {
			return nil, err
		}
		newblobnode := &Node{Db: db, Key: key, Label: ""}

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
func (db *Db) PutBlob(algo string, blob *[]byte) (key *Key, err error) {
	key, err = db.KeyFromBlob(algo, blob)
	if err != nil {
		return
	}
	path := db.Path(key)
	// check if it's already stored
	_, err = os.Stat(path)
	if err == nil {
		// content, err2 := ioutil.ReadFile(path)
		// if err2 != nil {
		// 	return nil, err2
		// }
		// fmt.Println("Exists:", key.String(), string(content))
	} else if os.IsNotExist(err) {
		// store it
		err = db.put(key, blob)
	}
	return
}

// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) Path(key *Key) (path string) {
	log.Debugf("db: %v, key: %v", db, key)
	path = filepath.Join(db.Dir, key.Path())
	return
}

// World is a reference to a timeline
type World struct {
	Db   *Db
	Name string
	Src  string
}

// String returns the path for a world
func (world *World) String() (path string) {
	return filepath.Join(world.Db.Dir, "world", world.Name)
}

// PutWorld takes a key and a name and creates a world with that name
func (db *Db) PutWorld(key *Key, name string) (world *World, err error) {
	world = &World{Db: db, Name: name}
	src := filepath.Join("..", key.Path())
	err = renameio.Symlink(src, world.String())
	if err != nil {
		return
	}
	world.Src = key.Path()
	return
}

// GetWorld returns a world pointer from a name
func (db *Db) GetWorld(name string) (world *World, err error) {
	world = &World{Db: db, Name: name}
	src, err := os.Readlink(world.String())
	if err != nil {
		return
	}
	parts := strings.Split(src, string(filepath.Separator))
	world.Src = filepath.Join(parts[1:]...)
	return
}

// Ls lists all of the leaf nodes in a world and optionally both
// leaf and inner
func (world *World) Ls(all bool) (nodes []*Node, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	key := world.Db.KeyFromPath(world.Src)
	rootnode, err := world.Db.GetNode(key)
	if err != nil {
		return
	}
	rootnode.Label = world.Name
	return rootnode.traverse(all)
}

// Cat concatenates all of the leaf node content in World and returns
// it as a pointer to a byte slice.
func (world *World) Cat() (buf *[]byte, err error) {
	key := world.Db.KeyFromPath(world.Src)
	rootnode, err := world.Db.GetNode(key)
	if err != nil {
		return
	}
	return rootnode.Cat()
}

// Cat concatenates all of the leaf node content in node's tree and returns
// it all as a pointer to a byte slice.
// XXX this should be a generator, to prevent memory consumption
// with large trees
func (node *Node) Cat() (buf *[]byte, err error) {

	// get leaf nodes
	rootnode := node
	nodes, err := rootnode.traverse(false)
	if err != nil {
		return
	}

	// append leaf node content to buf
	buf = &[]byte{}
	for _, node := range nodes {
		var content *[]byte
		content, err = node.Db.GetBlob(node.Key)
		if err != nil {
			return
		}
		*buf = append(*buf, *content...)
	}
	return
}

// Verify hashes the node content and compares it to its key
func (node *Node) Verify() (ok bool, err error) {
	nodes, err := node.traverse(true)
	if err != nil {
		return
	}
	for _, node := range nodes {
		key := node.Key
		switch key.Class {
		case "blob":
			// XXX add a verify flag to GetBlob and do this there
			content, err := node.Db.GetBlob(key)
			if err != nil {
				return false, err
			}
			// hash content
			binhash, err := Hash(key.Algo, content)
			if err != nil {
				return false, err
			}
			// compare hash with key.Hash
			hex := bin2hex(binhash)
			if key.Hash != hex {
				log.Debugf("node %v key %v content '%s'", node, key, *content)
				return false, fmt.Errorf("expected %v, calculated %v", key.Hash, hex)
			}
		case "node":
			_, err := node.Db.getNode(key, true)
			if err != nil {
				return false, err
			}
		default:
			err = fmt.Errorf("invalid key.Class %v", key.Class)
			return false, err
		}
	}
	return true, nil
}

// traverse recurses down the tree of nodes returning leaves or optionally all nodes
func (node *Node) traverse(all bool) (nodes []*Node, err error) {

	// include this node
	if all || node.Key.Class == "blob" {
		nodes = append(nodes, node)
	}

	// include child nodes
	if node.Key.Class == "node" {
		children, err := node.ChildNodes()
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			var childnodes []*Node
			childnodes, err = child.traverse(all)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, childnodes...)
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

// Key is a unique identifier for an object. An object is a Merkle tree inner or leaf node (blob), world, or
// ref.
type Key struct {
	Db    *Db
	Class string
	World string
	Algo  string
	Hash  string
}

// Path returns the filesystem path of a key.  We use the nesting depth
// described in the Db comments.  We use the full hash value in the
// last component of the path in order to make playing and
// troubleshooting using UNIX tools slightly easier (as opposed to the
// way git does it, truncating the leading subdir parts of the hash).
// (This may be a problem some decade in the future if a new hash algo
// produces hashes long enough to overflow the maximum filename
// length.)
func (k Key) Path() string {
	return k.path(true)
}

func (k Key) path(full bool) string {
	if full {
		var subpath string
		for i := 0; i < k.Db.Depth; i++ {
			subdir := k.Hash[(3 * i):((3 * i) + 3)]
			subpath = filepath.Join(subpath, subdir)
		}
		return filepath.Join(k.Class, k.Algo, subpath, k.Hash)
	} else {
		return filepath.Join(k.Class, k.Algo, k.Hash)
	}
}

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

// KeyFromString returns a key pointer corresponding to the given algo and string
func (db *Db) KeyFromString(algo string, s string) (key *Key, err error) {
	blob := []byte(s)
	return db.KeyFromBlob(algo, &blob)
}

// KeyFromBlob takes a class, algo, and blob and returns a populated Key object
func (db *Db) KeyFromBlob(algo string, blob *[]byte) (key *Key, err error) {
	binhash, err := Hash(algo, blob)
	if err != nil {
		return
	}
	key = &Key{
		Db:    db,
		Class: "blob",
		Algo:  algo,
		Hash:  bin2hex(binhash),
	}
	return
}

// Hash returns the hash of a blob using a given algorithm
func Hash(algo string, blob *[]byte) (hash *[]byte, err error) {
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

// NodeEntry stores the metadata of a Merkle tree inner or leaf node.
type NodeEntry struct {
	CanonPath string
	Label     string
}

// String combines the node's path and label into one string.
func (ne *NodeEntry) String() (out string) {
	out = strings.Join([]string{ne.CanonPath, ne.Label}, " ")
	out = strings.TrimSpace(out) + "\n"
	return
}

// Node is a vertex in a Merkle tree. Entries point at leafs or other nodes.
type Node struct {
	Key     *Key
	Db      *Db
	Label   string
	entries []NodeEntry
}

// String returns the concatenated node entries
func (node *Node) String() (out string) {
	for _, entry := range node.entries {
		out += entry.String()
	}
	return
}

// ChildNodes returns a list of the node's immediate inner or leaf node (blob) children as node objects.
func (node *Node) ChildNodes() (nodes []*Node, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	for _, entry := range node.entries {
		key := node.Db.KeyFromPath(entry.CanonPath)
		var child *Node
		switch key.Class {
		case "blob":
			// we are shoehorning blobs into node objects for easier handling here
			// XXX we should probably finish merging blobs and nodes into one object
			child = &Node{Key: key, Db: node.Db}
		case "node":
			child, err = node.Db.GetNode(key)
			if err != nil {
				log.Errorf("unreachable key %#v err %#v", key, err)
				return nil, err
			}
		}
		child.Label = entry.Label
		nodes = append(nodes, child)
	}
	return
}

// bin2hex converts byte slice into hex string
func bin2hex(bin *[]byte) (hex string) {
	hex = fmt.Sprintf("%x", *bin)
	return
}

// PutNode takes one or more child nodes, stores their keys and labels in a file under node/,
// and returns a pointer to a Node object.
func (db *Db) PutNode(algo string, children ...*Node) (node *Node, err error) {

	node = &Node{Db: db}

	// populate the entries field
	var entries []NodeEntry
	for _, child := range children {
		canon := child.Key.Canon()
		label := child.Label
		entry := NodeEntry{CanonPath: canon, Label: label}
		entries = append(entries, entry)
	}
	node.entries = entries

	// concatenate all keys together (include the full key string with
	// the 'blob/' or 'node/' prefix to help protect against preimage
	// attacks)
	content := []byte(node.String())

	binhash, err := Hash(algo, &content)
	if err != nil {
		return
	}
	hash := bin2hex(binhash)
	node.Key = &Key{
		Db:    db,
		Class: "node",
		Algo:  algo,
		Hash:  hash,
	}

	err = db.put(node.Key, &content)
	if err != nil {
		return
	}

	return
}

// GetNode takes a node key and returns a Node struct
func (db *Db) GetNode(key *Key) (node *Node, err error) {
	return db.getNode(key, true)
}

func (db *Db) getNode(key *Key, verify bool) (node *Node, err error) {
	fn := filepath.Join(db.Dir, key.Path())
	file, err := os.Open(fn)
	if err != nil {
		return
	}
	defer file.Close()

	node = &Node{Db: db, Key: key}
	scanner := bufio.NewScanner(file)
	var content []byte
	var entries []NodeEntry
	for scanner.Scan() {
		buf := scanner.Bytes()
		line := string(buf)

		parts := strings.Split(line, " ")
		canon := parts[0]
		var label string
		if len(parts) >= 2 {
			label = parts[1]
		}
		entry := NodeEntry{CanonPath: canon, Label: label}
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
		binhash, err := Hash(key.Algo, &content)
		if err != nil {
			return node, err
		}
		// compare hash with key.Hash
		hex := bin2hex(binhash)
		if key.Hash != hex {
			log.Debugf("node %v key %v content '%s'", node, key, content)
			err = fmt.Errorf("expected %v, calculated %v", key.Hash, hex)
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
// Stream is an ordered set of bytes of arbitrary (but not infinite)
// length.  It implements the io.ReadWriteCloser interface so a
// Stream acts like a file from the perspective of a caller.
type Stream struct {
	Db          *Db
	Algo        string
	RootNode    *Node
	chunker     *Rabin
	currentBlob *Key
	posInBlob   int64
}

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
		newblobnode := &Node{Db: db, Key: key, Label: ""}

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
