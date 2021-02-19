package pitbase

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/google/renameio"
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

// Open creates a db object and its directory (if one doesn't already exist)
func Open(dir string) (db *Db, err error) {
	db = &Db{}
	err = mkdir(dir)
	if err != nil {
		return
	}

	// XXX use filepath.Join() for any Sprintf that's doing something like this
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
func (world *World) PutBlob(algo string, blob *[]byte) (key *Key, err error) {
	return world.Db.PutBlob(algo, blob)
}

// PutBlob hashes the blob, stores the blob in a file named after the hash,
// and returns the hash.
func (db *Db) PutBlob(algo string, blob *[]byte) (key *Key, err error) {
	key, err = KeyFromBlob(algo, blob)
	if err != nil {
		return
	}

	// check if it's already stored
	// XXX

	// store it
	err = db.put(key, blob)
	if err != nil {
		return
	}
	return
}

// Path takes a key containing arbitrary 8-bit bytes and returns a safe
// hex-encoded pathname.
func (db *Db) Path(key *Key) (path string) {
	log.Debugf("db: %v, key: %v", db, key)
	path = filepath.Join(db.Dir, key.String())
	return
}

// World is a reference to a database
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
	src := filepath.Join("..", key.String())
	err = renameio.Symlink(src, world.String())
	if err != nil {
		return
	}
	world.Src = key.String()
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
	key := KeyFromPath(world.Src)
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
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	key := KeyFromPath(world.Src)
	rootnode, err := world.Db.GetNode(key)
	if err != nil {
		return
	}

	// get leaf nodes
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

func exists(parts ...string) (found bool) {
	path := filepath.Join(parts...)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// Key is a relative path to an object.  An object is a blob, tree, or
// ref.
type Key struct {
	Class string
	World string
	Algo  string
	Hash  string
}

// String returns the path of a key
func (k Key) String() string {
	if k.Class == "ref" {
		return filepath.Join(k.Class, k.World, k.Algo, k.Hash)
	}
	return filepath.Join(k.Class, k.Algo, k.Hash)
}

// KeyFromPath takes a path relative to db root dir and returns a populated Key object
func KeyFromPath(path string) (key *Key) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		panic(fmt.Errorf("path not found: %q", path))
	}
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

// KeyFromString returns a key pointer corresponding to the given algo and string
func KeyFromString(algo string, s string) (key *Key, err error) {
	blob := []byte(s)
	return KeyFromBlob(algo, &blob)
}

// KeyFromBlob takes a class, algo, and blob and returns a populated Key object
func KeyFromBlob(algo string, blob *[]byte) (key *Key, err error) {
	binhash, err := Hash(algo, blob)
	if err != nil {
		return
	}
	key = &Key{
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
	Path  string
	Label string
}

// String combines the node's path and label into one string.
func (ne *NodeEntry) String() (out string) {
	out = strings.Join([]string{ne.Path, ne.Label}, " ")
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

//
func (node *Node) String() (out string) {
	for _, entry := range node.entries {
		out += entry.String()
	}
	return
}

func (node *Node) ChildNodes() (nodes []*Node, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	for _, entry := range node.entries {
		key := KeyFromPath(entry.Path)
		var child *Node
		switch key.Class {
		case "blob":
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
		path := child.Key.String()
		label := child.Label
		entry := NodeEntry{Path: path, Label: label}
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
	fn := filepath.Join(db.Dir, key.String())
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
		path := parts[0]
		var label string
		if len(parts) >= 2 {
			label = parts[1]
		}
		entry := NodeEntry{Path: path, Label: label}
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
