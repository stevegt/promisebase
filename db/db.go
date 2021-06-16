package db

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	resticRabin "github.com/restic/chunker"
	log "github.com/sirupsen/logrus"
	. "github.com/stevegt/goadapt"
)

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

// Open loads an existing db object from dir.
// XXX Open should be a db method
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
func (db *Db) ObjectFromPath(path *Path) (obj Object, err error) {
	defer Return(&err)

	class := path.Class
	switch class {
	case "blob":
		file, err := OpenWORM(db, path)
		Ck(err)
		return Blob{}.New(db, file), nil
	case "tree":
		file, err := OpenWORM(db, path)
		Ck(err)
		return Tree{}.New(db, file), nil
	default:
		Assert(false, "unhandled class %s", class)
	}
	return
}

// Create initializes a db directory and its contents
// XXX Create should call Open
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
	file, err := OpenWORM(db, path)
	if err != nil {
		return nil, err
	}
	return file.ReadAll()
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

	file, err := CreateWORM(db, "blob", algo)
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
	treepath, err := Path{}.New(db, treeabspath)
	Ck(err)
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

// PutTree takes one or more child nodes, stores relpaths in a file
// under tree/,
// and returns a pointer to a Tree object.
func (db *Db) PutTree(algo string, children ...Object) (tree *Tree, err error) {
	defer Return(&err)

	Assert(db != nil, "db is nil")

	file, err := CreateWORM(db, "tree", algo)
	Ck(err)
	tree = Tree{}.New(db, file)

	// populate the entries field (this is a write of a new tree, so
	// we can't call loadEntries() here)
	tree._entries = children

	// concatenate entry paths together
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

	file, err := OpenWORM(db, path)
	Ck(err)
	defer file.Close()

	tree = Tree{}.New(db, file)

	err = tree.loadEntries()
	Ck(err)

	return
}
