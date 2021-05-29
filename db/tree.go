package pitbase

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/google/renameio"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	. "github.com/stevegt/goadapt"
)

// Tree is a vertex in a Merkle tree. Entries point at leafs or other nodes.
type Tree struct {
	Db *Db
	*WORM
	_entries    []Object
	_leaves     []Object
	currentLeaf int64
}

func (tree Tree) New(db *Db, file *WORM) *Tree {
	tree.Db = db
	tree.WORM = file
	return &tree
}

/*
func (tree *Tree) CurrentLeaf() Object {
	// XXX use a generator to provide leafs to Read(); reset generator
	// position on Seek()
}
*/

func (tree *Tree) Entries() []Object {
	if len(tree._entries) == 0 {
		err := tree.loadEntries()
		// we might panic here
		// it's up to callers to recover() if they want to continue operation
		Ck(err)
	}
	return tree._entries
}

// AppendBlob puts a blob in the database, appends it to the node's
// Merkle tree as a new leaf node, and returns the new root node.
// This function can be used to append new records or blocks to journals
// or files in accounting, trading, version control, blockchain, and file
// storage applications.
// XXX refactor for streaming, or add an AppendBlobStream
func (tree *Tree) AppendBlob(algo string, buf []byte) (newrootnode *Tree, err error) {
	oldrootnode := tree

	// put blob
	blob, err := tree.Db.PutBlob(algo, buf)

	// put tree for new root of merkle tree
	newrootnode, err = tree.Db.PutTree(algo, oldrootnode, blob)
	if err != nil {
		return
	}
	return
}

/*
// Cat concatenates all of the leaf node content in node's tree and returns
// it all as a pointer to a byte slice.
// XXX return io.Reader instead of buf
func (tree *Tree) XXXCat() (buf []byte, err error) {
	defer Return(&err)

	// db := tree.Db

	// get leaf nodes
	objects, err := tree.traverse(false)
	Ck(err)

	// append leaf node content to buf
	buf = []byte{}
	for _, obj := range objects {
		var content []byte
		blob, ok := obj.(*Blob)
		if !ok {
			panic("assertion failure: blob type")
		}
		file, err := File{}.New(blob.Db, blob.File.Path)
		Ck(err)
		blob = Blob{}.New(blob.Db, file)
		// XXX rework for streaming
		content, err = blob.ReadAll()
		Ck(err)
		buf = append(buf, content...)
	}
	return
}
*/

func (tree *Tree) GetPath() *Path {
	return tree.Path
}

func (tree *Tree) Leaves() (leaves []Object, err error) {
	defer Return(&err)
	if len(tree._leaves) == 0 {
		tree._leaves, err = tree.traverse(false)
		Ck(err)
	}
	return tree._leaves, nil
}

// LinkStream makes a symlink named label pointing at tree, and returns
// the resulting stream object.
// XXX do we need this?  creating the stream with rootnode == nil is risky
func (tree *Tree) LinkStream(label string) (stream *Stream, err error) {
	stream = Stream{}.New(tree.Db, label, tree)
	src := filepath.Join("..", tree.Path.Rel)
	// XXX sanitize label
	linkabspath := filepath.Join(tree.Db.Dir, "stream", label)
	log.Debugf("linkabspath %#v", linkabspath)
	err = renameio.Symlink(src, linkabspath)
	if err != nil {
		return
	}
	return
}

func (tree *Tree) loadEntries() (err error) {
	defer Return(&err)

	Assert(tree.WORM != nil)
	Assert(tree.WORM.Path != nil)
	if tree.WORM.Path.Abs == "" {
		return
	}
	file := tree.WORM
	scanner := bufio.NewScanner(file)
	var content []byte
	var entries []Object
	for scanner.Scan() {
		buf := scanner.Bytes()
		line := string(buf)
		line = strings.TrimSpace(line)
		path := Path{}.New(tree.Db, line)
		entry, err := tree.Db.ObjectFromPath(path)
		Ck(err)
		log.Debugf("entry %#v", entry)
		entries = append(entries, entry)

		content = append(content, buf...)
		content = append(content, '\n')
	}
	err = scanner.Err()
	Ck(err, "%v: %q", err, file.Path.Abs)

	tree._entries = entries

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
				log.Debugf("getTree verify failure path %v content '%s'", path.Abs, content)
				err = fmt.Errorf("expected %v, calculated %v", path.Hash, hex)
				return node, err
			}
		}
	*/

	return
}

// Read fills buf with the next chunk of data from tree's leaf nodes.
func (tree *Tree) Read(buf []byte) (n int, err error) {
	defer Return(&err)

	leaves, err := tree.Leaves()
	Ck(err)

	for {
		if tree.currentLeaf >= int64(len(leaves)) {
			log.Debugf("tree.Read() returning 0, io.EOF")
			return 0, io.EOF
		}

		obj := (leaves)[tree.currentLeaf]
		n, err = obj.Read(buf)
		if errors.Cause(err) == io.EOF {
			// go's finalizer might close files for us when obj goes
			// out of scope, and since this was a read-only file
			// anyway, don't check err after obj.Close()
			obj.Close()
			Assert(n == 0)
			tree.currentLeaf++
			log.Debugf("tree.Read() advancing to leaf %v", tree.currentLeaf)
			continue
		}
		Ck(err)
		break
	}

	return
}

func (tree *Tree) Rewind() error {
	tree.currentLeaf = 0
	tree._entries = []Object{}
	return nil
}

// Seek sets the offset for the next Read on tree to offset,
// interpreted according to whence: 0 means relative to the origin of
// the file, 1 means relative to the current offset, and 2 means
// relative to the end.  It returns the new offset and an error, if
// any.
func (tree *Tree) Seek(offset int64, whence int) (newOffset int64, err error) {
	defer Return(&err)
	// XXX ensure readonly?

	var pos int64

	// SeekStart   = 0 // seek relative to the origin of the file
	// SeekCurrent = 1 // seek relative to the current offset
	// SeekEnd     = 2 // seek relative to the end
	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		n, err := tree.Tell()
		Ck(err)
		pos = n + offset
	case io.SeekEnd:
		n, err := tree.Size()
		Ck(err)
		pos = n + offset
	}

	var total int64
	leaves, err := tree.Leaves()
	Ck(err)
	for i, leaf := range leaves {
		size, err := leaf.Size()
		Ck(err)
		// add up all leaf sizes until we pass pos
		newtotal := total + size
		if newtotal >= pos {
			// seek in last leaf
			leafPos := pos - total
			_, err := leaf.Seek(leafPos, io.SeekStart)
			Ck(err)
			tree.currentLeaf = int64(i)
			break
		}
		total = newtotal
	}

	return offset, nil
}

func (tree *Tree) Size() (total int64, err error) {
	defer Return(&err)
	leaves, err := tree.Leaves()
	Ck(err)
	for _, leaf := range leaves {
		size, err := leaf.Size()
		Ck(err)
		total += size
	}
	return
}

// Tell returns the current seek position in the tree.
func (tree *Tree) Tell() (n int64, err error) {
	var pos int64
	// add up all leaf sizes until we get to the current leaf

	// add position in current leaf

	return pos, nil
}

// Txt returns the concatenated tree entries
func (tree *Tree) Txt() (out string) {
	for _, entry := range tree.Entries() {
		out += strings.TrimSpace(entry.GetPath().Canon) + "\n"
	}
	return
}

// Verify hashes the node content and compares it to its address
// XXX move to File
// XXX refactor to take advantage of streaming
// XXX right now we only verify trees by default -- what about blobs?
func (tree *Tree) Verify() (ok bool, err error) {
	defer Return(&err)
	objects, err := tree.traverse(true)
	Ck(err)
	for _, obj := range objects {
		switch child := obj.(type) {
		case *Blob:
			// XXX add a verify flag to GetBlob and do this there
			path := child.Path
			content, err := child.Db.GetBlob(path)
			Ck(err)
			// hash content
			content = append([]byte(path.header()), content...)
			binhash, err := Hash(path.Algo, content)
			Ck(err)
			// compare hash with path.Hash
			hex := bin2hex(binhash)
			Assert(path.Hash == hex)
		case *Tree:
			path := child.Path
			log.Debugf("child %#v", child)
			_, err := tree.Db.getTree(path, true)
			Ck(err)
		default:
			panic(fmt.Sprintf("unhandled type %T", child))
		}
	}
	return true, nil
}

// traverse recurses down the tree of nodes returning leaves or optionally all nodes
func (tree *Tree) traverse(all bool) (objects []Object, err error) {
	defer Return(&err)

	if tree.WORM == nil {
		file, err := OpenWORM(tree.Db, tree.Path)
		Ck(err)
		tree.WORM = file
	}

	if all {
		objects = append(objects, tree)
	}

	log.Debugf("traverse tree %#v", tree)
	for _, obj := range tree.Entries() {
		log.Debugf("traverse obj %#v", obj)
		switch child := obj.(type) {
		case *Tree:
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
