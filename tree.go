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
	*File
	entries      *[]Object // XXX shouldn't this be a slice of pointers instead?
	currentEntry int64
	posInBlob    int64
}

func (tree Tree) New(db *Db, file *File) *Tree {
	tree.Db = db
	tree.File = file
	return &tree
}

// Read fills buf with the next chunk of data from tree's leaf nodes,
// recursing as needed to reach all the leaf nodes.
func (tree Tree) Read(buf []byte) (n int, err error) {
	defer Return(&err)
	// objects, err := tree.traverse(false)
	// Ck(err)

	// Iterate over tree.entries:
	//
	// We are typically called repeatedly from within a caller's for{}
	// loop.  But we need to act like a nested set of for{} loops
	// instead; one loop to track current blob and another loop to
	// track seek() position in the blob.  We accomplish this by
	// implementing a state machine in the following switch{}, using
	// tree.currentEntry and tree.posInBlob to track current state as
	// we traverse the tree.
	if tree.currentEntry >= int64(len(*tree.entries)) {
		return
	}
	obj := (*tree.entries)[tree.currentEntry]
	switch entry := obj.(type) {
	case *Tree:
		// if entry is a tree, then recurse
		tree.currentEntry++
		Assert(tree.posInBlob == 0)
		return entry.Read(buf)
	case *Blob:
		// else, load bytes into buf and save our position in posInBlob
		_, err = entry.Seek(tree.posInBlob, 0)
		Ck(err)
		n, err = entry.Read(buf)
		if errors.Cause(err) == io.EOF {
			tree.currentEntry++
			tree.posInBlob = 0
			return
		}
		Ck(err)
		// there is still more to read from current blob
		tree.posInBlob += int64(n)
		return
	default:
		panic(fmt.Sprintf("unhandled type %T", entry))
	}
	Assert(false)
	return
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

// Cat concatenates all of the leaf node content in node's tree and returns
// it all as a pointer to a byte slice.
// XXX return io.Reader instead of buf
func (tree *Tree) Cat() (buf []byte, err error) {
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

func (tree *Tree) GetPath() *Path {
	return tree.Path
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

	Assert(tree.File != nil)
	file := tree.File
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
	Ck(err)

	tree.entries = &entries

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

	log.Debugf("getTree tree.entries %#v", tree.entries)
	return
}

// Txt returns the concatenated tree entries
func (tree *Tree) Txt() (out string) {
	for _, entry := range *tree.entries {
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

	if tree.File == nil {
		file, err := File{}.New(tree.Db, tree.Path)
		Ck(err)
		tree.File = file
	}

	if tree.entries == nil {
		err = tree.loadEntries()
		Ck(err)
	}

	if all {
		objects = append(objects, tree)
	}

	log.Debugf("traverse tree %#v", tree)
	for _, obj := range *tree.entries {
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
