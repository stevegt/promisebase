package db

import (
	"path/filepath"

	"github.com/google/renameio"
	. "github.com/stevegt/goadapt"
)

// Stream is an ordered set of bytes of arbitrary (but not infinite)
// length.  It implements the io.ReadWriteCloser interface so a
// Stream acts like a file from the perspective of a caller.
// XXX Either (A) stop exporting Tree and Blob, and have callers only
// see Stream, or (B) be prepared to expose trees and blocks to open
// market operations, and redefine `address` to include blocks as well
// as trees.
type Stream struct {
	Db       *Db
	RootNode *Tree
	Label    string
	Path     *Path
	// chunker     *Rabin
}

func (stream Stream) New(db *Db, label string, rootnode *Tree) (out *Stream, err error) {
	defer Return(&err)
	stream.Db = db
	stream.Label = label
	stream.RootNode = rootnode
	linkrelpath := filepath.Join("stream", label)
	path, err := Path{}.New(db, linkrelpath)
	Ck(err)
	stream.Path = path
	return &stream, nil
}

// AppendBlock puts a block in the database, appends it to the Merkle
// tree as a new leaf node, and then rewrites the stream label's symlink
// to point at the new tree root.
func (stream *Stream) AppendBlock(algo string, buf []byte) (newstream *Stream, err error) {
	defer Return(&err)
	oldrootnode := stream.RootNode
	newrootnode, err := oldrootnode.AppendBlock(algo, buf)
	if err != nil {
		return
	}

	// rewrite symlink
	treerel := filepath.Join("..", newrootnode.Path.Rel)
	linkabs := filepath.Join(stream.Db.Dir, stream.Path.Canon)
	err = renameio.Symlink(treerel, linkabs)
	if err != nil {
		return
	}
	newstream, err = Stream{}.New(stream.Db, stream.Label, newrootnode)
	Ck(err)
	return

}

/*
// Cat concatenates all of the leaf node content in World and returns
// it as a pointer to a byte slice.
// XXX return io.Reader instead of buf
func (stream *Stream) Cat() (buf []byte, err error) {
	return stream.RootNode.Cat()
}
*/

func (stream *Stream) Read(buf []byte) (n int, err error) {
	return stream.RootNode.Read(buf)
}

func (stream *Stream) Rewind() error {
	return stream.RootNode.Rewind()
}

// Ls lists all of the leaf nodes in a stream and optionally both
// leaf and inner
func (stream *Stream) Ls(all bool) (objects []Object, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	// XXX should be passthrough to tree.Ls()
	return stream.RootNode.traverse(all)
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
