package pitbase

import (
	"path/filepath"

	"github.com/google/renameio"
)

// Stream is an ordered set of bytes of arbitrary (but not infinite)
// length.  It implements the io.ReadWriteCloser interface so a
// Stream acts like a file from the perspective of a caller.
// XXX Either (A) stop exporting Tree and Blob, and have callers only
// see Stream, or (B) be prepared to expose trees and blobs to open
// market operations, and redefine `address` to include blobs as well
// as trees.
type Stream struct {
	Db          *Db
	RootNode    *Tree
	Label       string
	Path        *Path
	chunker     *Rabin
	currentBlob *Blob
	posInBlob   int64
}

func (stream Stream) New(db *Db, label string, rootnode *Tree) *Stream {
	stream.Db = db
	stream.Label = label
	stream.RootNode = rootnode
	linkrelpath := filepath.Join("stream", label)
	stream.Path = Path{}.New(db, linkrelpath)
	return &stream
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
	treerel := filepath.Join("..", newrootnode.Path.Rel)
	linkabs := filepath.Join(stream.Db.Dir, stream.Path.Canon)
	err = renameio.Symlink(treerel, linkabs)
	if err != nil {
		return
	}
	newstream = Stream{}.New(stream.Db, stream.Label, newrootnode)
	return

}

// Cat concatenates all of the leaf node content in World and returns
// it as a pointer to a byte slice.
func (stream *Stream) Cat() (buf []byte, err error) {
	return stream.RootNode.Cat()
}

// Ls lists all of the leaf nodes in a stream and optionally both
// leaf and inner
func (stream *Stream) Ls(all bool) (objects []Object, err error) {
	// XXX this should be a generator, to prevent memory consumption
	// with large trees
	return stream.RootNode.traverse(all)
}
