package main

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	. "github.com/stevegt/goadapt"
	"github.com/t7a/pitbase/db"
	pb "github.com/t7a/pitbase/db"
)

type HelloRoot struct {
	fs.Inode
}

func (r *HelloRoot) OnAdd(ctx context.Context) {
	ch := r.NewPersistentInode(
		ctx, &fs.MemRegularFile{
			Data: []byte("hello"),
			Attr: fuse.Attr{
				Mode: 0644,
			},
		}, fs.StableAttr{Ino: 2})
	r.AddChild("hello.txt", ch, false)
}

func (r *HelloRoot) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*HelloRoot)(nil))
var _ = (fs.NodeOnAdder)((*HelloRoot)(nil))

func hello(dir string) (server *fuse.Server, err error) {
	defer Return(&err)
	opts := &fs.Options{}
	opts.Debug = true
	server, err = fs.Mount(dir, &HelloRoot{}, opts)
	Ck(err)
	// server.Wait()
	return
}

// root

type fsRoot struct {
	fs.Inode
	db *pb.Db
}

var _ = (fs.NodeOnAdder)((*fsRoot)(nil))

func (root *fsRoot) OnAdd(ctx context.Context) {
	// XXX get valid algos from db
	for _, algo := range []string{"sha256", "sha512"} {
		node := root.NewPersistentInode(ctx, &algoNode{db: root.db, algo: algo}, fs.StableAttr{Mode: syscall.S_IFDIR})
		root.AddChild(algo, node, false)
	}
}

// algo

type algoNode struct {
	fs.Inode
	db   *pb.Db
	algo string
}

var _ = (fs.NodeLookuper)((*algoNode)(nil))

func (n *algoNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {

	path := pb.Path{}.New(n.db, filepath.Join("tree", n.algo, name))
	child := n.NewInode(
		ctx,
		&treeNode{db: n.db, path: path},
		fs.StableAttr{Mode: fuse.S_IFDIR},
	)

	return child, 0
}

// tree

type treeNode struct {
	fs.Inode
	db   *pb.Db
	path *db.Path
}

var _ = (fs.NodeOnAdder)((*treeNode)(nil))

func (n *treeNode) OnAdd(ctx context.Context) {
	content := n.NewInode(
		ctx,
		&contentNode{db: n.db, path: n.path},
		fs.StableAttr{Mode: fuse.S_IFREG},
	)
	n.AddChild("content", content, false)
}

// content

type contentNode struct {
	fs.Inode
	db   *pb.Db
	path *db.Path
	tree *pb.Tree
}

var _ = (fs.NodeOpener)((*contentNode)(nil))

func (n *contentNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {

	// disallow writes
	if flags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	db := n.db
	tree, err := db.GetTree(n.path)
	if err != nil {
		// "Object is remote" if we don't find it in our local db
		// XXX return EFAULT if address format is bad
		// XXX have GetTree always return syscall.Errno
		return nil, 0, syscall.EREMOTE
	}

	// make a copy so tree.currentLeaf is unique
	// XXX is this actually needed?
	// XXX what about seek position within leaf file?
	fh := &contentNode{
		db:   n.db,
		path: n.path,
		tree: tree,
	}

	// The file content is immutable, so ask the kernel to cache the data.
	return fh, fuse.FOPEN_KEEP_CACHE, fs.OK
}

/*
var _ = (fs.FileReader)((*contentNode)(nil))

func (fh *contentNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {

	// seek

	// read

	// return
	return fuse.ReadResult(XXX), 0
}
*/

// server

func Serve(db *db.Db, mnt string) (server *fuse.Server, err error) {
	defer Return(&err)
	opts := &fs.Options{}
	opts.Debug = true
	opts.FirstAutomaticIno = 1 << 16
	server, err = fs.Mount(mnt, &fsRoot{db: db}, opts)
	Ck(err)
	// server.Wait()
	return
}
