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

	db := n.db
	path := pb.Path{}.New(db, filepath.Join("tree", n.algo, name))
	tree, err := db.GetTree(path)
	if err != nil {
		return nil, syscall.ENOENT
	}

	operations := &treeNode{tree: tree}
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, operations, stable)

	return child, 0
}

// tree

type treeNode struct {
	fs.Inode
	tree *pb.Tree
}

var _ = (fs.NodeOnAdder)((*treeNode)(nil))

func (n *treeNode) OnAdd(ctx context.Context) {
	content := &contentNode{tree: n.tree}
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, content, stable)
	n.AddChild("content", child, false)
}

// content

type contentNode struct {
	fs.Inode
	tree    *pb.Tree
	seekPos int64
}

// var _ = (fs.NodeOpener)((*contentNode)(nil))

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
