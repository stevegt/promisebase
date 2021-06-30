package fuse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	. "github.com/stevegt/goadapt"
	"github.com/t7a/pitbase/db"
	pb "github.com/t7a/pitbase/db"
)

// XXX init(), caller(), and GetGID() are copies of the same from
// pitbase.go and all should be moved to a common lib
func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
	log.SetReportCaller(true)
	formatter := &log.TextFormatter{
		CallerPrettyfier: caller(),
		FieldMap: log.FieldMap{
			log.FieldKeyFile: "caller",
		},
	}
	formatter.TimestampFormat = "15:04:05.999999999"
	log.SetFormatter(formatter)
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

// GetGID returns the goroutine ID of its calling function, for logging purposes.
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

type DirNode struct {
	fs.Inode
}

// XXX add README in each dir

func (r *DirNode) Readdir(ctx context.Context) (stream fs.DirStream, errno syscall.Errno) {
	entries := []fuse.DirEntry{
		{Mode: syscall.S_IFDIR, Name: "."},
		{Mode: syscall.S_IFDIR, Name: ".."},
	}
	for name, child := range r.Children() {
		entry := fuse.DirEntry{Mode: child.Mode(), Name: name}
		entries = append(entries, entry)
	}
	return fs.NewListDirStream(entries), 0
}

type HelloRoot struct {
	DirNode
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
	DirNode
	db *pb.Db
}

var _ = (fs.NodeOnAdder)((*fsRoot)(nil))

func (root *fsRoot) OnAdd(ctx context.Context) {
	// XXX get valid algos from db
	for _, algo := range []string{"sha256", "sha512"} {
		node := root.NewPersistentInode(ctx,
			&algoNode{
				db:   root.db,
				algo: algo,
			},
			fs.StableAttr{
				Mode: syscall.S_IFDIR,
			},
		)
		root.AddChild(algo, node, false)
	}
	/*
		newnode := root.NewInode(
			ctx,
			&newNode{db: root.db},
			fs.StableAttr{Mode: fuse.S_IFREG},
		)
		root.AddChild("new", newnode, false)
	*/
}

func (n *fsRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fmt.Println("something")
	newnode := n.NewInode(
		ctx,
		&newNode{db: n.db},
		fs.StableAttr{Mode: fuse.S_IFREG},
	)
	return newnode, newnode, 0, 0
}

// algo

type algoNode struct {
	DirNode
	db   *pb.Db
	algo string
}

var _ = (fs.NodeLookuper)((*algoNode)(nil))

func (n *algoNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (child *fs.Inode, errno syscall.Errno) {
	defer Unpanic(&errno, msglog)

	raw := filepath.Join("tree", n.algo, name)
	path, err := pb.Path{}.New(n.db, raw)
	Ck(err)
	child = n.NewInode(
		ctx,
		&treeNode{db: n.db, path: path},
		fs.StableAttr{Mode: fuse.S_IFDIR},
	)

	return child, 0
}

// tree

type treeNode struct {
	DirNode
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

func (n *contentNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, outflags uint32, errno syscall.Errno) {
	defer Unpanic(&errno, msglog)

	// disallow writes
	if flags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	db := n.db
	tree, err := db.GetTree(n.path)
	if err != nil {
		// "Object is remote" if we don't find it in our local db
		// XXX return EFAULT if address format is bad
		return nil, 0, syscall.EREMOTE
	}

	// make a copy so tree.currentLeaf is unique
	// XXX is this actually needed?
	// XXX what about seek position within leaf file?
	fh = &contentNode{
		db:   n.db,
		path: n.path,
		tree: tree,
	}

	// The file content is immutable, so ask the kernel to cache the data.
	return fh, fuse.FOPEN_KEEP_CACHE, fs.OK
}

var _ = (fs.NodeGetattrer)((*contentNode)(nil))

func (n *contentNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) (errno syscall.Errno) {
	defer Unpanic(&errno, msglog)

	out.Mode = 0644
	out.Mtime = uint64(time.Now().Unix())

	db := n.db
	tree, err := db.GetTree(n.path)
	if err != nil {
		// log.Errorf("gattr tree error: %#v", err)
		return syscall.EREMOTE
	}
	// XXX change size fields to uint64 everywhere
	size, err := tree.Size()
	out.Size = uint64(size)
	if err != nil {
		log.Errorf("size error: %#v", err)
		return syscall.EIO
	}
	// log.Errorf("lkdsafj: %#v %d", tree, size)
	return 0
}

var _ = (fs.FileReader)((*contentNode)(nil))

func (fh *contentNode) Read(ctx context.Context, buf []byte, offset int64) (res fuse.ReadResult, errno syscall.Errno) {
	defer Unpanic(&errno, msglog)

	tree := fh.tree

	// seek
	_, err := tree.Seek(offset, io.SeekStart)
	if err != nil {
		log.Errorf("seek error: %#v", err)
		return nil, syscall.EIO
	}

	// read
	nread, err := tree.Read(buf)
	if err == io.EOF {
		if nread == 0 {
			// XXX is this the right way to report EOF?
			return nil, 0
			// return fuse.ReadResultData(buf[:0]), 0
		}
	} else if err != nil {
		log.Errorf("read error: %#v", err)
		return nil, syscall.EIO
	}

	// XXX use ReadResultFd for zero-copy
	return fuse.ReadResultData(buf[:nread]), 0
}

// new node

type newNode struct {
	fs.Inode
	db   *pb.Db
	path *db.Path
	tree *pb.Tree
}

var _ = (fs.FileWriter)((*newNode)(nil))

func (fh *newNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	defer Unpanic(&errno, msglog)
	if fh.path != nil {
		return 0, syscall.EEXIST
	}
	blob, err := fh.db.PutBlob("sha256", data)
	Ck(err)
	fh.tree, err = fh.db.PutTree("sha256", blob)
	Ck(err)
	fmt.Println(fh.tree.Path.Addr)
	return uint32(len(data)), 0

}

// server

func Serve(db *db.Db, mnt string) (server *fuse.Server, err error) {
	defer Return(&err)
	opts := &fs.Options{}
	// be verbose
	opts.Debug = true
	// start inode numbers at 2^16
	opts.FirstAutomaticIno = 1 << 16
	server, err = fs.Mount(mnt, &fsRoot{db: db}, opts)
	Ck(err)
	server.WaitMount()
	return
}

func msglog(msg string) {
	log.Errorf("unpanic: %v", msg)
}
