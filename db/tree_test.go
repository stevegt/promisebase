package pitbase

import (
	"bytes"
	"io"
	"testing"

	"github.com/hlubek/readercomp"
)

func TestTree(t *testing.T) {
	db := setup(t, nil)
	// setup
	buf1 := mkbuf("blob1value")
	child1, err := db.PutBlob("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	child2, err := db.PutBlob("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree, err := db.PutTree("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if tree == nil {
		t.Fatal("tree is nil")
	}

	/*
		nodekey := db.KeyFromPath("node/sha256/cb4/678/cb46789e72baabd2f1b1bc7dc03f9588f2a36c1d38224f3a11fad7386cb9cbcf")
		if nodekey == nil {
			t.Fatal("nodekey is nil")
		}
		// t.Log(fmt.Sprintf("nodekey %#v node %#v", nodekey, node))
		tassert(t, keyEqual(nodekey, node.Key), "node key mismatch: expect %s got %s", nodekey, node.Key)
	*/

	ok, err := tree.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "tree verify failed: %v", tree)

	// get
	gottree, err := db.GetTree(tree.Path)
	if err != nil {
		t.Fatal(err)
	}
	// t.Log(fmt.Sprintf("node\n%q\ngotnode\n%q\n", node, gotnode))
	tassert(t, tree.Txt() == gottree.Txt(), "tree %v mismatch: expect %v got %v", tree.Path.Abs, tree.Txt(), gottree.Txt())

}

func TestTreeRead(t *testing.T) {
	db := setup(t, nil)

	// setup
	buf1 := mkbuf("blob1value")
	blob1, err := db.PutBlob("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	blob2, err := db.PutBlob("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}
	buf3 := mkbuf("blob3value")
	blob3, err := db.PutBlob("sha256", buf3)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree1, err := db.PutTree("sha256", blob1, blob2)
	if err != nil {
		t.Fatal(err)
	}
	if tree1 == nil {
		t.Fatal("tree1 is nil")
	}
	tree2, err := db.PutTree("sha256", tree1, blob3)
	if err != nil {
		t.Fatal(err)
	}
	if tree2 == nil {
		t.Fatal("tree2 is nil")
	}

	expect := []byte("blob1valueblob2valueblob3value")

	// read explicitly
	file, err := OpenWORM(db, tree2.Path)
	tassert(t, err == nil, "tree2 file %#v err %v", file, err)
	tree2a := Tree{}.New(db, file)
	gotbuf := make([]byte, 99)
	gotbufn := 0
	for i := 0; i < 99; i++ {
		n, err := tree2a.Read(gotbuf[gotbufn:])
		gotbufn += n
		if err == io.EOF {
			tassert(t, n == len(expect), "n %v", n)
			break
		}
		tassert(t, err == nil, "err %#v", err)
	}
	tassert(t, len(expect) == gotbufn, "expect %v got %v", len(expect), gotbufn)
	tassert(t, bytes.Compare(expect, gotbuf[:gotbufn]) == 0, "expect %q got %q", string(expect), string(gotbuf[:gotbufn]))

	// read as stream
	file, err = OpenWORM(db, tree2.Path)
	tassert(t, err == nil, "tree2 file %#v err %v", file, err)
	tree2b := Tree{}.New(db, file)
	expectrd := bytes.NewReader(expect)
	ok, err := readercomp.Equal(expectrd, tree2b, 15) // XXX try different sizes
	tassert(t, err == nil, "readercomp.Equal: %v", err)
	tassert(t, ok, "tree.Read mismatch")

	// test seek
	// expect := []byte("blob1valueblob2valueblob3value")
	n, err := tree2.Seek(4, io.SeekStart)
	tassert(t, err == nil, "%#v", err)
	tassert(t, n == 4, "%v", n)
	nint, err := tree2.Read(gotbuf[:1])
	tassert(t, err == nil, "%#v", err)
	tassert(t, nint == 1, "%v", nint)
	tassert(t, string(gotbuf[0]) == "1", string(gotbuf[0]))

	// XXX test rewind
}

func TestVerify(t *testing.T) {
	db, err := Open("testdata")
	if err != nil {
		t.Fatal(err)
	}
	path := Path{}.New(db, "tree/sha256/22695d451d4f8383546f8cc3d3c93b78c4827f508ad682c620d02a78e58a3ab3")
	tree, err := db.GetTree(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, child := range tree.Entries() {
		switch i {
		case 0:
			expect := "tree/sha256/606/1c8/6061c8eb4f00c1039c0922f1cfb73233b7353b371227fd0a5cd380104ba58a7b"
			tassert(t, expect == child.GetPath().Rel, "expected %v got %v", expect, child.GetPath().Rel)
		case 1:
			expect := "blob/sha256/32b/cc6/32bcc691cfa205d4a4be7f47cfca49253fd76cbdfd93124388b1824499cdb36b"
			tassert(t, expect == child.GetPath().Rel, "expected %q got %q", expect, child.GetPath().Rel)
		}
	}
	ok, err := tree.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "tree verify failed: %v", pretty(tree))
}
