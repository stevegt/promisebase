package pitbase

import (
	"bytes"
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

	// read
	file, err := File{}.New(db, tree2.Path)
	tassert(t, err == nil, "tree2 file %#v err %v", file, err)
	t2 := Tree{}.New(db, file)
	expect := bytes.NewReader([]byte("blob1valueblob2valueblob3value"))
	ok, err := readercomp.Equal(expect, t2, 5) // XXX try different sizes
	tassert(t, err == nil, "readercomp.Equal: %v", err)
	tassert(t, ok, "tree.Read mismatch")

	// XXX test rewind
	// XXX test seek

	// file, err := File{}.New(db, tree2.Path)
	// tassert(t, err == nil)
	// rd, err := Tree{}.New(db, file)
	// tassert(t, err == nil)
	// ok, err := readercomp.Equal(src, cmp, 4096)

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
	for i, child := range *tree.entries {
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
