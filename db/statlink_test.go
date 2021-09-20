package db

import "testing"

func TestStatLink(t *testing.T) {
	db := setup(t, nil)
	// setup
	buf1 := mkbuf("blob1value")
	child1, err := db.PutBlock("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	child2, err := db.PutBlock("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}
	// put tree
	tree, err := db.PutTree("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if tree == nil {
		t.Fatal("tree is nil")
	}

	// XXX pending accounting
	/*
		// create link
		link, err := OpenLink("/foo/bar/baz")
		tck(t, err)

		// put value in link
		err := link.Put(tree, 1.23) // XXX multicurrency?  user?  auth?
		tck(t, err)

		// get link
		val, err := link.Get()

		val, err := link.GetAll() // XXX use a generator instead

		// get tree from link
		gottree, err := db.GetTree(tree.Path)
		if err != nil {
			t.Fatal(err)
		}
		expecttxt, err := tree.Txt()
		tassert(t, err == nil, "%#v", err)
		gottxt, err := gottree.Txt()
		tassert(t, err == nil, "%#v", err)
		tassert(t, expecttxt == gottxt, "tree %v mismatch: expect %v got %v", tree.Path.Abs, expecttxt, gottxt)
	*/

}
