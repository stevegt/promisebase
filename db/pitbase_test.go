package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/stevegt/goadapt"
)

const testDbDirPrefix = "pitbase"

func asString(input interface{}) (out string) {
	out = fmt.Sprintf("%v", input)
	return
}

func deepEqual(a, b interface{}) bool {
	// fmt.Printf("a:\n%s\nb:\n%s\n", pretty(a), pretty(b))
	return pretty(a) == pretty(b)
}

func mkbuf(s string) []byte {
	tmp := []byte(s)
	return tmp
}

/*
func mkpath(t *testing.T, db *Db, class, s string) (path *Path) {
	path, err := pathFromString(db, class, "sha256", s)
	if err != nil {
		t.Fatal(err)
	}
	return
}
*/

func nonMissingErr(err error) error {
	switch err.(type) {
	case *os.PathError:
		return nil
	case nil:
		return nil
	}
	return err
}

// an example of how an Object might be used
func objectExample(t *testing.T, o Object) {

	abspath := o.GetPath().Abs
	tassert(t, len(abspath) > 0, "path len %v", len(abspath))
	// fmt.Printf("object path %s\n", abspath)

	size, err := o.Size()
	tassert(t, err == nil, "Blob.Size() size %d err %v", size, err)
	// fmt.Printf("object %s is %d bytes\n", o.GetPath().Canon, size)
}

func objs2str(objects []Object) (out string) {
	for _, obj := range objects {
		line := string(obj.GetPath().Canon)
		line = strings.TrimSpace(line) + "\n"
		out += line
	}
	return
}

func pathEqual(a, b *Path) bool {
	return a.Rel == b.Rel && a.Canon == b.Canon
}

func pathFromBuf(db *Db, class string, algo string, buf []byte) (path *Path, err error) {
	b := append([]byte(class+"\n"), buf...)
	binhash, err := Hash(algo, b)
	if err != nil {
		return
	}
	hash := bin2hex(binhash)
	path, err = Path{}.New(db, filepath.Join(class, algo, hash))
	Ck(err)
	return
}

// XXX deprecate
func pathFromString(db *Db, class, algo, s string) (path *Path, err error) {
	buf := []byte(s)
	return pathFromBuf(db, class, algo, buf)
}

func setup(t *testing.T, db *Db) *Db {
	// XXX test other depths
	// db, err = Db{Depth: 4}.Create(dir)

	var err error
	var dir string

	if db == nil {
		db = &Db{}
	}
	Assert(db.Dir == "")

	debug := os.Getenv("DEBUG")
	if debug == "1" {
		dir, err = ioutil.TempDir("", testDbDirPrefix)
		Ck(err)
		fmt.Println(dir)
		// no cleanup
	} else {
		dir = t.TempDir()
		// automatically cleaned up
	}
	db.Dir = dir

	db, err = db.Create()
	Ck(err)
	db, err = Open(dir)
	Ck(err)
	tassert(t, db != nil, "db is nil")

	return db
}

func shell(path string, args ...string) (out []byte, err error) {
	cmd := exec.Command(path, args...)
	out, err = cmd.CombinedOutput()
	return
}

/*
func rmdb(db *Db) {
	// remove symlinks and '..'
	dir, err := filepath.Abs(db.Dir)
	Ck(err)
	dir, err = filepath.EvalSymlinks(dir)
	Ck(err)
	// make sure dir starts with /tmp/pitbase*
	Assert(len(testDbDirPrefix) > 0)
	pattern := fmt.Sprintf("/tmp/%s*", testDbDirPrefix)
	match, err := filepath.Match(pattern, dir)
	Assert(err == nil, err)
	Assert(match)
	// rm -rf
	err = os.RemoveAll(dir)
	Ck(err)
}
*/

// test boolean condition
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
	}
}

func TestGetGID(t *testing.T) {
	n := GetGID()
	if n == 0 {
		t.Fatalf("oh no n is 0")
	}
}

func TestHash(t *testing.T) {
	val := mkbuf("somevalue")
	binhash, err := Hash("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	hexhash := bin2hex(binhash)
	expect := "70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342"
	tassert(t, expect == hexhash, "expected %q got %q", expect, hexhash)

	binhash, err = Hash("sha512", val)
	if err != nil {
		t.Fatal(err)
	}
	hexhash = bin2hex(binhash)
	expect = "8e77e71abe427ced1c93d883aeeddfa57ce39b787f229caaf176fdd71353f3466d340a2cdb5a219c429c53ad37f2f144c7ce01b985b6b33e397c4b8fd1433cc3"
	tassert(t, expect == hexhash, "expected %q got %q", expect, hexhash)

	binhash, err = Hash("foobar", val)
	if err == nil {
		t.Fatal("expected error, received none")
	}

	//expecterr := fmt.Errorf("not implemented: %s", "foobar")
	//binhash, err = Hash("foobar", val)
	//tassert(t, err == expecterr, "expected %q got %q", err, expecterr)
}

/*
func TestMain(m *testing.M) {
	rc := m.Run()
	if rc == 0 {
	}
	os.Exit(rc)
}
*/

// XXX add chattr for failure test
func TestMkdir(t *testing.T) {
	err := mkdir("/etc/foobar")
	if err == nil {
		t.Fatal("expected error, got none")
	}
}

/*
func TestPath(t *testing.T) {
	db := setup(t)
	val := mkbuf("somevalue")
	path, err := pathFromBuf(db, "blob", "sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	expectabs := filepath.Join(db.Dir, "blob/sha256/70a/524/70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342")
	gotabs := path.Abs
	if expectabs != gotabs {
		t.Fatalf("expected %s, got %s", path.Abs, gotabs)
	}
}
*/
