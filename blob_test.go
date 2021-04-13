package pitbase

import (
	"bytes"
	"testing"
)

func TestBlob(t *testing.T) {
	db := setup(t, nil)

	hash := "d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	canpath := "blob/sha256/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	relpath := "blob/sha256/d2c/71a/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	path := Path{}.New(db, canpath)
	file, err := File{}.New(db, path)
	tassert(t, err == nil, "File.New err %v", err)
	b := Blob{}.New(db, file)

	// put something in the blob
	data := mkbuf("somedata")
	nwrite, err := b.Write(data)
	tassert(t, err == nil, "b.Write err %v", err)
	tassert(t, nwrite == len(data), "b.Write len expected %v, got %v", len(data), nwrite)

	// close writeable
	err = b.Close()
	tassert(t, err == nil, "b.Close() err %v", err)

	// re-open readable
	file, err = File{}.New(db, path)
	tassert(t, err == nil, "File.New err %v", err)
	b = Blob{}.New(db, file)
	tassert(t, err == nil, "OpenBlob err %v", err)

	// check size
	size, err := b.Size()
	tassert(t, err == nil, "Blob.Size() size %d err %v", size, err)
	// fmt.Printf("object %s is %d bytes\n", b.Path.Canon, size)

	// seek to a location
	nseek, err := b.Seek(2, 0)
	tassert(t, err == nil, "b.Seek err %v", err)
	tassert(t, nseek == int64(2), "b.Seek expected %v, got %v", 2, nseek)

	// check our current location
	ntell, err := b.Tell()
	tassert(t, err == nil, "b.Tell err %v", err)
	tassert(t, ntell == 2, "b.Tell expected %v, got %v", 2, ntell)

	// read from that location
	buf := make([]byte, 100)
	nread, err := b.Read(buf)
	// fmt.Printf("dsaf nread %#v buf %#v", nread, buf)
	tassert(t, err == nil, "b.Read err %v", err)
	tassert(t, nread == 6, "b.Read len expected %v, got %v", 6, nread)
	expect := mkbuf("medata")
	got := buf[:nread]
	tassert(t, bytes.Compare(expect, got) == 0, "b.Read expected %v, got %v", expect, got)

	// ensure we can't write to a read-only blob
	_, err = b.Write(data)
	tassert(t, err != nil, "b.Write to a read-only file should throw error")

	// test Object methods
	objectExample(t, b)

	abspath := b.Path.Abs
	tassert(t, len(abspath) > 11, "path len %v", len(abspath))
	// fmt.Printf("object path %s\n", abspath)

	gotrelpath := b.Path.Rel
	tassert(t, relpath == gotrelpath, "relpath '%v'", gotrelpath)

	class := b.Path.Class
	tassert(t, class == "blob", "class '%v'", class)

	algo := b.Path.Algo
	tassert(t, algo == "sha256", "algo '%v'", algo)

	gothash := b.Path.Hash
	tassert(t, gothash == hash, "hash '%v'", gothash)

	gotcanpath := b.Path.Canon
	tassert(t, canpath == gotcanpath, "canpath '%v'", gotcanpath)

}
