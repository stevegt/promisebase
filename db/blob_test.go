package db

import (
	"bytes"
	"io"
	"testing"
)

func TestBlock(t *testing.T) {
	db := setup(t, nil)

	hash := "303f7138f2be7b918fbd55af43653760b16f6b13a046fa0d71dd7a3909b64486"
	canpath := "block/sha256/303f7138f2be7b918fbd55af43653760b16f6b13a046fa0d71dd7a3909b64486"
	relpath := "block/sha256/303/f71/303f7138f2be7b918fbd55af43653760b16f6b13a046fa0d71dd7a3909b64486"
	file, err := CreateWorm(db, "block", "sha256")
	tassert(t, err == nil, "File.New err %v", err)
	b := Block{}.New(db, file)

	// put something in the block
	data := mkbuf("somedata")
	nwrite, err := b.Write(data)
	tassert(t, err == nil, "b.Write err %v", err)
	tassert(t, nwrite == len(data), "b.Write len expected %v, got %v", len(data), nwrite)

	// close writeable
	err = b.Close()
	tassert(t, err == nil, "b.Close() err %v", err)

	// re-open readable
	path, err := Path{}.New(db, canpath)
	tassert(t, err == nil, "File.New err %v", err)
	file, err = OpenWorm(db, path)
	tassert(t, err == nil, "File.New err %v", err)
	b = Block{}.New(db, file)
	tassert(t, err == nil, "OpenBlock err %v", err)

	// check size
	size, err := b.Size()
	tassert(t, err == nil, "Block.Size() size %d err %v", size, err)
	// fmt.Printf("object %s is %d bytes\n", b.Path.Canon, size)

	// seek from start
	testSeek(t, b, 2, io.SeekStart, 2, mkbuf("medata"))

	// seek from end
	testSeek(t, b, -3, io.SeekEnd, 5, mkbuf("ata"))

	// seek from current
	hl := int64(len("block\n"))
	tellpos, err := b.fh.Seek(4+hl, io.SeekStart)
	tassert(t, tellpos == 4+hl, "tellpos %v", tellpos)
	testSeek(t, b, -1, io.SeekCurrent, 3, mkbuf("edata"))

	// ensure we can't write to a read-only block
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
	tassert(t, class == "block", "class '%v'", class)

	algo := b.Path.Algo
	tassert(t, algo == "sha256", "algo '%v'", algo)

	gothash := b.Path.Hash
	tassert(t, gothash == hash, "hash '%v'", gothash)

	gotcanpath := b.Path.Canon
	tassert(t, canpath == gotcanpath, "canpath '%v'", gotcanpath)

}

func testSeek(t *testing.T, b *Block, seekpos int64, whence int, tellpos int64, expect []byte) {

	// seek from whence
	nseek, err := b.Seek(seekpos, whence)
	tassert(t, err == nil, "Seek err %v, seekpos %v, whence %v", err, seekpos, whence)
	tassert(t, nseek == tellpos, "expected nseek %v, got %v, whence %v", tellpos, nseek, whence)

	// check our current location
	ntell, err := b.Tell()
	tassert(t, err == nil, "b.Tell err %v", err)
	tassert(t, ntell == tellpos, "b.Tell expected %v, got %v", tellpos, ntell)

	// read the rest of the buffer starting from that location
	buf := make([]byte, 100)
	nread, err := b.Read(buf)
	// fmt.Printf("dsaf nread %#v buf %#v", nread, buf)
	tassert(t, err == nil, "b.Read err %v", err)
	tassert(t, nread == len(expect), "b.Read len expected %v, got %v", len(expect), nread)
	got := buf[:nread]
	tassert(t, bytes.Compare(expect, got) == 0, "b.Read expected %v, got %v", expect, got)

}
