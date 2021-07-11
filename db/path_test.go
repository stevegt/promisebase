package db

import (
	"path/filepath"
	"testing"
)

func TestPath(t *testing.T) {
	db := setup(t, nil)

	hash := "d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	addr := "sha256/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	canpath := "block/sha256/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	relpath := "block/sha256/d2c/71a/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"

	path, err := Path{}.New(db, canpath)
	tassert(t, err == nil, "%#v", err)

	expect := filepath.Join(db.Dir, relpath)
	got := path.Abs
	tassert(t, expect == got, "expected %s, got %s", expect, got)

	expect = canpath
	got = path.Canon
	tassert(t, expect == got, "expected %s, got %s", expect, got)

	expect = hash
	got = path.Hash
	tassert(t, expect == got, "expected %s, got %s", expect, got)

	expect = addr
	got = path.Addr
	tassert(t, expect == got, "expected %s, got %s", expect, got)

}
