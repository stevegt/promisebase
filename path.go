package pitbase

import (
	"fmt"
	"path/filepath"
	"strings"
)

type Path struct {
	Db    *Db
	Raw   string
	Abs   string // absolute
	Rel   string // relative
	Canon string // canonical
	Class string
	Algo  string
	Hash  string
	Addr  string
	Label string // stream label
}

func (path Path) New(db *Db, raw string) (res *Path) {
	path.Db = db
	path.Raw = raw

	// XXX need to also or instead call some sort of realpath function
	// here to deal with symlinks that might exist in the db.Dir path
	clean := filepath.Clean(raw)

	// remove db.Dir
	index := strings.Index(clean, path.Db.Dir)
	if index == 0 {
		clean = strings.Replace(clean, path.Db.Dir+"/", "", 1)
	}

	// split into parts
	parts := strings.Split(clean, "/")
	if len(parts) < 2 {
		panic(fmt.Errorf("malformed path: %s", raw))
	}
	path.Class = parts[0]
	if path.Class == "stream" {
		path.Label = filepath.Join(parts[1:]...)
		path.Rel = filepath.Join(path.Class, path.Label)
		path.Abs = filepath.Join(path.Db.Dir, path.Rel)
		path.Canon = path.Rel
	} else {
		if len(parts) < 3 {
			panic(fmt.Errorf("malformed path: %s", raw))
		}
		path.Algo = parts[1]
		// the last part of the path should always be the full hash,
		// regardless of whether we were given the full or canonical
		// path
		path.Hash = parts[len(parts)-1]
		// log.Debugf("anypath %#v class %#v algo %#v hash %#v", anypath, class, algo, hash)

		// Rel is the relative path of any type of input path.  We
		// use the nesting depth described in the Db comments.  We use the
		// full hash value in the last component of the path in order to make
		// troubleshooting using UNIX tools slightly easier (in contrast to
		// the way git truncates the leading subdir parts of the hash).
		var subpath string
		for i := 0; i < path.Db.Depth; i++ {
			subdir := path.Hash[(3 * i):((3 * i) + 3)]
			subpath = filepath.Join(subpath, subdir)
		}
		path.Rel = filepath.Join(path.Class, path.Algo, subpath, path.Hash)
		path.Abs = filepath.Join(path.Db.Dir, path.Rel)
		path.Canon = filepath.Join(path.Class, path.Algo, path.Hash)
		// Addr is a universally-unique address for the data stored at path.
		path.Addr = filepath.Join(path.Algo, path.Hash)
	}

	return &path
}

func (path *Path) header() string {
	return fmt.Sprintf(path.Class + "\n")
}
