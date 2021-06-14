package db

// . "github.com/stevegt/goadapt"

type Blob struct {
	Db *Db
	*WORM
}

func (blob *Blob) GetPath() *Path {
	return blob.Path
}

func (blob Blob) New(db *Db, file *WORM) *Blob {
	blob.Db = db
	blob.WORM = file
	return &blob
}
