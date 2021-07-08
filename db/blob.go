package db

// . "github.com/stevegt/goadapt"

type Blob struct {
	Db *Db
	*worm
}

func (blob *Blob) GetPath() *Path {
	return blob.Path
}

func (blob Blob) New(db *Db, file *worm) *Blob {
	blob.Db = db
	blob.WORM = file
	return &blob
}
