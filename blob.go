package pitbase

// . "github.com/stevegt/goadapt"

type Blob struct {
	Db *Db
	*File
}

func (blob *Blob) GetPath() *Path {
	return blob.Path
}

func (blob Blob) New(db *Db, file *File) *Blob {
	blob.Db = db
	blob.File = file
	return &blob
}
