package db

// . "github.com/stevegt/goadapt"

type Block struct {
	Db *Db
	*worm
}

func (blob *Block) GetPath() *Path {
	return blob.Path
}

func (blob Block) New(db *Db, file *worm) *Block {
	blob.Db = db
	blob.worm = file
	return &blob
}
