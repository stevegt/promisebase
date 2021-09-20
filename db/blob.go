package db

// . "github.com/stevegt/goadapt"

type Block struct {
	Db *Db
	*worm
}

func (block *Block) GetPath() *Path {
	return block.Path
}

func (block Block) New(db *Db, file *worm) *Block {
	block.Db = db
	block.worm = file
	return &block
}
