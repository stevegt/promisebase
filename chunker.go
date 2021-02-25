package pitbase

import (
	"io"

	"github.com/restic/chunker"
)

const (
	kiB = 1024
	miB = 1024 * kiB

	// MinSize is the default minimal size of a chunk.
	defMinSize = 512 * kiB
	// MaxSize is the default maximal size of a chunk.
	defMaxSize = 8 * miB
)

// Chunker lightly wraps restic's chunker on the slight chance that we
// might need to replace it someday.
// XXX restic's Next() does copies rather than passing pointers --
// we might want to replace restic's lib sooner rather than later
type Chunker struct {
	Poly    chunker.Pol
	C       *chunker.Chunker
	MinSize uint
	MaxSize uint
}

func (c Chunker) Init() (res *Chunker, err error) {
	if c.MinSize == 0 {
		c.MinSize = defMinSize
	}
	if c.MaxSize == 0 {
		c.MaxSize = defMaxSize
	}
	if c.Poly == 0 {
		c.Poly, err = chunker.RandomPolynomial()
	}
	return &c, err
}

func (c *Chunker) Start(rd io.Reader) {
	c.C = chunker.NewWithBoundaries(rd, c.Poly, c.MinSize, c.MaxSize)
}

func (c *Chunker) Next(buf []byte) (chunk chunker.Chunk, err error) {
	// restic chunker.Next() is underdocumented -- as of this writing
	// it says:
	//
	// Next returns the position and length of the next chunk
	// of data. If an error occurs while reading, the error is
	// returned. Afterwards, the state of the current chunk is
	// undefined. When the last chunk has been returned, all
	// subsequent calls yield an io.EOF error.
	//
	// It should say something like:
	//
	// XXX we can't get the chunk back via the data []byte buffer,
	// because we're passing that to Next() as a value rather than as
	// a pointer. so the only way to get the chunk back is via
	// Chunk.Data
	//
	// XXX fix this and send them a PR:
	// Next fills the given empty buffer with the next chunk from the
	// io.Reader that was provided to New().  Next also returns a
	// Chunk struct containing the position and length of the chunk,
	// along with a duplicate copy of the buffer data.  If an error
	// occurs while reading, the error is returned. Afterwards, the
	// state of the current chunk is undefined. When the last chunk
	// has been returned, all subsequent calls yield an io.EOF error.
	//
	//

	// buf := make([]byte, 100000000)
	chunk, err = c.C.Next(buf)
	_ = err
	// data = buf         // nok

	// chunk, err := c.C.Next(buf)
	// data := chunk.Data // ok
	// data = *buf        // ok

	return
}
