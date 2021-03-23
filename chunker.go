package pitbase

import (
	"io"

	resticRabin "github.com/restic/chunker"
)

const (
	kiB = 1024
	miB = 1024 * kiB

	// MinSize is the default minimal size of a chunk.
	defMinSize = 512 * kiB
	// MaxSize is the default maximal size of a chunk.
	defMaxSize = 8 * miB
)

// Rabin lightly wraps restic's chunker on the slight chance that we
// might need to replace it someday.
// XXX restic's Next() does copies rather than passing pointers --
// we might want to replace restic's lib sooner rather than later
type Rabin struct {
	Poly    resticRabin.Pol
	C       *resticRabin.Chunker
	MinSize uint
	MaxSize uint
}

type Chunk resticRabin.Chunk

func (c Rabin) Init() (res *Rabin, err error) {
	if c.MinSize == 0 {
		c.MinSize = defMinSize
	}
	if c.MaxSize == 0 {
		c.MaxSize = defMaxSize
	}
	if c.Poly == 0 {
		c.Poly, err = resticRabin.RandomPolynomial()
	}
	return &c, err
}

/*
type CopyRes struct {
	n   int64
	err error
}

// XXX combine this with Init()
func (c *Rabin) Run(buf []byte) (src io.Writer, chunks chan *Chunk) {
	var copyres chan CopyRes
	dst, pipeWriter := io.Pipe()
	go func() {
		// XXX still obviously confused
		n, err := io.Copy(pipeWriter, src)
		copyres <- CopyRes{n, err}
	}()

	c.Start(dst)

	for {
		chunk, err := c.Next(buf)
		if err != nil {
			chunk.Err = err // XXX see above note at Chunk struct
			chunks <- chunk
			break
		}
		chunks <- chunk
	}

	return
}
*/

func (c *Rabin) Start(rd io.Reader) {
	c.C = resticRabin.NewWithBoundaries(rd, c.Poly, c.MinSize, c.MaxSize)
}

func (c *Rabin) Next(buf []byte) (chunk resticRabin.Chunk, err error) {
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

	return
}
