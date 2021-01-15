package pitbase

import (
	"os"
)

// Inode contains various file-related items such as file descriptor,
// file handle, maybe some methods, etc.
type Inode struct {
	fd   uintptr
	fh   *os.File
	path string
	key  []byte
}
