/*

Pitbase is a content-addressable deduplicating database able to store
data streams of arbitrary size.

XXX move to an RFC

Vocabulary:

- abspath: absolute path on hard disk, including subdirs
- relpath: path relative to db.Dir, including subdirs
- canpath: canonical path; relpath without subdirs
- hash: cryptographic hash of a blob or tree
- algo: name (string) describing hash algorithm
- subdir: three-character hexadecimal segment of hash
- subdirs: one or more subdir segments inserted in abspath or relpath
	in order to keep directory sizes small; the number of subdirs is fixed
	at database creation
- blob: chunk or block of data; deduplication atom; stored as file
- tree: list of one or more blobs or trees; stored as file containing blob or tree canpaths
- rootnode: the top-level tree for a stream
- stream: ordered set of one or more blobs; stored as a symlink
  pointing at rootnode canpath
- label: human-readable name of a stream;
  stored as the name of the symlink pointing at rootnode canpath
- object: blob, tree, or stream
- address: a user-visible path, always points to a tree; canpath without leading "tree/"
	- XXX Node-only addresses preclude being able to ship blobs around
		between machines, and we may need to either include "blob" or
		"tree" in addr, or collapse the tree/ and blob/ dirs on disk.  If
		we do collapse the tree/ and blob/ dirs into e.g. object/, then
		we'll also need to change the tree file format to avoid preimage
		attacks.

*/

package db
