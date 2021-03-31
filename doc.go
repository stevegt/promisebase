/*

Pitbase is a content-addressable deduplicating database able to store
data streams of arbitrary size.

XXX move to an RFC

Vocabulary:

- abspath: absolute path on hard disk, including subdirs
- relpath: path relative to db.Dir, including subdirs
- canpath: canonical path; relpath without subdirs
- hash: cryptographic hash of a blob or node
- algo: name (string) describing hash algorithm
- subdir: three-character hexadecimal segment of hash
- subdirs: one or more subdir segments inserted in abspath or relpath
	in order to keep directory sizes small; the number of subdirs is fixed
	at database creation
- blob: chunk or block of data; deduplication atom; stored as file
- node: list of one or more blobs or nodes; stored as file containing blob or node canpaths
- rootnode: the top-level node for a stream
- stream: ordered set of one or more blobs; stored as a symlink
  pointing at rootnode canpath
- label: human-readable name of a stream;
  stored as the name of the symlink pointing at rootnode canpath
- object: blob, node, or stream
- address: a user-visible path, always points to a node; canpath without leading "node/"
	- XXX Node-only addresses preclude being able to ship blobs around
		between machines, and we may need to either include "blob" or
		"node" in addr, or collapse the node/ and blob/ trees on disk.  If
		we do collapse the node/ and blob/ trees into e.g. object/, then
		we'll also need to change the node file format to avoid preimage
		attacks.

*/

package pitbase
