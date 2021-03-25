/*

Pitbase is a content-addressable deduplicating database able to store
data streams of arbitrary size.

Vocabulary:

- abspath: absolute path on hard disk, including subdirs
- relpath: path relative to db.Dir, including subdirs
- canpath: canonical path, without subdirs
- hash: cryptographic hash of a blob or node
- algo: name (string) describing hash algorithm
- subdir: three-character hexadecimal segment of hash
- subdirs: one or more subdir segments inserted in abspath or relpath
	in order to keep directory sizes small; the number of subdirs is fixed
	at database creation
- blob: chunk or block of data; deduplication atom; stored as file
- node: list of one or more blobs or nodes; stored as blob or node canpaths
- rootnode: the top-level node for a stream
- stream: ordered set of one or more blobs; stored as rootnode canpath
- label: human-readable name of a stream; stored as symlink pointing at rootnode canpath
- object: blob, node, or stream; addressed by canpath
- key: in-memory representation of a blob or node location; provides
  methods for generating abspath, relpath, canpath, or hash XXX deprecate?

*/

package pitbase
