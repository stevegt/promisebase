# Layered Architecture Overview
The Promisebase codebase can be structured into multiple layers.
This document describes a possible separation into at least three
distinct layers that work together to provide content-addressable
storage, high-level domain logic, and user interfaces. In addition, a
message storage module supporting DAGs and timeline or hypergraph
structures can be used to record event histories.

## Storage/KV Layer 

This is the lowest level and is responsible for raw data storage.
The KV layer underlies all other layers by mapping keys to stored bytes,
ensuring data persistence and basic retrieval semantics.

The KV layer must automatically and transparently create
subdirectories or nested buckets in the underlying storage system as
needed to avoid performance degradation from too many files or objects
in a single directory or bucket.

Because the chunking code already stores and passes around each chunk
in RAM, there is no need for a file-like Reader/Writer interface at
this layer. Instead, the interface should simply expose operations such
as:
- Get, Put, and Delete for arbitrary byte sequences.
- Enforced key naming rules (alphanumeric, no slashes) to ensure file
  safety.

Dependencies:
- None

## Content-Addressable (hashkv) Layer

The hashkv layer sits directly above the KV layer. Its main function
is to compute a content hash for each incoming chunk and to store the
data as content-addressable blocks. Because the chunking code already
stores and passes around each chunk in RAM, there is no need for a
file-like Reader/Writer interface at this layer. Instead, the interface
should simply expose operations such as:

- Put(data []byte) (cid string, err error)
- Get(cid string) ([]byte, error)
- Delete(cid string) error

Dependencies:
- KV Layer

## Reference layer

The reference layer builds on top of kv to provide human-friendly
names for content hashes. It manages mutable references that point to
immutable content identifiers (CIDs). This layer allows users to create,
update, rename, and delete refs:
- Create(ref string, cid string) error
- Replace(ref string, cid string) error
- Rename(oldRef, newRef string) error
- Delete(ref string) error
- ReadLink(ref string) (cid string, error)

Dependencies:
- KV layer

## Streaming layer

The streaming layer builds on top of hashkv and ref to provide a
higher-level abstraction for managing streams of data. It handles the
logic for:

- Writing streams by chunking data, hashing each chunk, storing it via
  hashkv, and building and storing Merkle tree inner nodes to represent
  the stream data stored in leaf nodes.
- Assigning a human-friendly ref to each stream via the ref layer.
- Reading streams by reference.

It supports an io.Reader/io.Writer interface for stream operations:

- NewStream(ref string) (Stream, error)
  - if ref is empty, a uuid-based ref is created and can be obtained via
    Stream.GetRef()
- Write(data []byte) (int, error)
- Read([]byte) (int, error)
- Close() error  

**Rabin Chunking Integration:**
The streaming layer integrates Rabin chunking to divide incoming data
into content-defined chunks. This method uses a rolling hash with a
chosen polynomial to determine variable-length chunk boundaries. The
resulting chunks, which are kept in memory, are then hashed and stored
via the KV layer.

Dependencies:
- Content-Addressable (hashkv) Layer
- Reference layer

## Directory tree layer

The directory tree layer builds on top of hashkv and ref to manage the
import and export of files and directory trees. It handles the logic
for:

- Importing a directory structure from the filesystem
- Importing changes to an existing stored directory tree
- Storing each new or changed file via the streaming layer, including
  POSIX metadata
- Storing new or changed directory tree nodes via the streaming layer,
  including POSIX metadata
- Showing differences between two stored directory trees
- Showing differences between a stored directory tree and the
  filesystem
- Filtering and ignoring files during import or comparison based on
  .gitignore-style patterns
- Listing the contents of a stored directory tree, including POSIX
  metadata
- Exporting a stored directory tree back to the filesystem, including
  POSIX metadata
- Extracting a single file from a stored directory tree to stdout
- Extracting a single file from a stored directory tree back to the
  filesystem, including POSIX metadata

The directory tree layer supports operations such as:

- DiffTree(treeCID1, treeCID2 string, options DiffOptions) ([]DiffEntry, error)
- DiffFS(treeCID string, path string, options DiffOptions) ([]DiffEntry, error)
- Import(path string) (cid string, error)
- List(cid string, options ListOptions) ([]DirEntry, error)
- Cat(cid string) (io.Reader, error)
- Export(cid string, path string) error

Dependencies:
- Content-Addressable (hashkv) Layer
- Reference layer
- Streaming layer

## VCS layer

The VCS layer builds on top of the directory tree and streaming layers
to provide version control functionality. It handles the logic for:

- Committing changes to a directory tree, creating a new tree object
  that references the previous tree and includes metadata such as
  author, timestamp, and commit message
- Creating and managing branches and tags as refs that point to specific
  commit objects
- Merging branches and resolving conflicts
- Viewing the history of commits, including metadata and diffs between
  trees
- Checking out a specific commit, branch, or tag to restore the
  directory tree to that state

The VCS layer supports operations such as:

- Commit(treeCID string, message string, author string, parents []CID) (commitCID CID, error)
- Branch(name string, commitCID string) error
- Tag(name string, commitCID string) error
- Merge(branch1 string, branch2 string) (commitCID string, error)
- Log(ref string, options LogOptions) ([]CommitEntry, error)
- Checkout(ref string, path string) error

Dependencies:
- Directory tree layer
- Streaming layer
- Reference layer

## Message and Timeline Layer
This layer records each message as a DAG node that includes parent references. It
captures a sequence of commands or events analogous to a commit history
or timeline, leveraging the VCS layer for storage and versioning capabilities.

**Interface Methods:**
- RecordMessage(msg Message) error
- GetMessage(cid string) (Message, error)
- ListMessages() ([]Message, error)

Dependencies:
- VCS Layer

## User Interface Layer

Interface layers such as FUSE mounts or web/CLI frontends provide user interaction with the
system through the Message and Timeline layer.

Dependencies:
- Message and Timeline Layer

