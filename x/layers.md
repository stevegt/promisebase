# Layered Architecture Overview

The Promisebase codebase implements an "everything's a message"
architecture where signed messages serve as foundational source
documents—similar to accounting source documents like invoices and
receipts. This document describes the layered structure from lowest‐level
storage through high–level user interfaces, with the Message layer serving
as the core archival foundation upon which all domain‐specific abstractions
are built.

## Storage/KV Layer 

This is the lowest level and is responsible for raw data storage. The
KV layer underlies all other layers by mapping keys to stored bytes,
ensuring data persistence and basic retrieval semantics.

The KV layer must automatically and transparently create subdirectories
or nested buckets in the underlying storage system as needed to avoid
performance degradation from too many files or objects in a single
directory or bucket.

Because the chunking code already stores and passes around each chunk
in RAM, there is no need for a file–like Reader/Writer interface at this
layer. Instead, the interface should simply expose operations such as:
- Get, Put, and Delete for arbitrary byte sequences
- Enforced key naming rules (alphanumeric, no slashes) to ensure file
  safety

Dependencies:
- None

## Content-Addressable (hashkv) Layer

The hashkv layer sits directly above the KV layer. Its main function is
to compute a content hash for each incoming chunk and to store the data as
content–addressable blocks. Because the chunking code already stores and
passes around each chunk in RAM, there is no need for a file–like
Reader/Writer interface at this layer. Instead, the interface should simply
expose operations such as:

- Put(data []byte) (cid string, err error)
- Get(cid string) ([]byte, error)
- Delete(cid string) error

Dependencies:
- KV Layer

## Message and Timeline Layer

The Message layer is the foundational archival system that stores signed
assertions as source documents. Each message is a cryptographically signed
statement that can reference one or more parent messages, creating a threaded
hypergraph structure. Messages can contain any type of assertion, promise, or
observation—from version control commits to computational results to
financial transactions to scientific observations.

Messages form a DAG (directed acyclic graph) where a message referencing
multiple parents represents a merge operation. This structure enables
PromiseBase to serve as an accounting system for both financial and
non–financial communications, with each message serving as verifiable evidence
that can be audited and traced through the hypergraph.

The protocol_CID field in each message serves dual purposes: routing messages
to appropriate handlers and enabling indexers to categorize and process message
content according to protocol–specific schemas.

**Interface Methods:**
- RecordMessage(msg Message) error
- GetMessage(cid string) (Message, error)
- ListMessages(options ListOptions) ([]Message, error)
- GetParents(cid string) ([]string, error)
- GetChildren(cid string) ([]string, error)

Dependencies:
- Content-Addressable (hashkv) Layer

## Reference Index Layer

The reference layer provides an index into the message archive, mapping
human–friendly names to message CIDs. It manages mutable references that
point to immutable messages in the archive. This layer allows users to
create, update, rename, and delete refs:

- Create(ref string, messageCID string) error
- Replace(ref string, messageCID string) error
- Rename(oldRef, newRef string) error
- Delete(ref string) error
- ReadLink(ref string) (messageCID string, error)

Dependencies:
- Message and Timeline Layer

## Streaming Layer

The streaming layer builds on top of the message archive to provide a
higher–level abstraction for managing streams of data. It handles the
logic for:

- Writing streams by chunking data, hashing each chunk, storing it via
  hashkv, and building and storing Merkle tree inner nodes to represent
  the stream data stored in leaf nodes
- Recording stream metadata as messages in the archive
- Assigning a human–friendly ref to each stream via the reference index
  layer
- Reading streams by reference

It supports an io.Reader/io.Writer interface for stream operations:

- NewStream(ref string) (Stream, error)
  - if ref is empty, a uuid–based ref is created and can be obtained via
    Stream.GetRef()
- Write(data []byte) (int, error)
- Read([]byte) (int, error)
- Close() error  

**Rabin Chunking Integration:**

The streaming layer integrates Rabin chunking to divide incoming data into
content–defined chunks. This method uses a rolling hash with a chosen
polynomial to determine variable–length chunk boundaries. The resulting
chunks, which are kept in memory, are then hashed and stored via the hashkv
layer.

Dependencies:
- Message and Timeline Layer
- Rabin Chunking (in-memory component)

## Directory Tree Layer

The directory tree layer builds on top of the message archive to manage the
import and export of files and directory trees. Each operation is recorded
as a message asserting changes to the filesystem state. It handles the
logic for:

- Importing a directory structure from the filesystem
- Importing changes to an existing stored directory tree
- Storing each new or changed file via the streaming layer, including POSIX
  metadata
- Storing new or changed directory tree nodes via the streaming layer,
  including POSIX metadata
- Recording directory tree operations as messages in the archive
- Showing differences between two stored directory trees
- Showing differences between a stored directory tree and the filesystem
- Filtering and ignoring files during import or comparison based on
  .gitignore–style patterns
- Listing the contents of a stored directory tree, including POSIX metadata
- Exporting a stored directory tree back to the filesystem, including POSIX
  metadata
- Extracting a single file from a stored directory tree to stdout
- Extracting a single file from a stored directory tree back to the filesystem,
  including POSIX metadata

The directory tree layer supports operations such as:

- DiffTree(treeCID1, treeCID2 string, options DiffOptions)
  ([]DiffEntry, error)
- DiffFS(treeCID string, path string, options DiffOptions)
  ([]DiffEntry, error)
- Import(path string) (cid string, error)
- List(cid string, options ListOptions) ([]DirEntry, error)
- Cat(cid string) (io.Reader, error)
- Export(cid string, path string) error

Dependencies:
- Reference Index Layer
- Streaming Layer

## VCS Layer

The VCS layer builds on top of the directory tree layer and message archive
to provide version control functionality. Each VCS operation (commit, branch,
merge) is recorded as a signed message in the archive, creating an auditable
history of all version control actions. It handles the logic for:

- Committing changes to a directory tree, creating a message that asserts
  "this is a better version of these files" and references the previous commit
  message
- Creating and managing branches and tags as refs in the reference index that
  point to specific commit messages
- Merging branches by creating a message that references multiple parent commit
  messages
- Viewing the history of commits by traversing the message DAG
- Checking out a specific commit, branch, or tag to restore the directory tree
  to that state

The VCS layer supports operations such as:

- Commit(treeCID string, message string, author string, parents []CID)
  (commitCID CID, error)
- Branch(name string, commitCID string) error
- Tag(name string, commitCID string) error
- Merge(branch1 string, branch2 string) (commitCID string, error)
- Log(ref string, options LogOptions) ([]CommitEntry, error)
- Checkout(ref string, path string) error

Dependencies:
- Directory Tree Layer
- Message and Timeline Layer

## Container Manager Layer

The Container Manager layer provides orchestration capabilities for
containerized workloads, serving as a Kubernetes replacement. It utilizes
the VCS, Directory Tree, Stream, and Reference Index layers to implement
infrastructure–as–code patterns where container definitions, configurations,
and deployment states are stored as versioned messages in the archive.

**Key Capabilities:**
- Container lifecycle management (create, start, stop, destroy)
- Service discovery and load balancing
- Resource allocation and scheduling
- Configuration management via VCS layer
- Deployment history tracking via Message layer

Dependencies:
- VCS Layer
- Directory Tree Layer
- Streaming Layer
- Reference Index Layer

## Bare Metal Manager Layer

The Bare Metal Manager layer provides configuration management and
orchestration for physical and virtual infrastructure, replacing legacy
DevOps tools like Puppet, Chef, and Ansible. It leverages the same foundational
layers as Container Manager to version and audit all infrastructure changes as
messages in the archive.

**Key Capabilities:**
- Infrastructure provisioning and configuration
- Configuration drift detection via VCS diff operations
- Rollback capabilities through message history
- Multi–host orchestration
- Idempotent operation execution

Dependencies:
- VCS Layer
- Directory Tree Layer
- Streaming Layer
- Reference Index Layer

## User Interface Layer

Interface layers such as FUSE mounts, web frontends, and CLI tools provide user
interaction with the system. These interfaces call into the VCS, Directory Tree,
Stream, and Reference Index layers to perform domain–specific operations, with all
actions ultimately recorded as messages in the foundational archive.

**Interface Types:**
- Command–line interface (CLI)
- Web–based user interface
- FUSE filesystem mount
- API endpoints for programmatic access

Dependencies:
- VCS Layer
- Directory Tree Layer
- Streaming Layer
- Reference Index Layer

## Architecture Summary

The architecture follows a clear call flow:

**UI → [VCS, DirTree, Stream, Refs] → Message → HashKV → KV**

This design treats messages as foundational source documents—signed
assertions that create an immutable, auditable hypergraph of all system
activity. Higher layers provide domain–specific abstractions (version control,
directory trees, streaming I/O) while building upon the message archive.
The protocol_CID in each message enables both routing to appropriate handlers
and protocol–aware indexing for efficient querying.

This "everything's a message" model enables PromiseBase to function as a
universal accounting system for computational assertions, extending beyond
traditional version control to support any domain requiring verifiable,
threaded communication with cryptographic accountability.
