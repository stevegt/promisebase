# Layered Architecture Overview
The Promisebase codebase can be structured into multiple layers.
This document describes a possible separation into at least three
distinct layers that work together to provide content-addressable
storage, high-level domain logic, and user interfaces. In addition,
a message storage module supporting DAGs and timeline or hypergraph
structures can be used to record event histories.

## 1. Storage/KV Layer
This is the lowest level and is responsible for raw data storage.
It provides a minimal key-value interface with operations such as:
- Get, Put, and Delete for arbitrary byte sequences.
- Enforced key naming rules (alphanumeric, no slashes) to ensure file
  safety.
- A simple file-based backend that can later be replaced if needed.

**Interface Methods:**
- Write(data []byte) error  
- Read(key string) ([]byte, error)  
- Close() error
- Rename(oldKey, newKey string) error  
- Delete(key string) error

The KV layer underlies all other layers by mapping keys to stored
bytes, ensuring data persistence and basic retrieval semantics.

## 2. Content-Addressable (hashkv) Layer
The hashkv layer sits directly above the KV layer. Its main functions
are to expose an io.Reader/io.Writer interface that computes a content
hash upon writing and to store data as content-addressable blocks.
It encapsulates low-level file handling such as header management,
temporary file usage, and atomic renaming. It serves as an adapter
between high-level domain logic and the raw KV operations.

**Interface Methods:**
- Put(data []byte) (cid string, err error)  
- Get(cid string) (io.Reader, error)  
- Delete(cid string) error

## 3. High-Level Database (db) Layer
The db layer sits at the top of the core storage stack and implements
the domain logic of Promisebase. It manages and manipulates Merkle trees,
stream abstractions, and block deduplication. Object lookup and
verification are performed using the underlying hashkv functions.
This layer depends on hashkv for all low-level operations and focuses
on data semantics rather than storage details.

**Interface Methods:**
- PutBlock(algo string, data []byte) (Block, error)  
- GetBlock(cid string) (io.Reader, error)  
- PutTree(algo string,
  children ...Object) (Tree, error)  
- GetTree(cid string) (Tree, error)  
- OpenStream(name string) (Stream, error)  
- AppendBlock(…) (Tree, error)

## 4. Message and Timeline Layer
A separate module handles message storage and event timelines.
This layer records each message as a DAG node that includes parent
references. It captures a sequence of commands or events analogous to
a commit history or timeline.

**Interface Methods:**
- RecordMessage(msg Message) error  
- GetMessage(cid string) (Message, error)  
- ListMessages() ([]Message, error)

- Pros:
  - Provides a clear historical record of changes using a DAG structure.
  - Enhances auditability and permits reconstruction of event histories.
  - Supports complex workflows with branching timelines.

- Cons:
  - Increases system complexity and may require extensive refactoring.
  - Maintenance of a DAG or hypergraph can incur additional overhead.
  - Integration with other modules demands careful consistency checks.

## Layer Interactions
- The **DB layer** calls functions in the hashkv layer to store and
  retrieve high-level objects (blocks, trees, streams). It uses the
  resulting content identifiers to maintain referential integrity.
- The **hashkv layer** converts data writes into content addresses and
  passes them to the KV layer. It similarly translates KV reads into an
  io.Reader for the DB layer.
- The **KV layer** underpins the system by providing basic storage,
  leaving higher-level concerns (such as content addressing and domain
  logic) to the upper layers.
- The **Message and Timeline layer** interacts with the DB layer to
  record messages as DAG nodes and to manage event timelines. It supplies
  interfaces for tracing history and supports command sourcing.

By cleanly separating responsibilities, the system benefits in terms
of modularity, testing, and the ability to swap out implementations at
each layer if needed.

Additional interface layers, such as FUSE mounts or web/CLI frontends,
may be built on top of the DB layer to provide user interaction with the
system.
