# Splitting the Database Package into "db", "hashkv", and "kv"

This document describes a plan to refactor the current db package into three distinct packages:

• The **db** package will continue to provide the high–level
functionality (Merkle trees, streams, block deduplication,
verification, and domain logic). However, instead of talking directly
to the file system, it will delegate storage of raw data blocks and
trees to the hashkv layer.

• The **hashkv** package will provide content–addressable storage
built on top of a generic key–value interface. It will expose an
io.Reader/io.Writer abstraction such that writing data returns its
content hash and reading data requires the hash. Internally, hashkv
will use the kv package as its storage backend.

• The **kv** package will offer a minimal key–value store interface.
Its responsibility is to provide basic operations—Get, Put, and
Delete—for storing arbitrary byte slices. Keys must be restricted to
alphanumeric characters and must not contain slashes. This package is
designed to be simple so that its underlying implementation can later
be replaced if needed.

Below we describe which existing files should be moved or modified,
what modifications are needed for each package, and what new files
should be created.

---

## Package Responsibilities and File Mapping

### 1. db Package (High–Level Database Functionality)

**Responsibilities:**

- Maintain all domain logic (Merkle tree management, block and stream
  abstraction, lookup logic, verification, object addressing, assembly
  of transactions, etc.).
- Delegate raw data storage/retrieval to the hashkv package rather
  than directly performing file system operations.
- Continue to expose the current API (e.g. PutBlock, GetBlock,
  PutTree, GetTree, OpenStream, etc.) but rewritten so that
  lower–level I/O calls are replaced with calls into hashkv.

**Files to Remain (with modifications):**

- `tree.go` – Remains in db; methods like `AppendBlock()`, traversal,
  Read/Seek/Tell must call hashkv (instead of directly calling worm or
  file I/O).
- `stream.go` – Remains in db; higher–level stream operations (e.g.
  linking a stream, appending blocks) must be adapted to use hashkv
  writes.
- `db.go` – High–level methods such as PutBlock, PutTree, GetTree,
  PutStream, and OpenStream will be updated to call hashkv functions.
  For example, instead of writing a block to a temporary file and
  renaming it, the method will call something like `hashkv.Put(data)`.
- `path.go` and other utility files (e.g. pitbase.go, account.go,
  tree_test.go, db_test.go, and stream_test.go) remain in the db
  package and use the new underlying storage abstraction.

**Modifications Required:**

- Remove or refactor low–level file handling (the worm type, header
  management, file renaming, etc.) such that these functions call
  corresponding methods in hashkv.
- Update tests in the db package to verify that high–level operations
  work correctly when the underlying storage is provided by hashkv.
- Remove direct dependencies on OS file I/O in functions like
  CreateWorm and OpenWorm; these routines will be transferred to
  hashkv.

---

### 2. hashkv Package (Content–Addressable Storage with CAS Semantics)

**Responsibilities:**
• Provide an io.Reader/io.Writer interface for writing and retrieving data.
• When data is written, compute its hash (using algorithms such as sha256 or sha512) and return that hash to the caller.
• When data is read, require the hashed key as input and then retrieve the stored data.
• Internally, use the kv package to store the raw byte slices.
• Serve as an adapter between the high–level db operations and the generic key–value store.

**Files to Move/Modify:**
• `file.go` – The worm type and its associated file I/O methods (Write, Read, Seek, etc.) are low–level. They should be moved to hashkv. Their logic (e.g. header management, temp file usage, calculating the hash on close, and renaming) will be preserved but restructured to implement the hashkv io.Writer/io.Reader contract.
• `blob.go` – Currently wraps worm in a Block object; its functionality will be used to implement block storage in hashkv. Methods such as New (to wrap a worm) and GetPath for retrieving a CAS–based path will be migrated.
• (Optionally) `chunker.go` – As the chunking functionality is used in PutStream, this can remain in db if viewed as higher–level. However, if chunking is tightly coupled with how blocks are stored, some helper functions might be moved into hashkv.
• Corresponding tests (e.g. portions of blob_test.go) that specifically test low–level block operations should be moved into hashkv’s test suite.

**Modifications Required:**
• Change package declarations from "db" to "hashkv".
• Refactor functions like CreateWorm and OpenWorm so that they do not expose raw file system paths to db; instead, they call kv’s methods to store or retrieve data.
• Update methods so that reading and writing follow the io.Reader/io.Writer interface. For example, Write() should return the full count of written bytes and eventually provide the content hash when the write operation is complete.
• Testing routines should be updated to reflect that hashkv now sits between db and the kv layer.

---

### 3. kv Package (Minimal Key–Value Store)

**Responsibilities:**
• Offer a simple key–value store interface with these operations:
  - Put(key string, value []byte) error
  - Get(key string) ([]byte, error)
  - Delete(key string) error
  - (Optionally) List(prefix string) ([]string, error)
• Define strict key rules: keys must be alphanumeric and contain no slashes.
• Implement a basic file–based backend mapping each key to a file name relative to a configurable root directory.
• Leave room for possible future alternative implementations (in–memory, networked, etc.) by exposing a well–defined interface.

**New Files to Create:**
• Create a new package directory (for example, `kv` or `x/kv`) with a file such as `kv.go` that defines the KvStore interface and its implementation.
• Create tests for the kv package (e.g. `kv_test.go`) to verify that basic operations (Get, Put, Delete, List) behave as intended.
• Documentation and examples for using this minimal key–value store interface.

**Modifications Required:**
• The hashkv package must be modified to use kv.KvStore instead of performing direct file I/O – this is done via adapter functions that convert content keys (hashes) into the underlying key–value operations.
• Certain configuration options (like the root directory and directory nesting) for the file–based kv store must be added in the constructor function (e.g. NewKvStore(rootDir string)).

---

## Migration and Integration Strategy

1. **Adapter Functions:**  
   Begin by writing adapter functions in the db package to wrap existing file–system–based functions. Then refactor these functions to delegate to methods in hashkv rather than handling files directly.

2. **Incremental Refactoring:**  
   Move the low–level storage functions (worm type, header processing, etc.) from the db package to hashkv. Do not break higher–level APIs; instead, have the db layer call into hashkv once the adapters are in place.

3. **Testing and Verification:**  
   Run all existing tests in the db package to ensure that high–level functionality (tree creation, stream operations, block deduplication) works correctly when using hashkv. Create and run tests for the new kv package.

4. **Documentation Update:**  
   Update developer documentation and inline comments to describe the new layered architecture:
   - db uses hashkv for storage.
   - hashkv handles content addressing and delegates storage to the kv package.
   - kv provides a minimal key–value interface.

5. **Performance Benchmarking:**  
   Benchmark key operations to check for any performance regressions after introducing the extra layer. Optimize the kv and hashkv implementations if needed.

---

## Summary

- The **db** package will continue providing high–level database functionality, but all low–level storage calls are now implemented by hashkv.
- The new **hashkv** package will handle content–addressable storage with CAS semantics via an io.Reader/io.Writer interface; it delegates raw byte storage to the kv package.
- The **kv** package will provide a minimal generic key–value store interface that can be implemented using a simple file–based backend.
- Existing files in the current db package will be reorganized: high–level logic (tree, stream, db orchestration, path handling) remains in db; low–level file operations (worm, block storage) move to hashkv; and new files implementing the KvStore interface are created in the kv package.
- The plan emphasizes incremental refactoring, thorough testing, and clear documentation to ensure a maintainable and modular system.

This phased approach allows us to isolate storage backends easily (for example, switching to an embedded database or network service) without disturbing the domain logic housed in the db package.
