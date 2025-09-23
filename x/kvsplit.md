# Splitting the Database Package into "db" and "kv"

This document outlines a plan to refactor the current `db` package into two distinct packages: `kv` and `db`. The aim is to isolate low-level key–value storage operations in the `kv` package and let the `db` package focus on higher-level functionality (such as Merkle tree management, block deduplication, stream handling, etc.) while using the `kv` package as its storage layer.

---

## Overview

- **kv Package:**  
  - Purpose: Provide a minimal, generic key–value store interface.
  - Responsibilities:
    - Store and retrieve raw byte slices by key.
    - Manage deletion and iteration within the storage space.
    - Hide the implementation details of the underlying storage (which may be file based).
    - Define a simple interface such as:
      
      • Put(key string, value []byte) error  
      • Get(key string) ([]byte, error)  
      • Delete(key string) error  
      • Optionally, List(prefix string) ([]string, error)
      
- **db Package:**  
  - Purpose: Provide higher-level functionality such as handling blocks, trees, and streams.
  - Responsibilities:
    - Use the `kv` package for all low-level I/O operations.
    - Continue to implement content-addressable storage constructs like block hashing, Merkle trees, and stream operations.
    - Expose the existing higher-level API and internally convert those operations into key–value calls via the `kv` package.

---

## Detailed Plan

### 1. Define the kv Interface

- Create a new package directory (e.g. `x/kv`) that will contain the key–value store code.
- In this package, define a minimal interface (`KvStore`) for operations. For example:

  - A type `KvStore` with methods:
    - `Put(key string, value []byte) error`
    - `Get(key string) ([]byte, error)`
    - `Delete(key string) error`
    - (Optional) `List(prefix string) ([]string, error)`

- Develop a simple file–based implementation using standard library functions (e.g. using `os` and `ioutil`). For instance, keys could be mapped onto file paths relative to a base directory established at store creation time.

- Include a basic constructor such as `NewKvStore(rootDir string) (KvStore, error)` that ensures the storage directory exists and establishes any configuration (such as directory depth or file naming conventions).

### 2. Refactor the db Package

- Identify all low-level I/O operations in the current `db` package (for instance, operations in worm, PutBlock, OpenWorm, file writes/reads, etc.) that deal directly with the underlying filesystem or temporary file handling.
  
- Replace direct file system calls with calls to the new `kv` store interface. For example:

  - In `PutBlock`, after computing the hash of the block, instead of writing a file via temporary file logic and rename operations, call `kvStore.Put(canonicalKey, blockBytes)` where the canonical key is derived from the block’s content.
  
  - In `GetBlock`, instead of directly opening a file, the `db` code calls `kvStore.Get(key)` to load the byte slice.

- Retain higher-level logic (Merkle tree assembly, block chaining, symlink updates for streams) entirely in the `db` package. The db package will now be responsible for:
  
  - Transforming domain objects (Block, Tree, Stream) to/from byte slices suitable for the `kv` package.
  - Managing hash computations, verifying data integrity, and invoking tree operations using the stored keys provided by the `kv` layer.

### 3. Migration and Compatibility

- Start by creating adapter functions in `db` that wrap the existing file–based logic with calls to the new `kv` interface. This will allow incremental refactoring.

- Gradually move chunks of storage-related code from `db` into `kv`. For example, the code that prepares file headers and paths (currently in the worm and Path structures) can be re-implemented as part of key composition for the `kv` store.

- Update tests for `db` (and related storage tests) so that they validate behavior against a kv–based store. Ensure that high-level functionality such as tree verification or block retrieval continues to work correctly.

### 4. Testing and Performance

- Once refactoring is complete, run the existing tests from the `db` package to verify that all operations (PutBlock, GetBlock, PutTree, streaming reads, etc.) behave as expected.

- Introduce additional unit tests for the `kv` package to confirm the correctness of `Put`, `Get`, and `Delete` operations as well as edge cases (e.g. missing keys, write errors).

- Consider performance implications of the additional abstraction. Benchmarking may reveal if further optimization (or replacement of file–based storage with a more efficient solution) is needed later.

### 5. Documentation

- Update the project documentation to reflect the new architecture.  
- Document both the minimal kv interface and the responsibilities of the high-level db package.

---

## Summary

The goal of this refactoring is to enforce a clean separation between low-level storage concerns and high–level database logic. By isolating the key–value store operations in a dedicated `kv` package, it becomes simpler to swap out or modify the storage backend (e.g., to use a networked key–value store or an embedded database) without impacting high–level functionality. The `db` package’s role is confined to managing content–addressable blocks, constructing Merkle trees, and handling stream operations, which in turn makes the overall system design more modular and maintainable.

This plan provides a roadmap for gradually migrating existing code, supported by comprehensive testing and clear interface boundaries between `kv` and `db`.

