# Layered Architecture Overview

The Promisebase codebase can be structured into multiple layers.
This document describes a possible separation into at least three
distinct layers that work together to provide content-addressable
storage, high-level domain logic, and user interfaces.

## 1. Storage/KV Layer

This is the lowest level and is responsible for raw data storage.
It provides a minimal key-value interface with operations such as:
- Get, Put, and Delete for arbitrary byte sequences.
- Enforced key naming rules (alphanumeric, no slashes) for file safety.
- A simple file–based backend that can later be replaced if needed.

The KV layer underlies all other layers by mapping keys to stored
bytes, ensuring data persistence and basic retrieval semantics.

## 2. Content–Addressable (hashkv) Layer

The hashkv layer sits directly above the KV layer.
Its main functions are:
- To expose an io.Reader/io.Writer interface that computes a content
  hash upon writing.
- To store data as content–addressable blocks, where the key is the hash
  returned by the KV layer.
- To encapsulate low–level file handling such as header management,
  temporary file usage, and renaming.
- To serve as an adapter between high–level domain logic and the raw KV
  operations.

By abstracting content addressing, this layer ensures data integrity
and deduplication across the system while hiding raw storage details.

## 3. High–Level Database (db) Layer

The db layer is at the top of the core storage stack.
It implements the domain logic of Promisebase such as:
- Management and manipulation of Merkle trees.
- Stream abstraction and block deduplication.
- Object lookup and verification using the underlying hashkv functions.
- Assembly of transactions and business rules based on content–addressed
  objects.

This layer depends on the hashkv layer for all low–level read/write
operations and focuses on the semantics of the data, rather than on
the details of its storage.

## Layer Interactions

- The **DB layer** calls functions in the hashkv layer to store and
  retrieve high–level objects (blocks, trees, streams). It uses the
  resulting content identifiers to maintain referential integrity.
- The **hashkv layer** converts data writes into content addresses and
  passes them to the KV layer. It similarly translates KV reads into an
  io.Reader for the DB layer.
- The **KV layer** underpins the system by providing basic storage,
  leaving higher–level concerns (such as content addressing and domain
  logic) to the upper layers.

By cleanly separating responsibilities, the system benefits in terms
of modularity, testing, and the ability to swap out implementations at
each layer if needed.

Additional interface layers, such as FUSE mounts or web and CLI
frontends, may be built on top of the DB layer to provide user
interaction with the system.

