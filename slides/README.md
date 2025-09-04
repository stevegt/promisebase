class: center, middle

# PromiseBase: A Content-Addressable Storage System

---

# What is PromiseBase?

PromiseBase (also called "pitbase" in the code) is a **content-addressable storage (CAS)** system that stores data by its cryptographic hash rather than by filename or location.

Key features include:
- **Deduplication**: Identical data stored only once
- **Merkle trees**: Hierarchical organization with cryptographic verification
- **Streaming support**: Handles large files via content-defined chunking
- **FUSE filesystem**: Mount the database as a regular filesystem
- **Container integration**: Store and run Docker containers from the database

The system addresses the fundamental problem of data duplication and provides a foundation for distributed systems, version control, and container management.

---

# Why is this Important?

PromiseBase tackles several critical problems in modern computing:

**Data Deduplication**: Eliminates redundant storage of identical content, saving disk space and bandwidth. In a world where the same container images, datasets, and files are copied endlessly across systems, this provides massive efficiency gains.

**Content Verification**: Using cryptographic hashes as addresses makes data corruption immediately detectable. You can't retrieve corrupted data without knowing it's corrupted.

**Immutable Storage**: Once stored, content cannot be changed without changing its address. This provides strong guarantees for reproducibility and auditing.

**Distributed Systems Foundation**: Content-addressable storage is the building block for peer-to-peer networks, blockchain systems, and distributed version control systems like Git.

**Container Storage**: Modern container ecosystems involve massive duplication of layers and images. A CAS approach can dramatically reduce storage requirements.

---

# Who Wrote This?

The primary author is **stevegt** (Steve GT), based on:
- Git repository ownership and commit history
- Import paths referencing `github.com/stevegt/goadapt`
- File paths showing `/home/stevegt/lab/promisebase/`
- Copyright and attribution patterns in the code

This appears to be a personal research project exploring content-addressable storage systems, likely building on experience with distributed systems, containerization, and storage technologies.

---

# When Was This Written?

**Timeline: Approximately 2020-2021**

Evidence from the codebase:
- **Go 1.15** specified in go.mod (released August 2020)
- Modern dependencies like go-fuse v2, Docker client APIs
- References to container technologies and distributed systems patterns

**World Context (2020-2021):**
- **COVID-19 pandemic**: Massive shift to remote work and distributed computing
- **Container explosion**: Docker and Kubernetes becoming ubiquitous
- **Edge computing**: Need for efficient data distribution to edge nodes
- **Storage costs**: Growing concern about cloud storage expenses
- **Supply chain**: Increased focus on reproducible builds and software supply chain security

This timing makes sense - remote work highlighted inefficiencies in data distribution and storage.

---

# What Tools Were Used?

**Programming Language:**
- **Go 1.15+**: Modern Go with modules, strong concurrency support

**Key Dependencies:**
- **go-fuse v2**: FUSE filesystem interface for Linux/macOS
- **restic/chunker**: Rabin fingerprinting for content-defined chunking
- **Docker client**: Container integration and image management
- **msgpack**: Binary serialization for efficient data storage

**Development Tools:**
- **Standard Go tooling**: go test, go build, coverage analysis
- **Docker/Skopeo**: Container image manipulation
- **inotify**: Filesystem watching for daemon functionality
- **Markdown/RemarkJS**: Documentation and presentations

**Testing & Quality:**
- Comprehensive test suite with coverage reporting
- Benchmark tests for performance validation
- Error checking with custom goadapt library

---

# What Does the Code Do? (1/2)

**Core Database Operations:**
- **PutBlock**: Store binary data, return SHA-256/SHA-512 address
- **GetBlock**: Retrieve data by cryptographic address
- **PutTree**: Create Merkle tree nodes pointing to blocks or other trees
- **GetTree**: Retrieve and verify tree structures

**Streaming & Chunking:**
- **PutStream**: Break large files into chunks using Rabin fingerprinting
- **Chunking Algorithm**: Content-defined boundaries for optimal deduplication
- **Tree Assembly**: Automatically build Merkle trees from chunked streams

**Container Integration:**
- Store Docker images as content-addressable trees
- Run containers directly from database storage
- Image deduplication across container layers

---

# What Does the Code Do? (2/2)

**Filesystem Interface:**
- **FUSE mounting**: Access database content as regular filesystem
- **Virtual directories**: Browse data by hash algorithms (sha256/, sha512/)
- **Content files**: Read tree content through filesystem API

**Command Line Tools:**
- **pb**: Full-featured CLI for all database operations
- **pitd**: Background daemon for serving requests
- **Streaming commands**: catstream, putstream for large data handling

**Security Features:**
- **Preimage attack prevention**: Salt data with type prefixes ("block\n", "tree\n")
- **Cryptographic verification**: All content automatically verified on read
- **Immutable storage**: Content addresses cannot be forged

---

# How is the Code Organized?

```
promisebase/
├── db/              # Core database engine
│   ├── db.go        # Main database interface
│   ├── file.go      # WORM (Write Once Read Many) file handling
│   ├── tree.go      # Merkle tree implementation
│   ├── chunker.go   # Content-defined chunking
│   └── stream.go    # Large file streaming support
│
├── fuse/            # FUSE filesystem interface
│   └── fuse.go      # Virtual filesystem implementation
│
├── cmd/
│   ├── pb/          # Command-line client
│   └── pitd/        # Background daemon
│
├── server/          # Network daemon functionality
├── client/          # Client library (stub)
└── rfc/            # Design documents and specifications
```

**Design Pattern**: Clean separation between storage engine, interfaces, and applications.

---

# Key Algorithms and Data Structures

**Content-Defined Chunking (Rabin Fingerprinting):**
- Uses rolling hash to find natural chunk boundaries
- Produces consistent chunks regardless of insertion/deletion
- Optimal for deduplication of similar files
- Configurable chunk size limits (512KB - 8MB default)

**Merkle Trees:**
- Each tree node contains hashes of child nodes
- Enables efficient verification of large datasets
- Supports both leaf nodes (blocks) and internal nodes (trees)
- File format: plain text list of child addresses

---

# Disk Storage Format

**Path Structure:**
```
block/sha256/d17/370/d173706e5ab6e45e3f99389002d085dc6ad663d4b8140cd98387708196425ab2
└─┬─┘ └──┬─┘ └┬┘ └┬┘ └────────────────────┬────────────────────┘
class  algo  subdir   full hash for easy debugging
```

**WORM Files (Write Once Read Many):**
- Immutable storage with cryptographic verification
- Automatic hash computation during write
- File headers prevent preimage attacks

---

# Important Files and Their Roles

**Core Database (`db/` package):**
- `db.go`: Main database interface, initialization, high-level operations
- `file.go`: Low-level file I/O, WORM semantics, hash computation  
- `tree.go`: Merkle tree operations, traversal, verification
- `chunker.go`: Content-defined chunking using Rabin fingerprinting
- `path.go`: Address parsing, directory structure management

**User Interfaces:**
- `cmd/pb/pbmain.go`: Full-featured command-line interface (881 lines)
- `fuse/fuse.go`: FUSE filesystem implementation for mounting database
- `server/server.go`: Network daemon with container integration

**Infrastructure:**
- `db/pitbase.go`: Utilities, logging, hash algorithms, object interface
- Various `*_test.go`: Comprehensive test suite with benchmarks
- `rfc/`: Design documents explaining architecture decisions

**Configuration & Build:**
- `Makefile`: Test automation, coverage reporting, profiling
- `covertest.sh`: Advanced test runner with coverage enforcement

---

# Work in Progress & Current Issues

**From TODO.md and code comments:**

**Multi-process Safety**: 
- Current implementation not safe for concurrent access
- Need proper file locking and coordination

**Performance Optimization:**
- Memory usage needs optimization for large trees
- `memtest.sh` shows memory limits around 50-100MB
- Wide trees not fully streamable yet

**Network Layer (In Progress):**
- Client/server architecture partially implemented
- UNIX domain socket protocol designed but not complete
- Distributed peer-to-peer functionality planned

**Container Integration:**
- Docker image handling works but needs refinement
- OCI image format support in development
- Container execution through stored images experimental

---

# Known Bugs & Limitations

**From code analysis and comments:**

**FUSE Interface:**
- Write operations not fully implemented
- Limited to read-only access in current state
- macOS compatibility issues noted

**Error Handling:**
- Some file handle management issues
- Potential resource leaks in container operations
- Network error recovery not implemented

**Storage Format:**
- File format is stable but may need versioning
- Path structure locked in, hard to change later
- Subdirectory depth fixed at database creation

**Testing Gaps:**
- Multi-process access scenarios
- Network layer testing incomplete
- Long-running daemon stability untested

---

# What's Missing: Future Plans

**From ROADMAP.md and RFCs:**

**Distributed Systems:**
- Peer-to-peer networking for content distribution  
- Consensus mechanisms for distributed operation
- Cross-node data synchronization and caching

**Advanced Features:**
- Accounting and payment systems for resource usage
- Digital signatures and access control
- Garbage collection for unused content

**Performance & Scalability:**
- Parallel chunk processing
- Database compaction and optimization
- Better memory management for large datasets

**User Experience:**
- Web interface for database browsing
- Integration with popular development tools
- Better documentation and tutorials

**Enterprise Features:**
- Backup and disaster recovery
- Monitoring and alerting
- Multi-tenancy support

---

# Technical Innovations

**Preimage Attack Prevention:**
The system prepends type information ("block\n", "tree\n") to all stored content before hashing. This prevents attackers from using the database to reverse known hashes of private data.

**Streaming Merkle Trees:**
Unlike traditional Merkle trees, this implementation supports streaming reads of arbitrarily large trees without loading entire structures into memory.

**Container-Native Storage:**
Direct integration with Docker and OCI formats, allowing containers to be stored and executed directly from the content-addressable database.

**FUSE Integration:**
Provides a standard filesystem interface, making the database accessible to any POSIX-compliant application without modification.

**Content-Defined Chunking:**
Uses Rabin fingerprinting to create consistent chunk boundaries, maximizing deduplication even when files are modified.

---

# Architecture Philosophy

**UNIX Philosophy:**
- Small, focused components that do one thing well
- Clean interfaces between database, filesystem, and network layers
- Command-line tools for scripting and automation

**Immutable Infrastructure:**
- Once stored, content never changes
- All modifications create new addresses
- Perfect audit trail and reproducibility

**Content-Centric Design:**
- Data location determined by content, not naming
- Natural deduplication and integrity checking
- Enables efficient distribution and caching

**Layered Architecture:**
- Core storage engine independent of interfaces
- Multiple access methods (CLI, FUSE, network)
- Extensible design for future protocols

This represents a thoughtful exploration of content-addressable storage with practical applications in modern computing infrastructure.

---

class: center, middle

# Questions & Discussion

**PromiseBase demonstrates the power of content-addressable storage**

- Revolutionary approach to data deduplication
- Strong foundation for distributed systems  
- Practical applications in container management
- Open source exploration of cutting-edge concepts

*Built with Go, FUSE, and innovative storage algorithms*
