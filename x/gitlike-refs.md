# Implementing Gitlike Refs in Promisebase

A pure–hash–keyed store forces users to work directly with long,
opaque content identifiers. In the current system, these identifiers
were generated using a custom `Path` type; however, with the planned
transition to IPFS–style v1 CIDs, every stored object is now
represented by a self–describing content identifier (CID). While CIDs
ensure integrity and interoperability, they are not inherently
human–friendly. To improve usability, Promisebase can implement
Gitlike refs that allow users to refer to objects with memorable
names.

---

## Overview

Replacing the custom `Path` type with standard CIDs means that data
objects (blocks, trees, streams) are identified by a CID rather than a
composite path string. These CIDs offer strong content–based
addressing along with multicodec and multihash metadata. However,
expecting users to manually reference CIDs can be cumbersome. A
Gitlike ref system provides mutable, friendly pointers (e.g.
refs/heads/main or refs/tags/v1.0) that map human–readable names to
immutable CIDs.

---

## Design Considerations

### 1. Storage Location and Format

**Where to store refs?**  

A straightforward solution is to reserve a top–level directory (e.g.
"refs") within the Promisebase database directory. Each ref is stored
as a file whose pathname mirrors its hierarchical name. For instance:
  
  - refs/heads/main  
  - refs/tags/v1.0  

**File Content:**  

The content of a ref file will simply be the CID (e.g. a CIDv1 string
such as "bafkreigh2akiscaildc3l7izv4z7kk4o67h3svwtb77sqqwrsg373t25pu") that
points to the desired object. This approach is analogous to Git’s use
of plain–text ref files and even allows the possibility of
implementing refs as symlinks that directly point to the canonical
storage location.

### 2. Operations on Refs

The following operations should be supported:

- **Create a Ref:**  
  When a user creates a new ref, Promisebase writes the CID (the
  IPFS–style content identifier) to a new file under the refs/
  directory.

- **Update a Ref:**  
  Updating a ref involves overwriting the file content (or reseating
  the symlink) with a new CID. This operation allows branches or tags
  to be moved without altering the immutable underlying data.

- **Delete a Ref:**  
  Removing a ref file simply deletes the pointer, similar to removing
  a ref in Git.

- **Read a Ref:**  
  Reading a ref returns the CID that the ref currently maps to, which
  can be used to fetch or verify the corresponding object.

### 3. Leveraging Existing Features

Promisebase already uses mechanisms for linking streams via symlinks
and assembling canonical paths from content. With the migration to
CIDs, these existing methods can be repurposed or adapted so that refs
simply point to a CID rather than a composite path. This approach
simplifies the resolution process—once a ref is read, its CID can be
directly used with existing functions such as `GetBlock` or `GetTree`.

### 4. Filesystem and FUSE Integration

For environments where Promisebase is exposed via a FUSE filesystem, a
dedicated "refs" directory can be added. Users would then be able to
navigate into `/mnt/promisebase/refs/`, where each file (or symlink)
represents a ref and contains the corresponding CID. This makes it
easy to create, update, or delete refs with standard file system
tools.

---

## Implementation Outline

1. **Create a Refs Module:**  

Develop a new module (or subpackage) within Promisebase that
encapsulates ref handling. This module will provide a clear API for
setting, getting, deleting, and listing refs.

2. **Define the Refs Interface:**  

 Implement utility functions such as:

   - `SetRef(name string, cid string) error` – Writes the CID (or sets
     the symlink target) for the given ref.
   - `GetRef(name string) (string, error)` – Reads and returns the CID
     for the given ref.
   - `DeleteRef(name string) error` – Removes the ref file.
   - `ListRefs(prefix string) ([]string, error)` – Lists all refs under a given namespace (for example, "refs/heads/").

3. **File/Directory Layout:**  
   Within the database directory, create a new subdirectory called "refs". Ensure that all file operations (creation, updates, deletion) are performed safely (e.g. using atomic file replacement techniques).

4. **Integrate with High–Level Logic:**  
   While high–level object creation (via PutBlock, PutTree, etc.) remains unchanged, command–line interfaces and FUSE layers can be extended to allow users to manipulate refs conveniently.

5. **FUSE Extension (Optional):**  
   Extend the FUSE mount to expose the "refs" directory, allowing interactive exploration and manipulation of refs through the normal file system.

6. **Testing and Documentation:**  
   Write unit tests to verify that all ref operations work correctly. Update documentation to explain the new mutable ref layer and how it relates to the underlying immutable CID–based addressing.

---

## Trade–Offs and Future Work

- **Flexibility vs. Complexity:**  
  Adding a refs layer improves user friendliness and flexibility but introduces an extra layer of indirection. It is essential to ensure that ref updates remain consistent with the underlying immutable objects.

- **Backward Compatibility:**  
  Systems previously relying solely on direct CID references may require additional support or migration utilities to take advantage of Gitlike refs.

- **Interfacing with External Tools:**  
  A robust refs system will facilitate integration with other version–control or distributed storage systems that utilize Git–like workflows.

---

## Conclusion

Introducing Gitlike refs in Promisebase allows users to manage content via human–friendly names rather than the raw, self–describing CIDs now used throughout the system. By storing refs as files (or symlinks) under a dedicated "refs" directory and providing standard operations for creating, updating, reading, and deleting these refs, Promisebase will become more accessible and easier to integrate into everyday workflows. This ref layer enhances usability without compromising the underlying benefits of content–addressable, immutable storage based on IPFS–style CIDs.

