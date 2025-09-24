# Replacing the Path Type with IPFS–style CIDs in Promisebase

This document discusses the impact, workload, and trade–offs of
replacing the current custom `Path` type with IPFS–style version 1
Content Identifiers (CIDs). In the current implementation, file
location and addressing information is encoded through a structured
`Path` (including fields such as class, algorithm, and hash) that
produces human–readable canonical strings. By switching to IPFS–style
CIDs, Promisebase will use a self–describing, multibase–encoded
identifier that includes the hash algorithm, the multihash digest, and
optionally a multicodec indicator.

## Overview

The existing system combines file system location with content
addressing. In contrast, IPFS v1 CIDs encapsulate all necessary
metadata about a data object in a compact binary–to–text
representation. Replacing our custom `Path` type with CIDs means that
instead of constructing paths from components like class, algorithm,
and hash, we simply compute a CID from the object’s content using
standard multihash and multicodec procedures.

In our proposed design, functions such as `PutBlock`, `PutTree`, and
`GetTree` will compute and return a CID (for example, in CIDv1 format
with multibase encoding) rather than a canonical path string.
Internally, this change simplifies the addressing logic because the
CID is self–describing and does not require additional directory–style
formatting.

## Advantages

1. **Interoperability & Standards Compliance**  
   - CIDs are widely adopted in the IPFS and content–addressable
     storage ecosystems. This provides compatibility with numerous
     tools and libraries that expect standard CID formats.
   - Developers familiar with IPFS will immediately recognize and
     understand the addressing model.

2. **Content Integrity and Self–Description**  
   - A CID embeds both the hash algorithm and the computed multihash.
     This inherently validates the content and provides a record of
     how the identifier was computed.
   - The self–describing nature of CIDs removes ambiguity about which
     algorithm was used, reducing potential errors.

3. **Simplification of Addressing Logic**  
   - Eliminating the need to split a composite path (e.g. separating
     “class”, “algo”, “hash”, and directory components) leads to a
     more streamlined codebase.
   - The uniform CID structure reduces the number of transformations
     needed between internal and external representations.

4. **Future Proofing**  
   - The CID format is designed to be extensible. It can support new
     hash functions and codecs without requiring a redesign of the
     addressing mechanism.
   - Upgrading or integrating with other distributed systems will be
     more straightforward since CIDs are a common standard.

## Disadvantages and Challenges

1. **Backward Compatibility Issues**  
   - The legacy system relies on the structured format of the current
     `Path` type. Replacing it with CIDs requires a migration strategy
     to convert existing stored paths into CID format or a period of
     dual support.
   - External tools and scripts that parse canonical paths may need to
     be updated.
   - But there are no instances of promisebase in the wild yet, so
     this is not a big concern.

2. **Refactoring Workload**  
   - A significant portion of the codebase (including tree management,
     blob handling, and various tests) references the current `Path`
     type. Comprehensive refactoring is necessary to remove
     dependencies on the old structure.
   - Adjustments to logging, debugging output, and documentation will
     be required to reflect the CID–based addressing.

3. **Performance and Overhead Considerations**  
   - Generating a CID involves multihash and multicodec encoding,
     which may introduce slight computational overhead compared to
     concatenating string components.
   - The increased length of a CID compared to the previous canonical
     string might affect storage or presentation in extremely
     performance–sensitive scenarios, but we consider this negligible.

4. **Developer Familiarity and Transition**  
   - Developers accustomed to the detailed breakdown of path
     components may require time to adapt to the opaque, yet
     standardized, CID format.  But there are no other developers yet,
     so this is not a big concern.
   - Training and updated documentation will be needed to mitigate the
     learning curve associated with IPFS–style addressing.

## Considerations for Integration

- **Migration Strategy:**  

Decide whether to fully deprecate the custom `Path` type in favor of
CIDs or support both formats during a transitional period. Tools for
converting legacy canonical paths into CIDs will ease the migration.

- We fully deprecate the custom Path type, since there are no instances of
  promisebase in the wild yet.

- **Testing and Documentation:**  

Extensive testing must be carried out to ensure that functions now
returning CIDs perform as expected. Documentation should clearly
describe the new addressing scheme and provide examples of working
with CID–based identifiers.

- **Impact on External Systems:**  

Any external processes that depend on the current canonical path
format would need to be updated to handle CIDs.   But promisebase is
not in the wild yet, so this is not a big concern. 

## Conclusion

Switching from a custom `Path` type to IPFS–style CIDs brings
substantial benefits in standardization, content verification, and
interoperability with other distributed systems. Despite challenges
such as migration complexity and potential performance overhead, the
streamlined addressing and future–proof architecture make CIDs an
appealing choice for Promisebase. With careful planning, thorough
testing, and updated documentation, Promisebase can successfully
transition to a CID–based content addressing scheme, simplifying both
internal code and user interactions.

