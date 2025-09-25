# PromiseGrid Message Format Specification

## Overview

PromiseGrid implements a command-sourced hypergraph architecture for academic research coordination, using structured messages that serve as cryptographic promises about computational work. Each message represents an executable command with verifiable inputs and outputs, enabling reproducible research workflows across distributed academic networks.

## Message Structure

The PromiseGrid message format uses a 5-element CBOR array that provides self-identification, routing, isolation, content, and integrity verification:

```cbor
[
  "grid",                    // Self-identifying protocol tag
  protocol_cid,              // Protocol handler routing
  grid_cid,                  // Grid instance isolation
  cwt_payload,               // Research coordination content
  signature                  // Self-describing cryptographic attestation
]
```

## Element Descriptions

### 1. Protocol Tag ("grid")
A string identifier that immediately marks the message as belonging to the PromiseGrid protocol family. This follows established patterns like Bitcoin's magic bytes and CBOR file magic, enabling quick protocol identification without deep parsing.

### 2. Protocol_CID
A content-addressable identifier referencing the specific protocol specification document. This enables:
- Immediate routing to appropriate protocol handlers at the kernel level
- Protocol evolution through content-addressable specifications
- Handler subscription management based on protocol capabilities

Example: `"bafybeiresearch_protocol_v1"`

### 3. Grid_CID  
A content-addressable identifier specifying which PromiseGrid instance the message belongs to, enabling:
- Multi-tenant isolation between different research networks
- Grid-specific routing and security policies
- Academic institutional boundaries

Example: `"bafybeiuniversity_network"`

### 4. CWT Payload
A CBOR Web Token containing both standard claims and PromiseGrid-specific research coordination data:

**Standard CWT Claims:**
- `1` (iss): Issuer identity using DID Web format
- `6` (iat): Issued at timestamp

**PromiseGrid Custom Claims (negative integers < -65536):**
- `-65537`: Parent event CIDs (hypergraph causality)
- `-65538`: Executable CID (content-addressable code reference)
- `-65539`: Execution arguments (method parameters)
- `-65540`: Input data (computational inputs)
- `-65541`: Output CIDs (result references, for attestation)

### 5. Self-Describing Signature
A signature object containing algorithm identification and signature data:
```cbor
{
  "alg": "Ed25519",
  "sig": "base64url_signature_data"
}
```

## Complete Message Examples

### Research Data Processing
```cbor
[
  "grid",
  "bafybeiresearch_protocol_v1",
  "bafybeiuniversity_grid", 
  {
    1: "did:web:alice.physics-lab.edu",
    6: 1640995200,
    -65537: ["bafybeidata_collection"],
    -65538: "bafybeistatistical_processor", 
    -65539: {
      "method": "correlation",
      "confidence": 0.95,
      "dataset": "bafybeiraw_measurements"
    },
    -65540: {
      "bafybeiraw_measurements": [
        [1.2, 3.4, 2.1],
        [2.8, 1.9, 3.7], 
        [1.5, 4.2, 2.9]
      ]
    },
    -65541: {
      "results": "bafybeianalysis_results"
    }
  },
  {
    "alg": "Ed25519",
    "sig": "A7xQc9fX...signature_data"
  }
]
```

### Accounting Workflow
```cbor
[
  "grid",
  "bafybeiinvoice_protocol_v1",
  "bafybeiaccounting_grid",
  {
    1: "did:web:vendor.supplies.com",
    6: 1641081300,
    -65537: ["bafybeipo_created"],
    -65538: "bafybeiinvoice_processor",
    -65539: {
      "invoice_number": "INV-2024-001", 
      "po_reference": "PO-2024-089"
    },
    -65540: {
      "bafybeiinvoice_data": {
        "amount": 15420.50,
        "items": [{"desc": "Laboratory Equipment", "qty": 2}]
      }
    }
  },
  {
    "alg": "Ed25519",
    "sig": "B8yRd0gY...signature_data"
  }
]
```

## Architectural Benefits

**Multi-Layer Routing**: The kernel routes by protocol_cid to appropriate handlers, which then manage protocol-specific parent_cid subscriptions for event chain coordination.

**Command Sourcing**: Messages capture computational intent rather than just results, enabling system reconstruction through command replay while preserving the "why" behind changes.

**Academic Integrity**: Each message serves as a cryptographic promise: "I executed this code with these inputs and obtained these outputs," enabling verifiable research coordination.

**Content Addressability**: All references use CIDs, ensuring data integrity and enabling efficient deduplication and verification across the research hypergraph.

## Comparison to Email

PromiseGrid messages provide structured computational coordination similar to how email revolutionized human communication. While agents currently must parse unstructured email into internal formats and generate outbound messages, PromiseGrid enables direct structured communication with native execution semantics, routing, and verification capabilities.

This eliminates the structured↔unstructured translation overhead that burdens current agent systems while providing email's store-and-forward benefits enhanced with cryptographic integrity and academic accountability.
