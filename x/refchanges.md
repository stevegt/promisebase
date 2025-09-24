# Tracking Ref Changes in Promisebase

When ref objects become first–class citizens in Promisebase (where
refs point to commits or trees that evolve over time), it is desirable
to record and track how these refs change. Below are several
approaches to track ref changes along with their advantages and
disadvantages.

---

## 1. Append–Only Ref Logs

**Description:**  
Each ref (for example, a branch or tag) is accompanied by an
append–only log file (similar to Git’s reflog). Every time a ref is
updated, a new log entry is appended recording the old CID, new CID,
timestamp, and possibly additional metadata (such as the operation
type and user information).

**Pros:**  
- **Simple and Understandable:** A sequential log is easy to implement
  and debug.  
- **Time–Stamped History:** Easily yields a chronological history of
  changes, which is beneficial for auditing and rollback operations.  
- **Low Overhead:** Appending text-based log entries is relatively
  inexpensive.

**Cons:**  
- **Data Duplication:** The same CID may appear multiple times across
  different log entries.  
- **Log Maintenance:** Over a long history, the log file may grow
  large; trimming or pruning strategies are needed.  
- **Loose Coupling:** The log exists as a side–channel to the main ref
  pointer; synchronizing it with ref updates requires careful
  transactional control.

---

## 2. Hypergraph–Based Commit Model for Refs

**Description:**  
In this design, ref updates are recorded by creating small commit–like
objects (or “ref commits”) that form a hypergraph. Each ref commit
contains the new state (the updated CID) along with a pointer to the
previous ref commit. This mirrors the commit graph in Git but is
applied directly to ref objects as first–class entities.

**Pros:**  
- **Integrated with Existing Model:** It leverages the
  hypergraph–based (Merkle tree) structure already used for commits
  and trees in Promisebase.  
- **Immutable History:** Each ref update is an immutable commit node,
  improving auditability and verifiability.  
- **Robust Merging:** If multiple users update the same ref
  concurrently, a directed acyclic graph (DAG) can represent divergent
  histories.

**Cons:**  
- **Complexity:** Handling a DAG per ref is conceptually more complex
  than a simple log.  
- **Query Overhead:** Traversing the graph to list history may require
  additional indexing or caching for performance.  
- **Implementation Overhaul:** Existing code might need significant
  modifications to generate, store, and retrieve these “ref commits.”

---

## 3. Embedded Timestamped Metadata in Ref Objects

**Description:**  
In this approach, the ref object itself is extended to be a record (or
a mini document) that includes a history section. Each update appends
a new record internally (for example, as an array of change records)
within the same stored object.

**Pros:**  
- **Self–Contained Ref:** The complete history is embedded within the
  single ref object.  
- **Easy Retrieval:** Accessing the ref returns both its current value
  and its complete changelog.  
- **Atomic Updates:** Changes made in one operation ensure history and
  current ref are updated together.

**Cons:**  
- **Potential Growth:** A heavily updated ref will lead to large
  history values held in a single object.  
- **Immutable Concerns:** Since underlying objects are immutable in
  Promisebase, updating a ref means writing a new object that combines
  previous history with new changes, which could complicate caching.  
- **Lack of Flexibility:** Merging histories across forks or parallel
  updates could be more challenging.

---

## 4. External Event or Notification System

**Description:**  
Another option is to decouple ref–change tracking from the ref object
itself. Every ref update emits an event that is recorded in a
dedicated event store or publish/subscribe system. This external log
could use a standard format (like JSON lines) and later be queried to
reconstruct the ref history.

**Pros:**  
- **Decoupled Architecture:** The ref’s primary role remains simple,
  while the external system handles history tracking.  
- **Scalability:** Event–stream systems are designed to handle high
  write rates and allow real–time monitoring.  
- **Flexible Querying:** The event store can support complex queries
  (e.g., filtering by user or time range).

**Cons:**  
- **External Dependency:** Requires maintaining and synchronizing an
  additional component.
- **Eventual Consistency:** There may be delays or occasional
  mismatches between the ref’s state and the event log.  
- **Complexity in Recovery:** Reconstructing history solely from
  events might require more sophisticated recovery mechanisms.

---

## Summary and Recommendations

Each approach offers a trade–off between simplicity, integration, and flexibility:

- For applications prioritizing straightforward implementation and
  auditability, **append–only ref logs** provide a practical solution.
- If the goal is to closely integrate ref history with the existing
  commit structure while ensuring immutability and robust merger
  semantics, the **hypergraph–based ref commit model** is an
  attractive, though more complex, option.
- Embedding history into the ref object itself offers atomic updates
  and ease of retrieval but can complicate storage and merge
  operations.
- An **external event system** separates concerns and scales well but
  adds an extra component to manage.

Given Promisebase’s focus on immutability and a hypergraph–based data
model, a model that mirrors commit–tracking (Approach 2) may provide
the best long–term benefits, while a transitional append–only log
(Approach 1) could serve as a simpler initial solution.

Implementers should weigh these approaches against their performance
and complexity budgets, the anticipated volume of ref updates, and the
desired user experience when interacting with mutable references.

