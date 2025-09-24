# Command Sourcing for Tracking Ref and Other Changes

Command sourcing is a technique that records a sequence of
commands that modify the state of a system. Rather than only storing
the final state or resulting events, this approach logs every command
that causes changes. In Promisebase, command sourcing can be used to
track ref updates as well as other state modifications.

## Approaches to Command Sourcing

- Append-only log of commands:  
  Each ref update is recorded as a new entry appended to a log. The
  log contains details such as the previous CID, the new CID, a
  timestamp, and metadata like user information. This gives an
  immutable timeline of ref changes.

- Transaction log with reversible commands:  
  Commands are stored with enough data to reverse their effects if
  necessary. With this method, it is possible to rollback to a prior
  state when a ref update causes an error or when concurrent changes
  need resolution.

- Integration with event sourcing:  
  Command sourcing may be combined with an event‐based model. In such a
  design, commands are transformed into events that are stored in an
  event log. The full history of operations may then be replayed to
  reconstruct any state.

## Pros and Cons

- Pros:
  - Provides a clear audit trail for all operations.
  - Enables replay to regenerate the current state.
  - Enhances transparency for ref updates and state changes.
  - Facilitates debugging and retrospective analysis.

- Cons:
  - May introduce performance overhead from frequent logging.
  - Increases complexity in managing command consistency.
  - Requires careful design to avoid duplicate state data.
  - Could lead to larger storage needs for extensive logs.

## Additional Use Cases

Command sourcing is not limited to tracking ref changes. It can also be
applied to:

- Audit all system state changes and user activities.
- Debug issues by examining the exact order of executed commands.
- Rebuild historical states by replaying the command log.
- Support eventual consistency in distributed or concurrent systems.

## Conclusion

Implementing command sourcing in Promisebase offers a robust method to
track changes, including ref updates and other modifications. While this
approach introduces some overhead and design challenges, its benefits in
auditability, reproducibility, and transparency can significantly
increase system reliability and accountability.
