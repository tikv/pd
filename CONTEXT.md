# PD GC domain

This glossary records the GC terms used by PD. It distinguishes snapshot-read safety, data collection, and the scopes that share those boundaries.

## Language

The following terms describe GC state and its scope.

**GC state**: The transaction safe point, GC safe point, and GC barriers associated with a GC management scope. A view of GC state can omit barriers.
_Avoid_: Safe point when referring to the entire state.

**GC scope**: A unit of independent GC management, either one keyspace using keyspace-level GC or the shared scope for unified GC.
_Avoid_: Keyspace when referring to a scope shared by multiple keyspaces.

**Enabled keyspace**: A keyspace whose lifecycle state is ENABLED. It can use either keyspace-level or unified GC, so an enabled keyspace does not necessarily have an independent GC state.
_Avoid_: Independent GC scope when referring to every enabled keyspace.

**Transaction safe point**: The timestamp at or after which snapshots are safe to read. The GC safe point cannot exceed the transaction safe point.
_Avoid_: GC safe point when referring to the read boundary.

**GC safe point**: The timestamp before which snapshots can be discarded by garbage collection.
_Avoid_: Transaction safe point when referring to the collection boundary.

**GC barrier**: A protection that constrains advancement of the transaction safe point beyond its barrier timestamp while valid. A global GC barrier applies across GC scopes.
_Avoid_: GC safe point when referring to protection registered by a component.

**Keyspace-level GC**: GC managed independently for one keyspace, with its own GC state.
_Avoid_: Unified GC when referring to independent per-keyspace management.

**Unified GC**: GC managed collectively for keyspaces that do not use keyspace-level GC. These keyspaces share the state managed by the null keyspace.
_Avoid_: Global GC barrier when referring to shared GC management.

**Null keyspace**: The scope used when no named keyspace is selected and the scope that manages unified GC.
_Avoid_: Default keyspace; the default keyspace is distinct.
