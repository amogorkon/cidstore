## 7. Deletion

### 7.1 Underfilled Nodes
- **Defined merge/redistribute policy:** Nodes below ⌈order/2⌉ will borrow or merge.

### 7.2 Value-List Cleanup
- **Background GC:** Periodically scans for and removes empty value-list datasets; deletions are logged via the WAL.

### 7.3 Deletion Log vs WAL
The deletion log and the WAL serve distinct but complementary roles in managing internal node deletions and ensuring system consistency:

**1. Roles and Responsibilities**
- **WAL:** Used for transactional changes like node splits, merges, inserts, and updates. Ensures atomic and recoverable tree operations.
- **Deletion Log:** Tracks nodes marked for cleanup, allowing deferred deletion during background processing. It’s not replayed for recovery.

**3. Benefits**
- **Separation of Concerns:** WAL ensures transactional recovery; deletion log handles deferred GC.
- **Efficiency:** Enables batch deletions and avoids WAL bloat.
- **Safety:** Background GC may validate deletion candidates before purging.

### Key Count Tracking for Internal Nodes

**Purpose:**
Key counts validate internal node structure, enforce occupancy limits, and facilitate splits, merges, and rebalancing.

**Approach:**
- **No Persistent Storage:** Key counts are not stored on-disk; they are maintained in an in-memory table to keep HDF5 files compact.
- **In-Memory Table:**
  ```python
  in_memory_table = {
      "/sp/nodes/internal/1234": {"key_count": 50},
      "/sp/nodes/internal/5678": {"key_count": 40},
  }
  ```
- **Lifecycle Management:**
  - **Initialization:** When a node is loaded, compute its key count from the keys dataset and store it in the table.\n  - **Updates:** Increment/decrement counts as keys are added or removed during operations.\n  - **Cleanup:** Remove entries when nodes are unloaded.
- **Overflow Handling:** Use the in-memory key count to decide on splits or merges and support dynamic thresholds if needed.

**Benefits:**
- **Efficiency:** Avoids repeated key count computations.\n  - **Compact Storage:** HDF5 structure remains lightweight.\n  - **Scalability:** Facilitates dynamic per-node threshold adjustments.

###  Underfilled Nodes
- Nodes with fewer than ⌈order/2⌉ keys trigger key redistribution from siblings or merge if redistribution isn’t feasible.

###  Value-List Cleanup
- A background process scans for empty external value-list datasets and reclaims their storage.

###  Deletion Log vs. WAL
- **WAL:** Captures all transactional operations for recovery.\n  - **Deletion Log:** Separately records nodes marked as obsolete, which are later processed by background garbage collection to remove unneeded datasets.
