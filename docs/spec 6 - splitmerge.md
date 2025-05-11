## 6. Split/Merge Operations

### 6.1 Atomicity Guarantees
- **Ensuring atomic WAL entries for splits/merges.**
- **Pointer updates use shadow paging for atomic application.**

### 6.2 Validation & Integrity
- **Node Validation Routine:**
  ```python
  def validate_node(path: str) -> bool:
       """
       Checks:
       - Keys are strictly in order.
       - The number of keys is within [min, max] thresholds.
       - Parent, child pointers are valid and consistent.
       - Checksums match.
       """
  ```
- **Checksum Choice:** CRC32C for fast per-chunk integrity.

### 12.1 Atomicity Guarantees
- Splits and merges are performed using copy-on-write and logged in the WAL to allow complete rollback if necessary.

### 12.2 Validation & Integrity
- Post-operation, the `validate_node` function confirms key order, occupancy, and pointer consistency.
