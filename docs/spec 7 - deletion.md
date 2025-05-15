# 7. Deletion

```mermaid

> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).

## 7.1 Underfilled Buckets

| Condition                | Action                                 |
|--------------------------|----------------------------------------|
| Bucket < load_factor_min | Redistribute or merge with neighbor(s) |

Buckets below threshold are merged or redistributed for efficiency.

## 7.2 Value-List Cleanup

| Task                | Mechanism                  |
|---------------------|---------------------------|
| Remove empty lists  | Background GC process      |
| Log deletions       | Dedicated deletion log     |

GC scans and removes empty value-lists, logging deletions for safe reclamation.

## 7.3 Deletion Log vs. WAL

| Log Type   | Purpose                                      |
|------------|----------------------------------------------|
| WAL        | Captures all transactional ops for recovery  |
| Deletion   | Tracks entries/datasets for background GC    |

Deletion log records removals; GC scans log to reclaim space and clean up orphans.

### Deletion & GC Workflow

```mermaid
flowchart TD
    A[Delete entry or value-list] --> B[Log deletion]
    B --> C[GC scans log]
    C --> D{Still referenced?}
    D -- Yes --> E[Do nothing]
    D -- No --> F[Remove dataset]
    F --> G[Reclaim space]
```

| Step                | Description                                 |
|---------------------|---------------------------------------------|
| Log Deletion        | Every deletion is logged with key, ID, time |
| Orphan Detection    | GC cross-checks log with current metadata   |
| Orphan Reclamation  | Orphaned datasets are deleted, space freed  |
| Full-File Scan      | Periodic scan to catch missed orphans       |

**Concurrency & Consistency:**
- Deletions/promotions are serialized with the global writer lock (HDF5 SWMR)
- Prevents races between insert/promote and GC

**Crash Recovery:**
- WAL and deletion log are replayed on startup; incomplete GC is retried

**Idempotence & Safety:**
- GC is idempotent; missed/partial deletions retried in next cycle
- Full scans ensure eventual cleanup
- WAL replay ensures metadata, deletion log, and disk state are consistent

| Factor      | Consideration                                 |
|------------ |----------------------------------------------|
| Performance | Frequent GC adds overhead but ensures cleanup |
| Safety      | Full scans/idempotence prevent orphans        |
| Degradation | Temporary discrepancies tolerated             |