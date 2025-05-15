
## 5. Multi-Value Keys


```mermaid

> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).

---

### ECC-Protected State Mask & Slot Layout

---


#### Layout Summary

| Mode   | state_mask | Slot Usage (per 128-bit slot)                | Notes                                  |
|--------|------------|----------------------------------------------|----------------------------------------|
| Inline | ≠ 0        | D₁…D₄ bits: valid CIDs in slots[0..3]        | No checksums; integrity via WAL replay |
| Spill  | 0          | slots[0].low: shard_id (16b), high: unused   | All slots repurposed                   |
|        |            | slots[1].low: bucket_local_id (32b), high: unused |
|        |            | slots[2].low: segment_id (16b), high: unused |
|        |            | slots[3]: bucket checksum (128 bit)          |

---


#### Promotion Algorithm

```mermaid
flowchart TD
    A[Inline Insert] --> B{Free slot?}
    B -- Yes --> C[Pack CID, set bit in state_mask]
    B -- No --> D[Promote: migrate all CIDs to external value-list]
    D --> E[Zero all 4 slots, set state_mask = 0 (spill)]
    E --> F[Encode spill pointers, update checksum]
```

---

#### ECC Encoding, Bit Layout, and Error Handling

The state mask is ECC-protected using an 8-bit Extended Hamming (SECDED) code:

- **Inline**: 4-bit mask (which slots are valid), ECC-encoded to 8 bits (SECDED)
- **Spill**: All slots repurposed for pointers/metadata

| Bit | Role         |
|-----|-------------|
| 1   | P₁ (parity) |
| 2   | P₂ (parity) |
| 3   | D₁ (data)   |
| 4   | P₃ (parity) |
| 5   | D₂ (data)   |
| 6   | D₃ (data)   |
| 7   | D₄ (data)   |
| 8   | P₀ (overall parity) |

**Encoding:**

| Parity | Equation |
|--------|----------|
| P₁     | D₁ ⊕ D₂ ⊕ D₄ |
| P₂     | D₁ ⊕ D₃ ⊕ D₄ |
| P₃     | D₂ ⊕ D₃ ⊕ D₄ |
| P₀     | (P₁ ⊕ P₂ ⊕ D₁ ⊕ P₃ ⊕ D₂ ⊕ D₃ ⊕ D₄) |

**Error Handling:**
- Single-bit errors: corrected by ECC
- Double-bit errors: detected, bucket flagged as corrupted

**SIMD/Data Layout:**
- 8-bit state mask, aligned for SIMD
- Multiple masks checked/corrected in parallel



---

#### Summary Table

| Feature         | Inline Mode         | Spill Mode                |
|-----------------|--------------------|---------------------------|
| state_mask      | ECC, ≠0            | ECC, 0                    |
| Slot usage      | CIDs               | Pointers/metadata         |
| Integrity       | WAL+ECC            | Per-bucket checksum       |
| SIMD-friendly   | Yes                | Yes                       |
| Overhead        | Minimal (8 bits)   | Minimal                   |

Refer to the layout summary table above for all slot mapping and mode details. Only one promotion diagram is retained for clarity.

<!-- Redundant code, prose, and repeated diagrams removed for clarity. See summary table and diagram above for slot usage, spill mode, and promotion workflow. -->

#### Background
The state mask in each bucket determines whether its inline slots are interpreted as direct data or as spill metadata. A single bitflip could misclassify the bucket, risking data loss or incorrect spill processing.

#### ECC Design Overview

The 4-bit raw state mask is encoded into an 8-bit codeword using an Extended Hamming (SECDED) scheme:

- **4 bits**: State value (which slots are active in inline mode)
- **4 bits**: ECC (3 parity + 1 overall parity)

**Bit Layout Example:**

| Bit Position | Role         |
|--------------|-------------|
| 1            | P₁ (parity) |
| 2            | P₂ (parity) |
| 3            | D₁ (data)   |
| 4            | P₃ (parity) |
| 5            | D₂ (data)   |
| 6            | D₃ (data)   |
| 7            | D₄ (data)   |
| 8            | P₀ (overall parity) |

**Encoding:**
Given a 4-bit state mask (D₁, D₂, D₃, D₄):

- P₁ = D₁ ⊕ D₂ ⊕ D₄
- P₂ = D₁ ⊕ D₃ ⊕ D₄
- P₃ = D₂ ⊕ D₃ ⊕ D₄
- P₀ = (P₁ ⊕ P₂ ⊕ D₁ ⊕ P₃ ⊕ D₂ ⊕ D₃ ⊕ D₄) (even parity)

The resulting 8-bit codeword is stored as the state mask field in the bucket.

**Error Detection & Correction:**
- On read, ECC decoding recomputes the syndrome using the parity equations.
- If the syndrome is zero and overall parity passes, the codeword is accepted.
- A nonzero syndrome identifies a single-bit error for correction.
- Double-bit errors are detected by a failed overall parity check, flagging the bucket as corrupted.

#### SIMD and Data Layout Considerations
- The 8-bit state mask is stored in bucket metadata, aligned on a 64-bit (or wider) boundary.
- Multiple state masks can be loaded in a single SIMD operation for parallel ECC verification/correction.
- SIMD instructions efficiently compute parity and syndrome vectors for many buckets at once.

**Minimal Overhead, Maximum Robustness:**
- Only 4 extra bits per bucket (vs. raw 4-bit mask)
- Protects against catastrophic misinterpretation of inline/spill mode



---

In this hash directory design, multi-value key support is directly integrated into the bucket storage:



### 5.1 Inline Duplicate Entries

Each bucket may contain several entries with the same key, each storing a value (CID). Duplicates remain inline until a threshold is exceeded. Insertion appends a new (key, value) entry; queries scan the bucket for all values; deletions remove the specific (key, value) entry.



### 5.2 External Value Lists

For high-cardinality keys, an external HDF5 dataset (value list) is used. The bucket entry is updated to point to the external dataset. Queries retrieve the dataset in one read; deletions remove values and clean up empty datasets.



### 5.4 Value‑List Compaction

External value‑lists are append‑only with tombstoning. Compaction is a background process that removes tombstoned entries, defragments lists, and reclaims space. Triggered by configurable thresholds, compaction is atomic and concurrent-safe, with metrics used to tune scheduling.



### 5.3 Hybrid & Promotion

Inline duplicates are used for low-cardinality keys; when a threshold is exceeded, promotion to an external value-list occurs (WAL-logged, atomic). Demotion is handled in maintenance to avoid thrashing. This hybrid approach reduces bucket bloat and ensures efficient memory use.


## 5.7 WAL‑Driven Adaptive Maintenance

See [Spec 4: Write-Ahead Log (WAL)](spec%204%20-%20WAL.md#45-wal-driven-adaptive-maintenance) for all details on danger score, merge scheduling, protections, and journaling.