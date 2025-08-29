# CIDStore

CIDStore is a high-performance, disk-backed B+Tree implementation designed as a backend for triplestores. It efficiently maps 128-bit composite keys to variable-sized sets of 128-bit values, supporting massive-scale many-to-many relationships with high throughput and low latency.

## Features

- **Optimized for Triplestores**: Handles relationships like `(A, loves, B)` and `(A, loves, C)` using composite keys.
- **Multi-Value Key Support**: Efficiently stores and queries keys with multiple associated values.
- **HDF5 Integration**: Uses HDF5 for scalable, compressed, and chunked storage.
- **Crash Consistency**: Write-Ahead Logging (WAL) ensures atomicity and recoverability.
- **Concurrency**: Supports single-writer, multiple-reader (SWMR) mode for concurrent access.
- **Dynamic Scalability**: Handles billions of keys and values with efficient sharding and chunking.

## Key Characteristics

- **Keys**: Immutable 128-bit identifiers derived from SHA3 hashes or composite triplestore logic.
- **Values**: Stored in contiguous, compressed datasets for efficient retrieval.
- **Performance**:
  - Insert throughput: >1M ops/sec.
  - Lookup latency: <50µs (avg), <100µs (P99).
- **Hybrid Multi-Value Handling**: Combines duplicate key storage with external value-list datasets for high-cardinality keys.

## Use Cases

CIDStore is ideal for triplestore backends requiring:

- Massive-scale many-to-many relationships.
- High insert throughput and low-latency lookups.
- Crash consistency with minimal recovery time.

## Architecture Overview

CIDStore uses a B+Tree structure with the following components:

- **Leaf Nodes**: Store semantic keys and values, linked for in-order traversal.
- **Internal Nodes**: Guide traversal using synthetic routing keys.
- **Write-Ahead Log (WAL)**: Ensures atomic updates and crash recovery.
- **HDF5 Storage**: Provides scalable, chunked, and compressed storage for nodes and value datasets.

## Multi-Value Key Handling

CIDStore supports two approaches for multi-value keys:

1. **Duplicate Keys**: Stores multiple entries with the same key directly in the B+Tree.
2. **Value Lists**: Promotes high-cardinality keys to external HDF5 datasets for efficient bulk operations.

A hybrid approach dynamically switches between these strategies based on key cardinality.

## Concurrency and Crash Recovery

- **Concurrency**: Single-writer, multiple-reader (SWMR) mode ensures safe concurrent access.
- **Crash Recovery**: WAL and shadow paging provide atomicity and consistency.

## Performance Targets

| Metric            | Target                  |
|-------------------|-------------------------|
| Insert Throughput | >1M ops/sec (batched)   |
| Lookup Latency    | <50µs (avg), <100µs (P99) |
| Recovery Time     | <30s after crash        |
| Scalability       | 1B keys, 100B values    |

## Getting Started

### Prerequisites

- Python 3.12 or later.
- HDF5 library for storage.

### Installation

Clone the repository and install the required dependencies:

```bash
# Clone the repository
git clone https://github.com/your-repo/cidstore.git
cd cidstore

# Install dependencies
pip install -r requirements.txt
```

### Usage

Refer to the documentation in the `docs/` folder for detailed usage instructions and examples.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Versioning Scheme

We use a CalVer versioning scheme: `YYYY.0W[.patchN/devN/rcN]`, where:
- `YYYY` is the year
- `0W` is the zero-padded ISO week number
- Optional `.patchN`, `.devN`, `.rcN` for patches, dev, or release candidates. Normally it's just the number of releases in that week.

For example, `2025.26` corresponds to week 26 of 2025. This mirrors the structure of our Scrum logs (see `/docs/scrum/README.md`).

# Office Hours
You can also contact me one-on-one! Check my [office hours](https://calendly.com/amogorkon/officehours) to set up a meeting :-)

If you have questions also feel free to use the github [Issues](https://github.com/amogorkon/cidstore/issues) or the [Discussions](https://github.com/amogorkon/cidstore/discussions).