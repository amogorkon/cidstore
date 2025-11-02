# CIDStore

CIDStore is a high-performance, disk-backed B+Tree implementation designed as a backend for triplestores. It efficiently maps 256-bit composite keys (full SHA-256 CIDs) to variable-sized sets of 256-bit values, supporting massive-scale many-to-many relationships with high throughput and low latency.

## Requirements

- **Python 3.13+** (required - no backward compatibility)
- **Free-threading build recommended** for parallel operations (`--disable-gil`)
- **JIT enabled** for optimal performance (`-X jit=1`)

### Quick Start with Optimizations

**Option 1: Enable for your session (PowerShell):**
```powershell
. .\scripts\enable_optimizations.ps1
```

**Option 2: Enable for your session (Bash/Zsh):**
```bash
source ./scripts/enable_optimizations.sh
```

**Option 3: Make permanent (add to your profile):**
```bash
# Add to ~/.bashrc, ~/.zshrc, or PowerShell profile:
export PYTHON_GIL=0
export PYTHON_JIT=1
```

**Option 4: Use the optimized wrapper:**
```bash
python cidstore_optimized.py
```

### CI: Free-threaded build & test

We provide a GitHub Actions workflow that builds CPython (Ubuntu) with optional `--disable-gil` and runs
the test-suite with `-X gil=0 -X jit=1`.

To run locally (Linux/WSL):

```bash
./scripts/ci_build_python.sh 3.13.7 /opt/python3.13-ff
export PATH=/opt/python3.13-ff/bin:$PATH
python -X gil=0 -X jit=1 -m pytest tests/ -q
```

The workflow is split into a build job (produces an artifact) and a test job that downloads the built
interpreter, so repeated CI runs are faster.

## Features

- **Optimized for Triplestores**: Handles relationships like `(A, loves, B)` and `(A, loves, C)` using composite keys.
- **Multi-Value Key Support**: Efficiently stores and queries keys with multiple associated values.
- **HDF5 Integration**: Uses HDF5 for scalable, compressed, and chunked storage.
- **Crash Consistency**: Write-Ahead Logging (WAL) ensures atomicity and recoverability.
- **True Parallelism**: Python 3.13 free-threading enables GIL-free concurrent operations.
- **JIT Optimization**: Python 3.13 JIT compiler accelerates hot code paths.
- **Dynamic Scalability**: Handles billions of keys and values with efficient sharding and chunking.

## Key Characteristics

- **Keys**: Immutable 256-bit Content Identifiers (CIDs) from full SHA-256 hashes, stored as 4×64-bit components for maximum cryptographic strength.
- **Values**: Stored in contiguous, compressed datasets for efficient retrieval.
- **Performance** (with Python 3.13 optimizations):
  - Insert throughput: >1M ops/sec (>3M with free-threading).
  - Lookup latency: <50µs (avg), <100µs (P99), <30µs with JIT.
  - Parallel maintenance: 2-4x faster with free-threading.
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

## Development setup

This repository contains convenience scripts to create a development virtual environment,
install the package in editable mode, and pin a resolved development dependency list.

PowerShell (Windows):

```powershell
.\scripts\setup_dev.ps1
```

POSIX (Linux/macOS):

```bash
./scripts/setup_dev.sh
```

Both scripts will create a `.venv` folder (if missing), install the package in editable
mode, install `pytest` (and minimal dev deps), and write `requirements-dev.txt` with the
resolved package versions. After running them you can run `pytest` from the repo root.

## Interface Stability with ZVIC

CIDStore uses **ZVIC (Zero-Version Interface Contracts)** to ensure interface stability without relying on semantic versioning. ZVIC:

- Validates contracts dynamically at runtime through signature hashing
- Catches incompatible changes immediately (fail-fast)
- Has **zero performance impact in production** (uses assert blocks removed with `python -O`)
- Provides detailed compatibility diagnostics

Key interfaces protected by ZVIC:
- Plugin system (`PluginRegistry`, `SpecializedDataStructure`)
- Storage backend (`Storage` class)
- Write-Ahead Log (`WAL` class)
- Main store interface (`CIDStore` class)
- Key utilities (`E` class, composite key functions)

For details, see [ZVIC Integration Documentation](docs/zvic_integration.md).

To run compatibility tests:

```bash
pytest tests/test_zvic_compatibility.py -v
```

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