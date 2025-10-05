# Agent command policy
If you are an automated assistant or CI tool reading this file, respect the instruction below.


## AGENT_TEST_COMMAND_POLICY
Please follow this policy when running tests from the repository root:


Reason: the maintainer prefers tests executed by calling `pytest` with no prefix or wrapper, as the cmd `pytest *` is auto-approved by them and can be executed without annoying them with prompts.

## Running python scripts

We are on a windows machine. use `py` to run python scripts, not `python` or `python3`.
```markdown
# Agent guidance for automated coding agents

This project contains an explicit test & runtime policy and a few repository-specific conventions that help automated agents act safely and productively.

## Test runner policy (authoritative)
- Always run tests from the repository root with the `pytest` command (e.g. `pytest` or `pytest tests/test_directory.py`). Do NOT run tests via `python -m pytest`, `py -3 -m pytest`, `.venv\Scripts\python -m pytest`, or other wrappers. If a different invocation is absolutely required, ask the maintainer.

## Platform / script notes
- Development helper scripts are provided in `scripts/`:
	- Windows PowerShell: `scripts/setup_dev.ps1` (runs in PowerShell; creates `.venv` and installs dev deps).
	- POSIX: `scripts/setup_dev.sh`.
- When running standalone Python scripts on Windows prefer the `py` launcher (e.g. `py script.py`), but still use `pytest` to run tests.

## Project big-picture (quick)
- CIDStore is a disk-backed B+Tree-like store with WAL + HDF5-based storage. Core code lives under `src/cidstore/`.
- Key modules to review when reasoning about behavior:
	- `src/cidstore/store.py` — high-level CIDStore API and async init/close
	- `src/cidstore/storage.py` — HDF5/storage integration
	- `src/cidstore/wal.py` — write-ahead log and in-memory WAL used in tests
	- `src/cidstore/maintenance.py` — background maintenance manager / analyzers (threads)
	- `src/cidstore/keys.py` — `E` key/value helpers and parsing conventions

## Testing & runtime patterns agents must follow
- Tests and fixtures deliberately avoid importing heavy native libs at collection time. See `conftest.py` for the convention: imports of cidstore modules are done lazily inside fixtures to avoid early numpy/native initialization. When modifying tests or fixtures, preserve this lazy-import pattern.
- Many tests exercise async code via synchronous helpers. The `tree` fixture wraps the async `CIDStore` with a thread + event loop; code generators should prefer calling the async APIs in `store.py` but keep compatibility with the sync wrapper pattern used in tests.
- Background threads/maintenance: components such as `maintenance_manager` are started/stopped in tests (see `conftest.py`). If you add long-running background work, ensure tests can disable/stop it deterministically.

## Conventions and small rules
- Avoid top-level imports of `cidstore` modules in test modules (use lazy imports in fixtures) to prevent collection-time failures.
- Tests expect integer-like values for results in some legacy tests; the `tree` fixture returns plain Python ints. When changing public return shapes, update tests accordingly.
- Use existing fixtures: `store`, `directory`, `wal_analyzer`, `maintenance_manager`, `tree`. They encapsulate the intended test setup (in-memory WAL, in-memory storage, testing=True).

## Where to look for examples
- `conftest.py` — test fixtures and patterns (lazy imports, SyncCIDStoreWrapper).
- `tests/` — many focused tests show expected behaviors and edge-cases (e.g. `test_multivalue.py`, `test_wal.py`, `test_storage.py`).
- `scripts/setup_dev.ps1` & `setup_dev.sh` — how maintainers create a dev environment.

If anything here is unclear or you'd like additional examples (e.g., a short snippet showing how to run a single test file or how to create a small in-memory WAL-backed store), tell me which section to expand and I will iterate.
```