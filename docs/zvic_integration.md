# ZVIC Integration in CIDStore

This document describes the integration of ZVIC (Zero-Version Interface Contracts) into the CIDStore project.

## What is ZVIC?

ZVIC is a paradigm and toolset for ensuring interface stability in Python codebases without relying on semantic versioning. It:

- Eliminates the need for version numbers through signature hashing
- Validates contracts dynamically at runtime
- Catches incompatible changes immediately (fail-fast)
- Works through assert blocks (zero cost in production with `python -O`)

## Integration Status

ZVIC 2025.43.0 is integrated throughout CIDStore with runtime enforcement enabled when running in debug mode (`__debug__ == True`).

### Modules with ZVIC Enforcement

The following modules have ZVIC runtime contract enforcement enabled:

1. **`cidstore.plugins`** - Plugin system interfaces
   - `PluginRegistry` class
   - `SpecializedDataStructure` base class

2. **`cidstore.storage`** - HDF5 storage backend
   - `Storage` class and all storage operations

3. **`cidstore.wal`** - Write-Ahead Log
   - `WAL` class and WAL operations

4. **`cidstore.store`** - Main CIDStore class
   - `CIDStore` async API
   - CRUD operations

5. **`cidstore.predicates`** - Predicate specialization system
   - `PredicateRegistry` class
   - Specialized data structures (CounterStore, MultiValueSetStore)

6. **`cidstore.keys`** - Entity (E) and key utilities
   - `E` class constructors and properties
   - Composite key functions

### How It Works

At the end of each enforced module, you'll find:

```python
# Enable ZVIC runtime contract enforcement when assertions are on
if __debug__:
    from cidstore.zvic_init import enforce_contracts
    enforce_contracts()
```

This code:
- Only runs when assertions are enabled (not in production with `python -O`)
- Wraps all public callables with ZVIC constraint checking
- Validates types and constraints at runtime
- Raises structured exceptions on contract violations

## Performance Impact

**ZVIC has ZERO performance impact in production** because:

1. All ZVIC checks are in `assert` blocks
2. Running with `python -O` removes all assertions and ZVIC code
3. The `if __debug__:` blocks are compiled out in optimized mode

In development/testing mode:
- Minimal overhead (ZVIC checks are fast)
- Performance tests in `test_zvic_compatibility.py` verify acceptable overhead
- Hot paths remain performant even with ZVIC active

## Configuration

### Enabling/Disabling ZVIC

ZVIC is enabled by default in debug mode. To disable it even in debug mode:

```bash
# Disable ZVIC runtime checks
export CIDSTORE_ZVIC_ENABLED=0

# Run your code
python your_script.py
```

### Production Deployment

For production, always run with optimization:

```bash
python -O your_script.py
```

This removes all assertion-based code, including ZVIC checks, for maximum performance.

### Checking ZVIC Status

```python
from cidstore.zvic_init import get_zvic_info

info = get_zvic_info()
print(f"ZVIC enabled: {info['enabled']}")
print(f"Debug mode: {info['debug_mode']}")
print(f"ZVIC available: {info['zvic_available']}")
```

## Testing

### Compatibility Tests

Run the ZVIC compatibility test suite:

```bash
# Run all ZVIC compatibility tests
pytest tests/test_zvic_compatibility.py -v

# Skip ZVIC tests if not needed
pytest -m "not zvic"
```

The test suite validates:
- Interface stability across all key modules
- Plugin system contract compliance
- Storage/WAL/Store integration contracts
- E key class interface consistency
- Performance baselines

### Writing Compatibility Tests

Add new compatibility tests to `tests/test_zvic_compatibility.py`:

```python
class TestYourModuleCompatibility:
    """Test your module interface compatibility."""

    def test_your_function_signature(self):
        """Document baseline and check compatibility."""
        from cidstore.your_module import your_function

        # Test that function exists and is callable
        assert callable(your_function)

        # Test signature (ZVIC validates at runtime)
        result = your_function(expected_args)
        assert result is not None
```

## CI/CD Integration

### GitHub Actions

Add ZVIC checks to your CI pipeline:

```yaml
# .github/workflows/test.yml
name: Test with ZVIC

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest
      - name: Run ZVIC compatibility tests
        run: pytest tests/test_zvic_compatibility.py -v
```

### Pre-commit Hook

Add ZVIC validation to pre-commit:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: zvic-compatibility
        name: ZVIC Compatibility Check
        entry: pytest tests/test_zvic_compatibility.py
        language: system
        pass_filenames: false
```

## Compatibility Guarantees

### What ZVIC Checks

ZVIC validates the following contract elements:

1. **Function Signatures**
   - Parameter names (for non-positional-only params)
   - Parameter types
   - Parameter kinds (positional-only, keyword-only, *args, **kwargs)
   - Default values
   - Return types

2. **Type Compatibility**
   - Contravariant parameter types (can widen)
   - Covariant return types (can narrow)
   - Union type expansions
   - Optional parameter additions

3. **Constraint Compatibility**
   - Parameter constraints (via annotations)
   - Constraint narrowing/widening rules
   - Cross-hair integration (if installed)

### Incompatible Changes

The following changes will be caught by ZVIC:

- **Breaking**: Renaming function parameters (non-positional-only)
- **Breaking**: Changing parameter types incompatibly
- **Breaking**: Removing parameters
- **Breaking**: Changing required parameters to optional
- **Breaking**: Narrowing parameter constraints
- **Breaking**: Changing parameter order

### Compatible Changes

These changes are safe and ZVIC-compliant:

- **Safe**: Adding new optional parameters at the end
- **Safe**: Widening parameter types (e.g., `int` → `int | str`)
- **Safe**: Making optional parameters required (contravariance)
- **Safe**: Widening parameter constraints
- **Safe**: Renaming positional-only parameters
- **Safe**: Narrowing return types (covariance)

## Advanced Features

### CrossHair Integration

For advanced constraint analysis using CrossHair:

```bash
# Install ZVIC with CrossHair support
pip install "zvic[crosshair]==2025.43.0"
```

This enables symbolic execution for deeper constraint validation.

### Baseline Comparisons

To compare current code against a baseline:

```python
from pathlib import Path
from zvic import load_module
from zvic.compatibility import is_compatible

# Load baseline (e.g., from git tag)
baseline = load_module(Path('baseline/module.py'), 'baseline')

# Load current version
current = load_module(Path('src/cidstore/module.py'), 'current')

# Check compatibility
try:
    is_compatible(baseline.function, current.function)
    print("✓ Compatible")
except SignatureIncompatible as e:
    print(f"✗ Incompatible: {e}")
```

## Troubleshooting

### ZVIC ImportError

If you see `ImportError: No module named 'zvic'`:

```bash
pip install zvic==2025.43.0
```

### ZVIC Disabled Unexpectedly

Check configuration:

```python
from cidstore.zvic_init import get_zvic_info
print(get_zvic_info())
```

Common causes:
- Running with `python -O` (intended for production)
- `CIDSTORE_ZVIC_ENABLED=0` in environment
- ZVIC not installed

### Constraint Violation Errors

If ZVIC raises constraint violations in your code:

1. **Review the error message** - ZVIC provides detailed diagnostics
2. **Check type annotations** - Ensure they match actual usage
3. **Verify constraints** - Ensure constraint logic is correct
4. **Update tests** - Add regression tests for the violation

### Performance Issues

If ZVIC checking slows down development:

1. **Disable for specific sessions**: `export CIDSTORE_ZVIC_ENABLED=0`
2. **Profile hot paths**: Use `pytest --profile` to identify bottlenecks
3. **Report to ZVIC**: Performance issues in ZVIC should be reported upstream

## References

- [ZVIC Project](https://github.com/amogorkon/zvic)
- [ZVIC Documentation](https://github.com/amogorkon/zvic/tree/main/docs)
- [ZVIC Specification](https://github.com/amogorkon/zvic/tree/main/specs)
- [CIDStore Plugin Authoring](docs/plugin_authoring.md)

## Migration Notes

### From 2025.35.1 (Pre-ZVIC) to 2025.35.2+ (With ZVIC)

- All existing code continues to work unchanged
- ZVIC enforcement is opt-in via debug mode
- No breaking changes introduced by ZVIC integration
- Production builds (`python -O`) are unaffected

### Future Versions

Going forward:
- All new public APIs will have ZVIC enforcement
- Breaking changes will be caught in CI before merge
- Plugin authors can rely on stable interfaces
- Semantic versioning becomes less critical (contracts self-document)

## Contributing

When contributing to CIDStore with ZVIC integration:

1. **Add ZVIC to new modules**: Include the enforcement block at module end
2. **Write compatibility tests**: Document baseline interfaces
3. **Test with ZVIC on**: Run tests without `python -O`
4. **Verify production**: Test with `python -O` to ensure no overhead
5. **Document contracts**: Use clear type hints and docstrings

## License

ZVIC is a separate project with its own license. CIDStore's integration with ZVIC is covered under CIDStore's MIT license.
