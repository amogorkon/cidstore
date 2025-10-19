# Migration to 256-bit SHA-256 CIDs - Status

## Completed ✅

### Core Infrastructure
- ✅ Updated `constants.py`:
  - KEY_DTYPE: Now 4×64-bit components (high, high_mid, low_mid, low)
  - HASH_ENTRY_DTYPE: 256-bit keys and values in slots
  - WAL_RECORD_DTYPE: 256-bit keys and values
  - DELETION_RECORD_DTYPE: 256-bit keys and values
  - OP NamedTuple: 4 components each for key and value
  - RECORD_SIZE: Increased from 64 to 128 bytes

### E Class (keys.py)
- ✅ Updated E class to support 256-bit integers
- ✅ Added 4 properties: `high`, `high_mid`, `low_mid`, `low`
- ✅ Updated `from_str()` to use full 256-bit SHA-256 hash
- ✅ Updated `to_hdf5()` to serialize 4 components
- ✅ Updated `from_entry()` to handle 256-bit fields
- ✅ Updated `from_hdf5()` to deserialize 4 components
- ✅ Updated `__new__()` to handle 2-component (legacy) and 4-component construction
- ✅ Updated `composite_key()` for 256-bit triples
- ✅ Updated `composite_value()` for 256-bit P/O encoding
- ✅ Updated `decode_composite_value()` for 256-bit decoding

### WAL Module (wal.py)
- ✅ Updated `pack_record()` to handle 4-component keys/values
- ✅ Updated `unpack_record()` to handle 4-component keys/values
- ✅ Updated `log_insert()` signature: (k_high, k_high_mid, k_low_mid, k_low, v_high, v_high_mid, v_low_mid, v_low)
- ✅ Updated `log_delete()` signature
- ✅ Updated `log_delete_value()` signature
- ✅ Updated all record construction code

### Storage Module (storage.py)
- ✅ Updated mem_index to use 5-tuple keys: (bucket_name, k_high, k_high_mid, k_low_mid, k_low)
- ✅ Updated `_publish_mem_index()` to accept 4 key components
- ✅ Updated `ensure_mem_index()` to accept 4 key components
- ✅ Updated `_ensure_mem_index_sync()` to accept 4 key components
- ✅ Updated `find_entry_in_bucket_sync()` to accept 4 key components
- ✅ Updated `find_entry()` async wrapper to accept 4 key components
- ✅ Updated `get_values()` to reconstruct 256-bit values from all 4 components (both inline and spill modes)
- ✅ Updated `_apply_insert_sync()` to pass all 4 components to _publish_mem_index

### Store Module (store.py)
- ✅ Updated `get()` mem_index lookup to use 5-tuple keys with all 4 key components
- ✅ Updated all `find_entry()` calls to pass all 4 key components (high, high_mid, low_mid, low)
- ✅ Updated `_maybe_merge_bucket()` to extract and pass all 4 key components
- ✅ Updated `compact()` to pass all 4 key components to _ensure_mem_index_sync
- ✅ Updated `_demote_from_spill()` to compute and pass all 4 key components

### Maintenance Module (maintenance.py)
- ✅ Updated `_sort_unsorted_regions()` to extract mid components from entries and pass to ensure_mem_index

### Tests
- ✅ `test_datatypes.py`: All 10 tests passing (E class, key/value storage and retrieval)

### Documentation
- ✅ Updated spec00-Intro.md: 256-bit CID description
- ✅ Updated spec02-Data Types.md:
  - E class diagram with 4 components
  - HashEntry with 256-bit fields
  - Data types table with 32B keys
  - Network message E structure table
- ✅ Updated README.md: 256-bit CID description

## TODO 📋

### Medium Priority (Recommended Next Steps)

1. **Run Extended Test Suite**
   - Run `test_store.py`, `test_wal.py`, `test_storage.py` to validate core functionality
   - Run `test_multivalue.py` to verify ValueSet/spill operations with 256-bit values
   - Fix any remaining test failures related to 4-component key handling

2. **Spill Dataset Naming** (Optional Enhancement)
   - Current: `sp_{bucket_id}_{key_high}_{key_low}` (uses only 2 components)
   - Consider: Include mid components for better uniqueness guarantee
   - Note: Internal spill records already store all 4 components correctly

3. **Remaining Call Sites** (Sweep and Verify)
   - Search for any remaining 2-arg ensure_mem_index calls
   - Verify all bucket routing uses full 4 components where needed
   - Check deletion/GC operations use 4 components

### Documentation (Low Priority)

6. **Remaining Specs**
   - [ ] spec03-Bucket Structure: Update key references
   - [ ] spec04-WAL: Update record structure
   - [ ] spec05-Multi-Value: Update value references
   - [ ] spec06-Splitting: Update split algorithms
   - [ ] spec07-Deletion: Update deletion records
   - [ ] spec08-Concurrency: Update examples
   - [ ] spec09-HDF5: Update dataset layouts
   - [ ] spec10-Microservice: Update message formats

### Optional/Future

7. **Performance Optimization**
   - [ ] Consider SIMD operations for 256-bit comparisons
   - [ ] Benchmark memory usage increase (2x per key/value)
   - [ ] Update cache strategies for larger keys

8. **Backward Compatibility**
   - [ ] Migration tool for existing 128-bit databases
   - [ ] Consider versioning scheme for data format

## Testing Strategy

1. **Phase 1**: Fix compilation errors
   - Update all syntax errors in core modules
   - Ensure project imports cleanly

2. **Phase 2**: Fix unit tests
   - Update test data to use 256-bit values
   - Verify E class functionality

3. **Phase 3**: Integration testing
   - Run full test suite
   - Verify WAL replay works correctly
   - Test bucket splitting/merging

4. **Phase 4**: Performance validation
   - Benchmark insert/lookup performance
   - Compare to baseline (128-bit)
   - Verify memory usage is acceptable

## Notes

- **Legacy Support**: E class maintains backward compatibility by accepting 2-component tuples and zero-extending to 256 bits
- **Breaking Change**: HDF5 file format is incompatible - existing databases cannot be read without migration
- **Wire Protocol**: Network messages (msgpack) need to send/receive 4 components per E value
- **Memory Impact**: Storage size doubles (16B → 32B per key/value), but provides full SHA-256 collision resistance

## Next Steps

1. Start with `wal.py` - update record packing/unpacking
2. Then `storage.py` - update key/value component handling
3. Then `store.py` - update bucket routing and operations
4. Finally run test suite and fix one test file at a time
