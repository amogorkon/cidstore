

| Area | Spec(s) | Coverage | Gaps/Notes |
|---------------------|--------------|----------|---------------------------------------------|
| Core workflow | 1, 5, 6, 8 | Good | Add more explicit promotion/demotion checks |
| Data types/struct | 2, 5 | Good | Add negative/invalid field tests |
| Multi-value keys | 5 | Good | Many xfail, implement promotion/demotion |
| Bucket/Directory | 3, 6, 9 | Good | Many xfail, implement migration/split logic |
| WAL/Recovery | 4, 8 | Good | Add partial WAL/crash tests |
| Deletion/GC | 7 | Good | Many xfail, implement compaction/orphan |
| Maintenance | 3, 7 | Good | Add adaptive/error recovery tests |
| HDF5 layout | 9 | Good | Add concurrent/atomicity tests |
| Concurrency | 8 | Good | Add lock contention/isolation tests |
| Control API | 10 | Good | Add more error/edge case tests |
| ZMQ integration | 10 | Good | Add more error/edge case tests |
| Roundtrip | 10, 11 | Good | - |

| Test File | Issue(s) | Action |
|----------------------------------|--------------------------------------------|-----------------------------------|
| wip.py | Not a real test | Move/remove from test suite |
| test_directory.py, test_node.py | xfail, non-canonical fields | Update/remove bad checks, xfail as needed |
| test_inline_slots.py | Bad slot checks, spill marker logic | Update to match spec |
| test_multivalue.py | xfail, non-canonical fields | Update/remove bad checks, xfail as needed |
| test_datatypes.py, test_keys.py | Non-canonical field checks | Update to match spec |
| test_hdf5_basic.py, test_hdf5_integration.py | Directory/valueset checks | Update to match canonical layout |
| test_concurrency.py | Sync/async mismatch, key/value types | Update to use async/E objects |
| test_store_integration.py, test_wal_integration.py | Not real tests | Move/remove from test suite |
| test_storage_context.py | Empty file | Remove |
| test_.py | Order-sensitive asserts | Use sets or sort |
| test_background_simple.py | Mock harness, not real test | Move/remove from test suite |
| test_tree.py | xfail, key/value types | Update/remove as needed |
| test_storage.py | xfail, deprecated features | Update/remove as needed |
| test_chaos_load.py | Load/chaos, not unit test | Move/remove from test suite |
| test_zmq_integration.py, test_async_zmq_server.py | Integration, needs server | Mark as integration, remove from CI |

| Test Name | Action |
|----------------------------------|---------------------------------------------|
| test_bucket_split_and_merge | Use E keys/values, check pointer validity |
| test_sorted_unsorted_region_logic| Use E keys/values, keep xfail if needed |
| test_directory_resize_and_migration | Use E keys/values, accept "sharded" type |
| test_directory_structure_and_types| Use E keys/values, check canonical fields |
