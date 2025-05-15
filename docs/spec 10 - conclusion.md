
# 10. Conclusion

This revised specification replaces the B⁺Tree design with a hash directory model—an approach that better suits our requirements for single-key exact lookups and multi-value key handling without range scans. Key highlights include:

- **Core Purpose & Data Types:** A disk-backed hash directory engineered to map 128-bit keys to variable-sized sets of 128-bit values using fixed-size, structured data types.
- **Bucket Structure:** A flat array of hash buckets (managed via HDF5) replaces hierarchical nodes. Each bucket is dynamically split or merged based on load, ensuring O(1) lookups.
- **SWMR & Copy-on-Write:** The design leverages HDF5’s SWMR mode, with all modifications performed via copy-on-write, ensuring atomic updates and quick recovery.
- **Hybrid Multi-Value Handling:** Multi-value keys are stored inline for low cardinality and are auto-promoted to external value lists when a threshold is exceeded.
- **Key Count Tracking & Deletion:** In-memory key counts drive dynamic bucket rebalancing while coordinated WAL and deletion logs ensure robust recovery and efficient garbage collection.