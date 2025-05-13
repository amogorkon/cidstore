## 9. HDF5


```plaintext
root/
  ├── wal: Dataset         # Write-Ahead Log records
  ├── config: Attribute    # e.g., root_node path, format_version
  ├── nodes/
  │   ├── root: Attribute  # Path to root node
  │   ├── 1234: Dataset    # Internal node (keys and child pointers stored separately or as parallel arrays)
  │   └── 5678: Dataset    # Leaf node (keys, values, prev, next)
  └── values/
      └── sp/             # External value-list datasets for promoted multi-value keys
          └── A_loves: Dataset  # Contains list of CIDs
```