"""config.py"""

# HDF5 file paths and dataset names
HDF5_FILE = "triplestore.h5"
WAL_DATASET = "/wal"
CONFIG_GROUP = "/config"
NODES_GROUP = "/nodes"
VALUES_GROUP = "/values/sp"
WAL_METADATA_GROUP = "/wal_meta"

# B+Tree parameters
ORDER = 128  # Max keys per node before split
MIN_KEYS = ORDER // 2 - 1
CHUNK_SIZE = 4096 // 32  # = 128 entries/chunk
MULTI_VALUE_THRESHOLD = 100

USE_SWMR = True
