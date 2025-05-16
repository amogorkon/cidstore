"""
CIDTree CLI tools for migration, backup, and recovery (Spec 3)

Usage:
    python -m cidtree.cli migrate <h5file>
    python -m cidtree.cli backup <h5file> <backupfile>
    python -m cidtree.cli restore <backupfile> <h5file>

Commands:
    migrate   - Migrate directory/bucket structure to canonical HDF5 layout
    backup    - Create a backup of the HDF5 file (with WAL flush)
    restore   - Restore HDF5 file from backup (with WAL replay)

"""
import argparse
import shutil
import sys
import h5py
from .tree import CIDTree
from .wal import WAL
from .storage import StorageManager

def migrate(h5file):
    """Migrate directory/bucket structure to canonical HDF5 layout."""
    with h5py.File(h5file, 'a', libver='latest', swmr=True) as f:
        tree = CIDTree(StorageManager(f))
        tree.migrate_directory()
        print(f"Migration complete for {h5file}.")

def backup(h5file, backupfile):
    """Create a backup of the HDF5 file (with WAL flush)."""
    with h5py.File(h5file, 'r+', libver='latest', swmr=True) as f:
        # Flush WAL and all data
        f.flush()
    shutil.copy2(h5file, backupfile)
    print(f"Backup created: {backupfile}")

def restore(backupfile, h5file):
    """Restore HDF5 file from backup (with WAL replay)."""
    shutil.copy2(backupfile, h5file)
    with h5py.File(h5file, 'a', libver='latest', swmr=True) as f:
        wal = WAL(StorageManager(f))
        wal.replay()
        print(f"Restore and WAL replay complete for {h5file}.")

def main():
    parser = argparse.ArgumentParser(description="CIDTree CLI tools")
    subparsers = parser.add_subparsers(dest="command")

    migrate_parser = subparsers.add_parser("migrate", help="Migrate HDF5 layout")
    migrate_parser.add_argument("h5file", help="Path to HDF5 file")

    backup_parser = subparsers.add_parser("backup", help="Backup HDF5 file")
    backup_parser.add_argument("h5file", help="Path to HDF5 file")
    backup_parser.add_argument("backupfile", help="Backup file path")

    restore_parser = subparsers.add_parser("restore", help="Restore HDF5 file from backup")
    restore_parser.add_argument("backupfile", help="Backup file path")
    restore_parser.add_argument("h5file", help="Path to HDF5 file to restore")

    args = parser.parse_args()
    if args.command == "migrate":
        migrate(args.h5file)
    elif args.command == "backup":
        backup(args.h5file, args.backupfile)
    elif args.command == "restore":
        restore(args.backupfile, args.h5file)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
