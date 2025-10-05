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

Python 3.13 Features:
    - Uses improved argparse with deprecation support
    - Benefits from enhanced error messages and tracebacks
    - Compatible with free-threaded CPython (PEP 703)
"""

import argparse
import shutil
from pathlib import Path

import h5py

from .storage import Storage
from .store import CIDStore
from .wal import WAL


def migrate(h5file: str | Path) -> None:
    """Migrate directory/bucket structure to canonical HDF5 layout."""
    h5file = Path(h5file)
    with h5py.File(str(h5file), "a", libver="latest", swmr=True):
        store = CIDStore(Storage(h5file), WAL(None))
        store.migrate_directory()
    print(f"Migration complete for {h5file}.")


def backup(h5file: str | Path, backupfile: str | Path) -> None:
    """Create a backup of the HDF5 file (with WAL flush)."""
    h5file = Path(h5file)
    backupfile = Path(backupfile)
    with h5py.File(str(h5file), "r+", libver="latest", swmr=True) as f:
        f.flush()
    shutil.copy2(str(h5file), str(backupfile))
    print(f"Backup created: {backupfile}")


def restore(backupfile: str | Path, h5file: str | Path) -> None:
    """Restore HDF5 file from backup (with WAL replay)."""
    backupfile = Path(backupfile)
    h5file = Path(h5file)
    shutil.copy2(str(backupfile), str(h5file))
    with h5py.File(str(h5file), "a", libver="latest", swmr=True):
        wal = WAL(Storage(h5file))
        wal.replay()
    print(f"Restore and WAL replay complete for {h5file}.")


def main() -> None:
    parser = argparse.ArgumentParser(description="CIDTree CLI tools")
    subparsers = parser.add_subparsers(dest="command")

    migrate_parser = subparsers.add_parser("migrate", help="Migrate HDF5 layout")
    migrate_parser.add_argument("h5file", help="Path to HDF5 file", type=Path)

    backup_parser = subparsers.add_parser("backup", help="Backup HDF5 file")
    backup_parser.add_argument("h5file", help="Path to HDF5 file", type=Path)
    backup_parser.add_argument("backupfile", help="Backup file path", type=Path)

    restore_parser = subparsers.add_parser(
        "restore", help="Restore HDF5 file from backup"
    )
    restore_parser.add_argument("backupfile", help="Backup file path", type=Path)
    restore_parser.add_argument(
        "h5file", help="Path to HDF5 file to restore", type=Path
    )

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
