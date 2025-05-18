"""
Spec 10: Conclusion/Full Roundtrip (TDD)
Covers full roundtrip, persistence, and advanced edge cases.
All tests are TDD-style and implementation-agnostic.
"""


def test_full_roundtrip(tmp_path):
    from cidtree.main import CIDTree

    path = tmp_path / "roundtrip.h5"
    tree = CIDTree(str(path))
    # Insert a variety of keys/values
    for i in range(100):
        tree.insert(f"key{i}", i)
    # Delete some
    for i in range(0, 100, 3):
        tree.delete(f"key{i}", i)
    # Close and reopen
    del tree
    from cidtree.main import CIDTree as CIDTree2

    tree2 = CIDTree2(str(path))
    # Check all values
    for i in range(100):
        expected = [] if i % 3 == 0 else [i]
        assert list(tree2.get(f"key{i}")) == expected


def test_persistence_and_recovery(tmp_path):
    from cidtree.main import CIDTree

    path = tmp_path / "persist.h5"
    tree = CIDTree(str(path))
    tree.insert("persist", 123)
    del tree
    tree2 = CIDTree(str(path))
    assert 123 in list(tree2.get("persist"))
    if hasattr(tree2, "recover"):
        tree2.recover()
        assert 123 in list(tree2.get("persist"))
