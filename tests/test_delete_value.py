import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


async def test_delete_value_with_E(store):
    key = E.from_int(0x00000000000000010000000000000002)
    val = E.from_int(0x00000000000000030000000000000004)
    await store.insert(key, val)
    res = await store.get(key)
    assert any(int(v) == int(val) for v in res)
    await store.delete_value(key, val)
    res2 = await store.get(key)
    assert all(int(v) != int(val) for v in res2)


async def test_delete_value_with_tuple(store):
    key = E.from_int((1 << 64) | 2)
    val = (3, 4)
    # Insert using E to ensure a real stored value
    await store.insert(key, E.from_int((3 << 64) | 4))
    res = await store.get(key)
    assert any(int(v) == ((3 << 64) | 4) for v in res)
    # Delete using tuple form
    await store.delete_value(key, val)
    res2 = await store.get(key)
    assert all(int(v) != ((3 << 64) | 4) for v in res2)


async def test_delete_value_with_list_and_numpy_like(store):
    key = E.from_int((10 << 64) | 11)
    await store.insert(key, E.from_int((12 << 64) | 13))
    # Delete using numpy-like list
    await store.delete_value(key, [12, 13])
    res = await store.get(key)
    assert all(int(v) != ((12 << 64) | 13) for v in res)
