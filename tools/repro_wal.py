import asyncio
from pathlib import Path

import h5py

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


async def main(tmpdir: str):
    tmp = Path(tmpdir)
    file = tmp / "walrec.h5"
    walfile = tmp / "walrec.wal"
    print('Creating storage1, wal1, tree1')
    storage1 = Storage(path=file)
    wal1 = WAL(path=walfile)
    tree1 = CIDStore(storage1, wal1)
    k = E.from_str('recov')
    v = E(1)
    print('Insert...')
    await tree1.insert(k, v)
    print('After insert: wal1.replay(truncate=False)=', wal1.replay(truncate=False))
    print('Closing storage1.file and wal1')
    if storage1.file:
        storage1.file.close()
    wal1.close()

    print('Reopen storage2 and wal2')
    storage2 = Storage(path=file)
    wal2 = WAL(path=walfile)
    print('wal2 head/tail', wal2._get_head(), wal2._get_tail())
    print('wal2.replay(truncate=False)=', wal2.replay(truncate=False))

    tree2 = CIDStore(storage2, wal2)
    # wait a bit for any background replay
    await asyncio.sleep(0.1)
    print('wal2.replay(truncate=False) after sleep=', wal2.replay(truncate=False))

    # Inspect HDF5 file contents
    with h5py.File(file, 'r') as f:
        print('HDF5 groups:', list(f.keys()))
        if 'buckets' in f:
            print('Buckets:', list(f['buckets'].keys()))
            for b in f['buckets']:
                ds = f['buckets'][b]
                print('Bucket', b, 'shape', ds.shape)
                for i in range(ds.shape[0]):
                    e = ds[i]
                    print('Entry', i, 'key_high', int(e['key_high']), 'key_low', int(e['key_low']), 'slots', e['slots'])

    # Call get
    result = await tree2.get(k)
    print('tree2.get result:', result)
    storage2.close()
    wal2.close()

if __name__ == '__main__':
    import tempfile
    td = tempfile.mkdtemp()
    asyncio.run(main(td))
