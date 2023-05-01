import numpy as np
import pandas as pd

import datetime
import time
import h5py

if __name__ == '__main__':
    path = 'D:\\temp'
    path2 = 'D:\\temp\\data_20-03-2023_09-56.csv'
    outpath = 'D:\\temp\\data_20-03-2023_09-56.hdf5'

    tdef = np.dtype([('nodeid', np.uint16), ('valdouble', np.float64), ('actualtime', np.str_), ('quality', np.uint8)])
    t = np.dtype([('nodeid', np.uint16), ('valdouble', np.float64), ('actualtime', np.uint64), ('quality', np.uint8)])

    start = time.time()
    df = pd.read_csv(path2, dtype=tdef)
    df['actualtime'] = pd.to_datetime(df['actualtime'])
    end = time.time()

    print('pd.read_csv|parse_dates:', end - start)

    recs = df.to_records(index=False)

    # SDB test
    recs.tofile('test.sdb')

    start = time.time()
    recs = np.fromfile('test.sdb', dtype=t)
    df = pd.DataFrame(recs)
    end = time.time()

    print('np.fromfile:', end - start)

    # hdf5 test

    with h5py.File(outpath, 'w') as f:
        f.create_dataset("default", data=recs)

