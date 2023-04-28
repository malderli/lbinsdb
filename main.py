import numpy as np
import pandas as pd

import datetime
import time


if __name__ == '__main__':
    tdef = np.dtype([('nodeid', np.uint16), ('valdouble', np.float64), ('actualtime', np.str_), ('quality', np.uint8)])
    t = np.dtype([('nodeid', np.uint16), ('valdouble', np.float64), ('actualtime', 'datetime64[ns]'), ('quality', np.uint8)])

    start = time.time()
    df = pd.read_csv('data_13-03-2023_17-08/data_13-03-2023_17-08.csv', dtype=tdef)
    df['actualtime'] = pd.to_datetime(df['actualtime'])
    # df['actualtime'] = pd.to_numeric(df['actualtime'], downcast='float')
    # dt_lst = [pd.to_datetime(element).to_numpy() for element in df['actualtime']]
    # df['actualtime'] = pd.DataFrame(dt_lst, dtype=object)
    end = time.time()

    print('pd.read_csv|parse_dates:', end - start)

    recs = df.to_records(index=False)

    recs.tofile('test.sdb')

    start = time.time()
    recs = np.fromfile('test.sdb', dtype=t)
    df = pd.DataFrame(recs)
    end = time.time()

    print('np.fromfile:', end - start)



