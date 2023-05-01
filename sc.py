from datetime import datetime
import pandas as pd
import numpy as np
import h5py
import hdf5plugin

dataFile = 'D:\\temp\\data_20-03-2023_09-56.csv'
iniFile = 'D:\\APD\\BigFile\\TrendsView.ini'
namesFile = dataFile.replace('data_', 'names_')
h5FileName = dataFile.replace('.csv', '.h5')

# Corrects csv file so actualtime can be successfully parsed by pandas

#import re
#dataFile = open(dataFile, 'r+' )
#dataFileCont = dataFile.read()
#dataFileCont = re.sub(r'(\d{2}:\d{2}:\d{2}),', r'\1.000000,', dataFileCont)
#dataFile.seek()
#dataFile.write(dataFileCont)

dtstart = datetime.now()
print('Start reading data:', dtstart)
df = pd.read_csv(dataFile, dtype={'valdouble': np.float32, 'nodeid': np.uint32}, parse_dates=['actualtime'], usecols=[0,1,2])
dtend = datetime.now()
print('Finish reading data:', dtend)
print('Reading time:', dtend-dtstart)

dtstart = datetime.now()
print('Start write h5 file:', dtstart)
h5File = h5py.File(h5FileName, 'w')

# namesFileCont = open(namesFile, 'r').read()
# h5File.create_dataset('/names.csv', data = namesFileCont)

# if not iniFile=='':
#     iniFileCont = open(iniFile, 'r').read()
#     h5File.create_dataset('/Trends.ini', data = iniFileCont)
hd5Item =  df['actualtime'].to_numpy(dtype=np.uint64)
h5File.create_dataset('/data/actualtime', data =  hd5Item, **hdf5plugin.Blosc(cname='blosclz', clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE ))
hd5Item =  df['nodeid'].to_numpy(dtype=np.uint32)
h5File.create_dataset('/data/nodeid', data =  hd5Item, **hdf5plugin.Blosc(cname='blosclz', clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE ))
hd5Item =  df['valdouble'].to_numpy(dtype=np.float32)
h5File.create_dataset('/data/valdouble', data =  hd5Item, **hdf5plugin.Blosc(cname='blosclz', clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE ))

del df
del hd5Item

dtend = datetime.now()
print('Finish write h5 file:', dtend)
print('Writing time: ', dtend - dtstart)

# np.array(f['data/actualtime'][:], dtype='datetime64[ns]')

dtbegin = datetime.now()
print('Start read h5 file:', dtbegin)

df = pd.DataFrame()

with h5py.File(h5FileName, 'r') as f:
    nodeid = np.array(f['data/nodeid'][:], dtype=np.uint16)
    valdouble = np.array(f['data/valdouble'][:], dtype=np.float64)
    actualtime = np.array(f['data/actualtime'][:], dtype='datetime64[ns]')


    df['nodeid'] = nodeid
    df['valdouble'] = valdouble
    df['actualtime'] = actualtime

dtend = datetime.now()
print('Finish read h5 file:', dtend)
print('Total h5 file reading time:', dtend - dtbegin)

pass
