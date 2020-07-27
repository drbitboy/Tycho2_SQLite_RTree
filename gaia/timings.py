"""
Evaluate and plot time it takes for each GAIA CSV file to be downloaded
and pickled, using inter-.gaiapickle file modification times

Usage:

  ls -lrt --full gaiapickles/ | grep gaiapickle\$ | python timings.py

[ls -lrt --full ...] output looks like this:

  -rw-rw-r-- 1 dad dad 1461685 2020-07-15 08:28:18.243727631 -0400 GaiaSource_3007619637220870144_3007779895338408704.gaiapickle

"""
import os
import sys
import gzip
import numpy
from datetime import datetime,timedelta

### Initialize last file's datetime, list of timedeltas, loop over STDIN
dt,tds = None,list()
for line in sys.stdin:

  ### Move last file's datetime to olddt, split input line into tokens
  olddt = dt
  toks = line.strip().split()

  ### Get year-month-day and hour-minute-second.fraction tokens
  ymd,hmsf = toks[5:7]

  ### Split off fraction seconds, parse time
  hms,f = hmsf.split('.')
  dt = datetime.strptime(' '.join([ymd,hms]),'%Y-%m-%d %H:%M:%S'
       ) + timedelta(microseconds=1e6*float('.{0}'.format(f)))

  if None is olddt: continue

  ### Calculate timedelta, ignore if > 120s, else add to list
  td = (dt - olddt).total_seconds()
  if td > 120: continue
  tds.append(td)

### Convert timedelta list to numpy array
tdsnp = numpy.array(tds)

### Sort for cumulative freq. distribution, calculate mean and median
tds.sort()
L = len(tds)
tdsmean,tdsmedian = tdsnp.mean(),tds[L>>1]

### - Sum number of bytes in CSV GZ files to calculate download bitrate
with gzip.open('csv/csv_gaia_index.html.gz','rt') as fgz:
  allbytes = [int(s.strip().split()[-1]) for s in fgz if 'GaiaSource_' in s]
  nbits = sum(allbytes) << 3
  Mbps = 1e-6 * nbits / (tdsmean * len(allbytes))

print(dict(mean=tdsmean,median=tdsmedian,Mbps=Mbps))

########################################################################
### Start plotting
import matplotlib.pyplot as plt

### figure with two plots, 2 high by 1 wide
fig,(ax0,ax1,) = plt.subplots(2,1)

### Figure title
fig.suptitle('After processing {0} CSVs; {1:.1f}Mbps'.format(L,Mbps))

### Plot raw data
ax0.set_yscale('log')
ax0.set_xlabel('CSV #')
ax0.set_ylabel('s/CSV')
ax0.plot(tdsnp,'k,')

### Plot cumulative frequency distribution
ax1.set_xscale('log')
ax1.set_xlabel('s/CSV')
ax1.set_ylabel('Cumulative CSV count')
ax1.axvline(tdsmean,color='r',label='Mean {0:.1f}s/CSV'.format(tdsmean))
ax1.axvline(tdsmedian,color='g',label='Median {0:.1f}s/CSV'.format(tdsmedian))
ax1.plot(tds,range(L),'k',label='Cum. freq. s/CSV')
ax1.legend(loc='upper left')

### Show plot
plt.show()

