"""
Search GAIA star database using RA/DEC boxes approximately
one square degree in size; accumulate count of stars found

"""
import os
import sys
import sqlite3
from math import cos,pi
from datetime import datetime,timedelta

### Radians/degree
rpd = pi / 180.0

### Connect to Gaia light database
cn = sqlite3.connect('gaia.sqlite3')
cu = cn.cursor()

### Initialize cumulative start count and time
count = 0
t1 = datetime.now()

### Loop over Declination lower limit from -90 ro +89 degrees
for lodecdeg in map(float,range(-90,90)):

  ### Search box will be one-degree high (in DEC)
  hidecdeg = lodecdeg + 1

  ### Scale RA step size so box searched will be ~1 square degree
  ### Initialize RA to 0; initialize sub-sum
  dradeg = 1.0 / cos((lodecdeg+0.5)*rpd)
  hiradeg,rowsum = 0.0,0

  ### Loop over RA steps
  while hiradeg < 360.0:

    ### Set RA limits
    loradeg = hiradeg
    hiradeg = loradeg + dradeg

    ### Query SQLite for stars in box
    ### - >= for lower RA and DEC limits
    ### - < for upper RA and DEC limits
    cu.execute("""
SELECT count(0)
FROM gaiartree
WHERE rahi >= ?
  AND ralo < ?
  AND declo >= ?
  AND declo < ?
""",(loradeg,hiradeg,lodecdeg,hidecdeg,))

    ### Sum count of stars found
    rowsum += cu.fetchone()[0]

  ### Calculate delta-time spent in RA steps loop
  t0 = t1
  t1 = datetime.now()
  dt = (t1 - t0).total_seconds()
  
  ### Accumulate star count from the RA steps loop
  count += rowsum

  ### Print out results
  sys.stdout.write(lodecdeg < -89.5 and 'results=[' or ',')
  print(dict(rowsum=rowsum,dt=dt,rate=rowsum/dt,count=count,lodecdeg=lodecdeg,dradec=dradeg))

### Clean-up:  close database cursor and connection
cu.close()
cn.close()
print(']')
