"""
Sum number of rows in GAIA pickle files to get total
number of stars

Usage:

  find gaiapickles/ -name 'GaiaSource_*_*.gaiapickle' \
  | python count_pickled_stars.py

"""
import gaia,sys

### Initialize count, loop over filenames
n = 0
for rawline in sys.stdin:
  ### Read pickled data, accumulate row count, delete data
  gp = gaia.gaia_read_pickle(rawline.strip())
  n += gp.rows.__len__()
  del gp

  ### Output progress:  one full-stop every 1kfiles
  if n%1000: continue
  sys.stdout.write('.')
  sys.stdout.flush()

### Output total
print(n)
