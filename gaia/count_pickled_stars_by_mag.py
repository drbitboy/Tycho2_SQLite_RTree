"""
Count number of stars in GAIA pickle files, also numbers of stars with
magnitude less than or equal to 18.0 and to 17.6, 

Usage:

  find gaiapickle/ -name 'GaiaSource_*_*.gaiapickle" \
  | python count_pickled_stars_by_mag.py

"""
import os
import sys
import gaia

### Initialize start counts, file count
nall,n176,n180,nfile = [0]*4

### Loop over lines from STDIN
for rawline in sys.stdin:
  
  ### Read GAIA pickle file
  with open(rawline.strip(),'rb') as f:
    gp = gaia.pickle.load(f)

  ### Get column names, look for photometric magnitudes
  ### - phot_g_mean_mag
  ### - phot_bp_mean_mag
  ### - phot_rp_mean_mag
  cns = gp.column_names
  cnsi = cns.index
  imagcols = [cnsi(s) for s in cns if s.endswith('_mean_mag')]

  ### Add number of rows to total count
  nall += len(gp.rows)

  ### Loop over rows (stars)
  for row in gp.rows:
    ### Get minimum, non-None magnitude
    minmag = min([mag for mag in [row[i] for i in imagcols] if not None is mag])

    ### Increment count for magnitudes 18.0 and 17.6, as appropriate
    if minmag > 18.0: continue
    n180 += 1
    if minmag > 17.6: continue
    n176 += 1

  ### Increment file counter
  nfile += 1

  ### Progress indicator
  if nfile % 1000: continue
  sys.stdout.write('.')
  sys.stdout.flush()

### Output results
print('')
print(dict(n176=n176,n180=n180,nall=nall))
