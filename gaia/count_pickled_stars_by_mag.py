"""
Count number of stars in GAIA pickle files, also numbers of stars with
magnitude less than or equal to 17.5 and to 17.0, 

Usage:

  find gaiapickles/ -name 'GaiaSource_*_*.gaiapickle' \
  | python count_pickled_stars_by_mag.py \
  | tee count_pickled_stars_by_mag.log

"""
import os
import sys
import gaia

### Initialize start counts, file count
nall,n170,n175,nfile = [0]*4

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

    ### Increment count for magnitudes 17.5 and 17.0, as appropriate
    if minmag > 17.5: continue
    n175 += 1
    if minmag > 17.0: continue
    n170 += 1

  ### Increment file counter
  nfile += 1

  ### Progress indicator
  if nfile % 1000: continue
  sys.stderr.write('.')
  sys.stderr.flush()

### Output results
sys.stderr.write('\n')
sys.stderr.flush()
print(dict(n170=n170,n175=n175,nall=nall))
