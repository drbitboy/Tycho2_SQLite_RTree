"""
Verify that SourceIDs do not overlap between GAIA CSV files

Usage (BASH):

  find gaiapickles/ -name 'GaiaSource_*_*.gaiapickle' \
  | sort -t_ -n -k2,2 \
  | python overlap_check_gaia.py \
    && echo No overlap \
    || echo Overlap found

"""
import os
import sys
import gaia

### Loop over GAIA pickle filenames sorted by 1st source ID in filename
idlasthi = 0
for rawline in sys.stdin:
  ### Load pickled data, initialize lo and hi ID variables
  gp = gaia.gaia_read_pickle(rawline.strip())
  idlo = idhi = gp.rows[0][0]

  ### Loop over rows, adjust lo and hi ID variables as needed
  for row in gp.rows:
    idnext = row[0]
    if idlo > idnext: idlo = idnext
    elif idhi < idnext: idhi = idnext

  ### Assert lo ID of data is greater than hi ID of previous data
  assert idlasthi < idlo

  ### Advance highest ID so far
  idlasthi = idhi
