"""
Test 18.0 magnitude-limited Gaia SQLite3 R-Tree database

"""

import os
import sys
import sqlite3 as sl3

sl3_cmds = [
 """ATTACH 'gaia.sqlite3' AS light;"""
,"""ATTACH 'gaia_heavy.sqlite3' AS heavy;"""
,"""
SELECT light.gaiartree.ralo
     , light.gaiartree.declo

     , light.gaialight.phot_g_mean_mag
     , light.gaialight.phot_bp_mean_mag
     , light.gaialight.phot_rp_mean_mag

     , heavy.gaiaheavy.source_id
     , heavy.gaiaheavy.offset

FROM light.gaiartree

INNER JOIN light.gaialight
ON light.gaiartree.offset=light.gaialight.offset

INNER JOIN heavy.gaiaheavy
ON light.gaiartree.offset=heavy.gaiaheavy.offset

WHERE light.gaiartree.ralo>123.0
  AND light.gaiartree.rahi<124.0
  AND light.gaiartree.declo>-45.0
  AND light.gaiartree.dechi<-44.0
  AND light.gaiartree.lomag>17.0

ORDER BY light.gaialight.phot_g_mean_mag DESC
       , light.gaialight.phot_bp_mean_mag DESC
       , light.gaialight.phot_rp_mean_mag DESC

LIMIT 10
;
"""
]

expected_results = [(123.6518325805664,  -44.16962432861328,  20.31247,  17.843435, 17.843435, 5520900294000500480, 383657976)
                   ,(123.2238998413086,  -44.50053787231445,  20.167744, 17.977682, 17.977682, 5520867033770796288, 383652959)
                   ,(123.18251037597656, -44.79827117919922,  19.955414, 17.496376, 17.496376, 5520853255516256384, 383650733)
                   ,(123.10675811767578, -44.3040771484375,   19.881796, 17.735321, 17.735321, 5520884591606437760, 383655743)
                   ,(123.08183288574219, -44.84864044189453,  19.865965, 17.862461, 17.862461, 5520851262660577792, 383650350)
                   ,(123.27877044677734, -44.69755935668945,  19.765274, 17.835224, 17.835224, 5520859886945800704, 383651863)
                   ,(123.88534545898438, -44.31599807739258,  19.736536, 17.659071, 17.659071, 5520145547994385536, 383527688)
                   ,(123.79359436035156, -44.6627082824707,   19.729372, 17.783945, 17.783945, 5520119468951554432, 383523166)
                   ,(123.9137191772461,  -44.186824798583984, 19.712257, 17.896118, 17.896118, 5520897167263692416, 383657443)
                   ,(123.19523620605469, -44.06882095336914,  19.703314, 17.786676, 17.786676, 5520957296812428928, 383666943)
]

def fi(s):
  try   : return int(s.strip())
  except: return float(s.strip())

expected_results_2 = [tuple(map(fi,row.split('|'))) for row in """
123.651832580566|-44.1696243286133|20.31247|17.843435|17.843435|5520900294000500480|383657976
123.223899841309|-44.5005378723145|20.167744|17.977682|17.977682|5520867033770796288|383652959
123.182510375977|-44.7982711791992|19.955414|17.496376|17.496376|5520853255516256384|383650733
123.106758117676|-44.3040771484375|19.881796|17.735321|17.735321|5520884591606437760|383655743
123.081832885742|-44.8486404418945|19.865965|17.862461|17.862461|5520851262660577792|383650350
123.278770446777|-44.6975593566895|19.765274|17.835224|17.835224|5520859886945800704|383651863
123.885345458984|-44.3159980773926|19.736536|17.659071|17.659071|5520145547994385536|383527688
123.793594360352|-44.6627082824707|19.729372|17.783945|17.783945|5520119468951554432|383523166
123.913719177246|-44.186824798584|19.712257|17.896118|17.896118|5520897167263692416|383657443
123.195236206055|-44.0688209533691|19.703314|17.786676|17.786676|5520957296812428928|383666943
""".strip().split('\n')]

if "__main__" == __name__:
  cn = sl3.connect(":memory:")
  cu = cn.cursor()
  
  actual_results = sum([[row for row in cu.execute(cmd)] for cmd in sl3_cmds],[])

  if '--log-results' in sys.argv[1:]:
    pfx = '['
    for row in actual_results:
      sys.stdout.write(pfx)
      pfx = ','
      print(row)
    sys.stdout.write(']\n')
    sys.stdout.flush()

  biggest_error = 0.0
  biggest_error_2 = 0.0
  for expected_row,actual_row,expected_row_2 in zip(expected_results,actual_results,expected_results_2):
    for expected_column,actual_column,expected_column_2 in zip(expected_row,actual_row,expected_row_2):
      if expected_column!=actual_column:
        frac_err = abs((actual_column-expected_column) / (actual_column+expected_column))
        biggest_error = max([biggest_error,2*frac_err])
        assert 0.5e-6 > abs(frac_err)
      if expected_column_2!=actual_column:
        frac_err = abs((actual_column-expected_column_2) / (actual_column+expected_column_2))
        biggest_error_2 = max([biggest_error_2,2*frac_err])

  sys.stderr.write('SUCCESS:  [{0}]; max fractional errors = {1:.2e},{2:.2e}\n'.format(__file__,biggest_error,biggest_error_2))
