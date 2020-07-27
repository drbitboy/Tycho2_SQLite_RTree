"""
Parse and plot output from count_gaia.py

Usage:

  python -u count_gaia.py | tee count_gaia.log
  ### - setup

  python plot_count_gaia.py

"""

### Output from count_gaia.py:
with open('count_gaia.log','r') as f: exec(f.read())


from math import cos,pi
import matplotlib.pyplot as plt
rpd = pi / 180.0

### Variable results is list of dicts, all dicts have the same keys;
### Re-sample results to be dict of lists, each list comprises data
### from one key
keys = results[0].keys()
lts = dict(zip(keys,[[dt[key] for dt in results] for key in keys]))

### Extract lists to be plotted to local variables
lodecdegs,rates,rowsums,counts,deltats = [lts[key] for key in 'lodecdeg rate rowsum count dt'.split()]

### Declinations list is -90 to +89; convert to -89.5 to +89.5, and
### and take cosine of each value to estimate length of each parallel
decdegs = [dec+0.5 for dec in lodecdegs]
coss = [cos(decdeg*rpd) for decdeg in decdegs]

########################################################################
### Start figure with four plots (2x2), with decdegs as abscissa for all
fig,((axspdec,axcumul,),(axspra,axrate,)) = plt.subplots(2,2,sharex=True)

### rowsums is list of star count at each declination
axspdec.plot(decdegs,rowsums)
axspdec.set_ylabel('Stars/1degParallel')

### Scale to star count per square degree
axspra.plot(decdegs,[rowsum/(c*360) for rowsum,c in zip(rowsums,coss)])
axspra.set_ylabel('Stars/square degree')

### Plot cumulative star count
axcumul.plot(decdegs,counts)
axcumul.set_ylabel('Cumulative star count')

### Plot rate stars were queried from SQLite3 database
axrate.plot(decdegs,rates)
axrate.set_ylabel('Star rate, Hz')

### Label abscissae of bottom plots; add fig title, show plot, sum times
axspra.set_xlabel('Dec, deg')
axrate.set_xlabel('Dec, deg')

fig.suptitle('Gaia:  ~557M stars < mag 18.0; {0} retrieved'.format(counts[-1]))

plt.show()

print(sum(deltats))
