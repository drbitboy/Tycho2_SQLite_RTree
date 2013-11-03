Tycho2_SQLite_RTree
===================

Accessing the Tycho2 star catalog using the R-Tree module of SQLite

From comments in tyc2_loadindex.py:

Put Tycho-2 main and supplement 1 catalogs and index into
SQLite DB file (tyc2.sqlite3) using SQLite R-Tree module

Creates four tables:

- tyc2indexrtree  - Index R-Tree
- tyc2index       - Index table
- tyc2catalog_uvs - Main catalog subset, magnitude and unit vector at RA,DEC
- tyc2suppl1_uvs  - Supplemental 1 catalog subset, mag and unit vector at RA,DEC


Usage:

  python tyc2_loadindex.py [reload] [test]


Prerequisites:

- SQLite (see import sqlite3 below)

  - With R-Tree module compiled in

- Assumes catalog.dat, suppl_1.dat and index.dat are in the current
  directory

  - e.g. in BASH

      wget http://cdsarc.u-strasbg.fr/viz-bin/nph-Cat/tar.gz?I%2F259 -O I_259.tar.gz -O - | tar zxf -

      gunzip index.dat.gz

      gunzip suppl_1.dat.gz

      for i in tyc2.dat.??.gz ; do gunzip < $i ; done > catalog.dat

    OR 

      wget -q ftp://cdsarc.u-strasbg.fr/cats/I/259/index.dat.gz -O - | gunzip > index.dat

      wget -q ftp://cdsarc.u-strasbg.fr/cats/I/259/suppl_1.dat.gz -O - | gunzip > suppl_1.dat

      wget -q -O - ftp://cdsarc.u-strasbg.fr/cats/I/259/ \

      | grep tyc2.dat....gz \

      | sed 's,^.*[^/]\(tyc2.dat....gz\).*$,\1,' \

      | while read i ; do wget -q -O - ftp://cdsarc.u-strasbg.fr/cats/I/259/$i ; done \

      | gunzip > catalog.dat

