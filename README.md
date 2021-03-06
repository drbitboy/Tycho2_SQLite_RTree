Tycho2_SQLite_RTree
===================

Accessing the Tycho2 star catalog using the R-Tree module of SQLite.


Quick start
----

    git clone https://github.com/drbitboy/Tycho2_SQLite_RTree.git
    cd Tycho2_SQLite_RTree/
    ./one_step_build.bash


Quick start with explanations
----

    ####################################################################
    ### Get star catalog SQLite R*Tree code and CHDIR into repository

    git clone https://github.com/drbitboy/Tycho2_SQLite_RTree.git
    cd Tycho2_SQLite_RTree/


    ####################################################################
    ### Configure and build

    ./one_step_build.bash


    ### That one_step_build.bash script does the following:

       #################################################################
       ### Ensure automake does not run

       # touch --date="`date`" configure.ac Makefile.am aclocal.m4 Makefile.in configure


       #################################################################
       ### Configure, build and test

       # ./configure 


Overview
----

From comments in tyc2_loadindex.py:

Put Tycho-2 main and supplement 1 catalogs and index into a SQLite DB
file (tyc2.sqlite3) using SQLite R-Tree module for the index.

Creates four tables:

- tyc2indexrtree  - Index R-Tree
- tyc2index       - Index table
- tyc2catalog_uvs - Main catalog subset, magnitude and unit vector at RA,DEC
- tyc2suppl1_uvs  - Supplemental 1 catalog subset, mag and unit vector at RA,DEC

It may make sense to put the whole catalog into an R-Tree, but I'm just winging it and I am not sure if the R-Tree module would work for zero-sized ranges.
The nice thing about doing catalog is that magnitude could be the third axis.

Usage
----

  python tyc2_loadindex.py [reload] [test[plot]]

    ALSO

  make clean test


Prerequisites
----

- SQLite (see import sqlite3 below)

  - With R-Tree module compiled in

- Assumes catalog.dat, suppl_1.dat and index.dat are in the current
  directory

  - Using provided python script:

      python httpgunzip.py --doit index.dat suppl_1.dat tyc2.dat ReadMe


  - Alternatives via BASH

      wget http://cdsarc.u-strasbg.fr/viz-bin/nph-Cat/tar.gz?I%2F259 -O I_259.tar.gz -O - | tar zxf -

      gunzip index.dat.gz

      gunzip suppl_1.dat.gz

      for i in tyc2.dat.??.gz ; do gunzip < $i ; done > catalog.dat

    OR 

      pfx="ftp://cdsarc.u-strasbg.fr/cats/I/259/"

      wget -q $pfx/index.dat.gz -O - | gunzip > index.dat

      wget -q $pfx/suppl_1.dat.gz -O - | gunzip > suppl_1.dat

      wget -q -O - $pfx/ \

      | grep 'tyc2.dat.[0-9][0-9].gz' \

      | gawk '{printf "tyc2.dat.%02d.gz\n", i++}' \

      | while read gz ; do wget -q -O - $pfx/$gz | gunzip ; done > catalog.dat

