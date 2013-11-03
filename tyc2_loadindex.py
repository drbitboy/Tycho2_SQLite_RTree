#!/usr/bin/env python
"""
Put Tycho-2 main and supplement 1 catalogs and index into
SQLite DB file (tyc2.sqlite3) using SQLite R-Tree module

Creates four tables:

  tyc2indexrtree  - Index R-Tree
  tyc2index       - Index table
  tyc2catalog_uvs - Main catalog subset, magnitude and unit vector at RA,DEC
  tyc2suppl1_uvs  - Supplemental 1 catalog subset, mag and unit vector at RA,DEC


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

"""

import sys
import math
import sqlite3 as sl3


### Option reload:  Load data into SQLite DB
if 'reload' in sys.argv[1:]:

  ### Connect to DB, get cursor
  cn = sl3.connect("tyc2.sqlite3")
  cu = cn.cursor()

  ### DROP and CREATE index tables
  cu.execute("""DROP TABLE IF EXISTS tyc2indexrtree""")
  cu.execute("""DROP TABLE IF EXISTS tyc2index""")
  cu.execute("""CREATE VIRTUAL TABLE tyc2indexrtree using rtree(offset,lora,hira,lodec,hidec)""")
  cu.execute("""CREATE table IF NOT EXISTS tyc2index (offset int primary key,tyc2start int, suppl1start int, tyc2end int, suppl1end int)""")

  ### DROP and CREATE catalog tables
  for tableid in 'catalog suppl1'.split():
    table = ("""tyc2%s_uvs""" % (tableid,),)
    cu.execute("""DROP TABLE IF EXISTS %s""" % table)
    cu.execute("""CREATE TABLE IF NOT EXISTS %s (offset int primary key,x double, y double, z double,mag double)""" % table)
    cu.execute("""CREATE INDEX IF NOT EXISTS mag ON %s (mag)""" % table)
    cu.execute("""DELETE FROM %s""" % table)

  cu.execute("""DELETE FROM tyc2indexrtree""")
  cu.execute("""DELETE FROM tyc2index""")
  cn.commit()

  ### Open index.dat and read first line
  offset = 0
  f = open('index.dat','rb')
  nexttoks = [i.strip() for i in f.readline().split('|')]
  rows = [int(i)-1 for i in nexttoks[:2]]

  ### For each subsequent records, add data from previous line to index table
  ### - R-Tree table, tyc2indexrtree, contains line offset (key), RA, DEC ranges
  ### - Index table, tyc2index, contains line offset (key), plus start and
  ###   end+1 offsets into main and supplemental_1 catalogs
  for line in f:
    toks = nexttoks[:]
    hilos = [offset] + [float(i) for i in toks[-4:]]
    nexttoks = [i.strip() for i in line.split('|')]
    rows = [offset] + rows[-2:] + [int(i)-1 for i in nexttoks[:2]]
    cu.execute( """INSERT INTO tyc2indexrtree VALUES (?,?,?,?,?)""", hilos)
    cu.execute( """INSERT INTO tyc2index VALUES (?,?,?,?,?)""", rows)
    offset += 1

  sys.stdout.write('%d records written for tyc2index and rtree\n'%(offset,)),
  f.close()

  cn.commit()

  ### Radian per degree conversion factor
  rpd = math.pi / 180.0

  ### For each line (star) in main and supplemental_1 catalogs, parse
  ### zero-based columns here per one-based columns in ReadMe.  INSERT into
  ### table the line ### offset (key), XYZ components of RA,DEC unit vector,
  ### and magnitude

  for nameplus in 'suppl_1:suppl1:X:83:89:90:95 catalog:81:110:116:123:129'.split():
    ### nameplus string values, one for main catalog and one for supplemental 1 catalog:
    ### - catalogName:tableName:BVFlagColumn:BMagColStart:BMagColEnd:VMagColStart:VMagColEnd
    ### - table name is optional if it is the same as the catalog name
    ### - BVFlag only used in suppl_1.dat; set column to X for supplemental_1 catalog

    ### Split colon-separated tokens from nameplus string
    ngtoks = nameplus.split(':')

    ### Build insert command (table name is ngtoks[-6])
    sql = """INSERT INTO tyc2%s_uvs VALUES (?,?,?,?,?)""" % (ngtoks[-6],)

    ### Get columns in line containing B- and V-Magnitude values
    blo,bhi,vlo,vhi = [int(i) for i in ngtoks[-4:]]

    ### Get bvflag, first character of catalog name
    bvflag =  int(ngtoks[-5]) if ngtoks[-5]!='X' else 0
    cs1 = ngtoks[0][0]

    ### Read one line from catalog file; offset is relative to first line
    offset = 0
    for line in open(ngtoks[0]+'.dat'):

      ### Skip catalog.dat lines with X in Column 13
      ### - suppl_1.dat has H or T in Column 13
      if cs1!='c' or line[13]!='X':

        ### Parse RA,DEC, BMag, VMag fields
        ra,dec = [rpd*float(line[i:i+12]) for i in range(15,40,13)]
        btok,vtok = [line[lo:hi].strip() for lo,hi in ((blo,bhi,),(vlo,vhi,),)]

        ### Parse magnitude pre bvflag and/or B/V mag values
        mag = 0.0
        if bvflag>0:
          if line[bvflag]==' ' or line[bvflag]=='B': mag = float(btok)
          if mag==0.0 and vtok: mag = float(vtok)
        else:
          if btok:  mag = float(btok)
          if mag==0.0 and vtok: mag = float(vtok)

        ### Reset zero magnitude to 20.0
        if mag==0.0: mag=20.0

        ### Convert RA,DEC to unit vector
        ### Build record of offset,unit vector XYZ components, magnitude
        ### INSERT data
        cosra,sinra,cosdec,sindec = math.cos(ra),math.sin(ra),math.cos(dec),math.sin(dec)
        rek4 = (offset, cosra*cosdec,sinra*cosdec,sindec,mag)
        cu.execute( sql, rek4)

      ### Increment offset, output a full stop every 50k lines for progress
      offset += 1
      if (offset %50000) == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
        cn.commit()

    sys.stdout.write('%d records written for %s\n'%(offset,nameplus,)),
    sys.stdout.flush()
    cn.commit()

  ### Close DB connection
  cn.close()


### Option test:  list all possible stars from 50<RA<55, 20<DEC<25, mag<7.5 (Around Pleiades)
if 'test' in sys.argv[1:]:
  cn = sl3.connect("tyc2.sqlite3")
  cu = cn.cursor()
  cu.execute("""
SELECT tyc2catalog_uvs.offset
      ,tyc2catalog_uvs.x ,tyc2catalog_uvs.y ,tyc2catalog_uvs.z ,tyc2catalog_uvs.mag
FROM tyc2indexrtree
INNER JOIN tyc2index
   ON tyc2indexrtree.offset=tyc2index.offset
INNER JOIN tyc2catalog_uvs
   ON tyc2index.tyc2start<=tyc2catalog_uvs.offset
  AND tyc2index.tyc2end>tyc2catalog_uvs.offset
  AND tyc2catalog_uvs.mag<7.5
WHERE tyc2indexrtree.offset=tyc2index.offset
  AND tyc2indexrtree.lora<55.0
  AND tyc2indexrtree.hira>50.0
  AND tyc2indexrtree.lodec<25.0
  AND tyc2indexrtree.hidec>20.0
ORDER BY tyc2catalog_uvs.mag asc;
""")

  for row in cu: print row
