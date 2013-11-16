#!/usr/bin/env python
"""
Put Tycho-2 main and supplement 1 catalogs and index into
SQLite DB file (default tyc2.sqlite3) using SQLite R-Tree module

Creates four TABLEs:

  tyc2indexrtree  - Index R-Tree
  tyc2index       - Index table
  tyc2catalog_uvs - Main catalog subset, magnitude and unit vector at RA,DEC
  tyc2suppl1_uvs  - Supplemental 1 catalog subset, mag and unit vector at RA,DEC


Usage:

  python tyc2_loadindex.py [--sqlite3db=tyc2.sqlite3] \\
                           [--index=index.dat] \\
                           [--catalog=catalog.dat] \\
                           [--suppl1=suppl_1.dat] \\
                           [reload] \\
                           [loadallpaths] \\
                           [test]


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
      wget -q -O - ftp://cdsarc.u-strasbg.fr/cats/I/259/ \\
      | grep tyc2.dat....gz \\
      | sed 's,^.*[^/]\(tyc2.dat....gz\).*$,\1,' \\
      | while read i ; do wget -q -O - ftp://cdsarc.u-strasbg.fr/cats/I/259/$i ; done \\
      | gunzip > catalog.dat

"""

if __name__ == "__main__":
  import os
  import sys
  import math
  import sqlite3 as sl3

  filekeys = 'index catalog suppl1 sqlite3db'.split()

  dikt = dict( reload=False
             , test=False
             , loadallpaths=False
             , sqlite3db='tyc2.sqlite3'
             , index='index.dat'
             , catalog='catalog.dat'
             , suppl1='suppl_1.dat'
             )

  for arg in sys.argv[1:]:

    for key in dikt:

      if arg == key and dikt[key] is False:
        dikt[key] = True
        break

      dashkey = '--%s=' % key
      L = len(dashkey)
      if dashkey == arg[:L]:
        dikt[key] = arg[L:]
        break

  ### Option loadallpaths:  Load all filepaths into SQLite DB
  if dikt['reload'] or dikt['loadallpaths']:

    pathsDict = {}

    for key in filekeys:
      realpath = os.path.realpath(dikt[key])
      try:
        assert os.path.isfile(realpath)
        if key == 'sqlite3db':
          assert os.access(realpath,os.W_OK)
        else:
          assert os.access(realpath,os.R_OK)
      except:
        if key == 'sqlite3db':
          if not os.path.exists(realpath):
            try:
              assert os.access(os.path.dirname(realpath),os.W_OK)
              continue
            except:
              print( '\nERROR:  %s file (%s) not writeable; exiting\n' % (key, realpath,) )
              raise
        print( '\nERROR:  %s file (%s) either not found or not a regular file; exiting\n' % (key, realpath,) )
        raise

      pathsDict[key] = os.path.realpath(dikt[key])

    ### Connect to DB, get cursor
    cn = sl3.connect(dikt['sqlite3db'])
    cu = cn.cursor()

    cu.execute("""CREATE TABLE IF NOT EXISTS tyc2paths (key varchar(32) primary key, fullpath varchar(1024))""")

    for key in pathsDict:
      print( "Loading %s file %s" % (key, pathsDict[key],) )
      cu.execute("""REPLACE INTO tyc2paths VALUES (?,?)""", (key,pathsDict[key],))

    cn.commit()
    cn.close()


  ### Option reload:  Load data into SQLite DB
  if dikt['reload']:

    ### Connect to DB, get cursor
    cn = sl3.connect(dikt['sqlite3db'])
    cu = cn.cursor()

    cu.execute("""PRAGMA synchronous = OFF""")
    cu.execute("""PRAGMA journal_mode = MEMORY""")

    ### DROP and CREATE index tables
    cu.execute("""DROP TABLE IF EXISTS tyc2indexrtree""")
    cu.execute("""DROP TABLE IF EXISTS tyc2index""")
    cu.execute("""CREATE VIRTUAL TABLE tyc2indexrtree using rtree(offset,lora,hira,lodec,hidec)""")
    cu.execute("""CREATE TABLE IF NOT EXISTS tyc2index (offset int primary key,tyc2start int, suppl1start int, tyc2end int, suppl1end int)""")

    ### DROP and CREATE catalog TABLEs
    for tableid in 'catalog suppl1'.split():
      table = ("""tyc2%s_uvs""" % (tableid,),)
      cu.execute("""DROP TABLE IF EXISTS %s""" % table)
      cu.execute("""CREATE TABLE IF NOT EXISTS %s (offset int primary key,x double, y double, z double,mag double)""" % table)
      cu.execute("""DELETE FROM %s""" % table)

    cu.execute("""DELETE FROM tyc2indexrtree""")
    cu.execute("""DELETE FROM tyc2index""")
    cn.commit()

    ### Open index.dat and read first line
    offset = 0
    f = open(dikt['index'],'rb')
    nexttoks = [i.strip() for i in f.readline().split('|')]
    rows = [int(i)-1 for i in nexttoks[:2]]

    cu.execute("""BEGIN TRANSACTION""")

    ### For each subsequent record, add data from previous line to index table
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

    class IterCat:

      def __init__(self, openfile, ngtoks):

        self.openfile = openfile

        ### Get columns in line containing B- and V-Magnitude values
        self.blo,self.bhi,self.vlo,self.vhi = [int(i) for i in ngtoks[2:]]

        ### Get bvflag, first character of catalog name
        self.bvflag =  int(ngtoks[1]) if ngtoks[1]!='X' else 0
        self.isCatalog = ngtoks[0][0] == 'c'

        self.offset = -1

        self.sql = """INSERT INTO tyc2%s_uvs VALUES (?,?,?,?,?)""" % (ngtoks[0],)

      def __iter__(self): return self
      def getSelectStatement(self): return self.sql
      def getOffset(self): return self.offset

      def next(self):

        ### Read one line from catalog file; offset is relative to first line

        for line in self.openfile:

          self.offset += 1
          if (self.offset % 100000) == 99999: sys.stdout.write('.') ; sys.stdout.flush()

          ### Skip catalog.dat lines with X in Column 13
          ### - suppl_1.dat has H or T in Column 13
          if self.isCatalog and line[13]=='X': continue

          ### Parse RA,DEC, BMag, VMag fields
          ra,dec = [rpd*float(line[i:i+12]) for i in range(15,40,13)]
          btok,vtok = [line[lo:hi].strip() for lo,hi in ((self.blo,self.bhi,),(self.vlo,self.vhi,),)]

          ### Parse magnitude pre bvflag and/or B/V mag values
          mag = 0.0
          if self.bvflag>0:
            if line[self.bvflag]==' ' or line[self.bvflag]=='B': mag = float(btok)
            if mag==0.0 and vtok: mag = float(vtok)
          else:
            if btok:  mag = float(btok)
            if mag==0.0 and vtok: mag = float(vtok)

          ### Reset zero magnitude to 20.0
          if mag==0.0: mag=20.0

          ### Convert RA,DEC to unit vector
          ### Build record of offset,unit vector XYZ components, magnitude
          ### INSERT data
          cosra, sinra, cosdec, sindec = math.cos(ra), math.sin(ra), math.cos(dec), math.sin(dec)
          return (self.offset, cosra*cosdec, sinra*cosdec, sindec, mag)

        self.openfile.close()
        raise StopIteration


    for nameplus in 'suppl1,X,83,89,90,95 catalog,81,110,116,123,129'.split():
      ### nameplus string values, one for main catalog and one for supplemental 1 catalog:
      ### - tableID,BVFlagColumn,BMagColStart,BMagColEnd,VMagColStart,VMagColEnd
      ### - tableID is key into dikt to get catalog filename
      ### - BVFlag only used in suppl_1.dat; set column to X for supplemental_1 catalog

      ngtoks = nameplus.split(',')

      ### Build insert command (table ID is ngtoks[0])

      reciter = IterCat(open(dikt[ngtoks[0]],'rb'),ngtoks)

      sql = reciter.getSelectStatement()

      print( sql )

      cu.execute("""BEGIN TRANSACTION""")
      cu.executemany(sql,reciter)
      cn.commit()

      offset = reciter.getOffset()

      reciter = None

      sys.stdout.write('%s%d records written for %s\n'%('' if offset<100000 else '\n',offset,nameplus,)),
      sys.stdout.flush()

    ### Create INDEX for each magnitude
    for tableid in 'catalog suppl1'.split():
      table = """tyc2%s_uvs""" % (tableid,)
      s ="""CREATE INDEX IF NOT EXISTS %(tableid)s_mag ON %(table)s (mag)""" % dict(tableid=tableid,table=table)
      print(s)
      cu.execute(s)

    ### Close DB connection
    cn.close()


  ### Option test:  list all *possible* stars from 56<RA<58, 23<DEC<25, mag<6.5 (Pleiades)
  if dikt['test']:

    ### HiMag, LoRA, HiRA, LoDEC, HiDEC
    dflt5=[6.5,56.0,58.0,23.0,25.0,]

    for arg in sys.argv[1:]:
      i=0
      for name in 'himag lora hira lodec hidec'.split():
        pfx = '--%s=' % (name,)
        lenPfx = len(pfx)
        if arg[:lenPfx]==pfx: dflt5[i] = float(arg[lenPfx:])
        i += 1

    cn = sl3.connect(dikt['sqlite3db'])
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
  AND tyc2catalog_uvs.mag<?
WHERE tyc2indexrtree.offset=tyc2index.offset
  AND tyc2indexrtree.hira>?
  AND tyc2indexrtree.lora<?
  AND tyc2indexrtree.hidec>?
  AND tyc2indexrtree.lodec<?
ORDER BY tyc2catalog_uvs.mag asc;
  """, dflt5)

    print(" Offset      X      Y      Z   Magn.")
    for row in cu: print( "%7d %6.3f %6.3f %6.3f %7.3f" % row )
