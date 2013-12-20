#!/usr/bin/env python
"""
Put Harvard Binary Catalog  into
SQLite DB file (*.sqlite3) using SQLite R-Tree module

Creates three TABLEs:

  PREFIXrtree  - R-Tree (Index,Lo/HiRA,Lo/HiDEC,Lo/HiMagnitude)
  PREFIXdata   - Data table (Index,X,Y,Z)
  PREFIXpaths  - Filepath(s) to raw input file


Usage:

  python hbc_loadindex.py [--sqlite3db=bsc5.sqlite3] \\
                          [--hbcatalog=BSC5] \\
                          [--prefix=bsc5] \\
                          [reload] \\
                          [loadallpaths] \\
                          [test[plot]]

Prerequisites:

- SQLite (see import sqlite3 below)

  - With R-Tree module compiled in

- Assumes Harvard Binary Catalog (e.g. BSC5) is in the current
  directory

  - e.g. in BASH

      wget http://tdc-www.harvard.edu/catalogs/BSC5

"""

if __name__ == "__main__":
  import os
  import sys
  import math
  try:
    import sqlite3 as sl3
  except:
    import sqlite3ghost as sl3
    sys.stderr.write( "Falling back to sqlite2ghost...\n" )

  ######################################################################
  ### Parse arguments

  filekeys = 'hbcatalog sqlite3db'.split()

  dikt = dict( reload=False
             , loadallpaths=False
             , test=False
             , testplot=False
             , sqlite3db='bsc5.sqlite3'
             , prefix='bsc5'
             , hbcatalog='BSC5'
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

  ######################################################################
  ### Option loadallpaths:  Load all filepaths into SQLite DB
  if dikt['reload'] or dikt['loadallpaths']:

    pathsDict = {}

    for key in filekeys:
      try:
        if key == 'sqlite3db' and not dikt[key]:
          dikt[key] = dikt[key + 'Fmt'] % (os.path.basename(dikt['hbcatalog']),)
        realpath = os.path.realpath(dikt[key])
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
              sys.stderr.write( '\nERROR:  %s file (%s) not writeable; exiting\n\n' % (key, realpath,) )
              raise
        sys.stderr.write( '\nERROR:  %s file (%s) either not found or not a regular file; exiting\n\n' % (key, realpath,) )
        raise

      pathsDict[key] = os.path.realpath(dikt[key])

    ### Connect to DB, get cursor
    cn = sl3.connect(dikt['sqlite3db'])
    cu = cn.cursor()

    cu.execute("""CREATE TABLE IF NOT EXISTS %(prefix)spaths (key varchar(32) primary key, fullpath varchar(1024))""" % dikt)

    for key in pathsDict:
      sys.stderr.write( "Loading %s file %s\n" % (key, pathsDict[key],) )
      cu.execute("""REPLACE INTO %(prefix)spaths VALUES (?,?)""" % dikt, (key,pathsDict[key],))

    cn.commit()
    cn.close()


  ######################################################################
  ### Option reload:  Load data into SQLite DB
  if dikt['reload']:

    ### Connect to DB, get cursor
    cn = sl3.connect(dikt['sqlite3db'])
    cu = cn.cursor()

    cu.execute("""PRAGMA synchronous = OFF""")
    cu.execute("""PRAGMA journal_mode = MEMORY""")

    ### DROP and CREATE index tables
    cu.execute("""DROP TABLE IF EXISTS %(prefix)srtree""" % dikt)
    cu.execute("""DROP TABLE IF EXISTS %(prefix)sdata""" % dikt)
    cu.execute("""CREATE VIRTUAL TABLE %(prefix)srtree using rtree(offset,lora,hira,lodec,hidec,lomag,himag)""" % dikt)
    cu.execute("""CREATE TABLE IF NOT EXISTS %(prefix)sdata (offset int primary key, x double, y double, z double)""" % dikt)
    cu.execute("""DELETE FROM %(prefix)srtree""" % dikt)
    cu.execute("""DELETE FROM %(prefix)sdata""" % dikt)
    cn.commit()

    ### Radian per degree and degree per radian conversion factors
    rpd = math.pi / 180.0
    dpr = 180.0 / math.pi

    ### For each line (star) in main and supplemental_1 catalogs, parse
    ### zero-based columns here per one-based columns in ReadMe.  INSERT into
    ### table the line ### offset (key), XYZ components of RA,DEC unit vector,
    ### and magnitude

    class IterCat:

      def __init__(self, HBCfile):

        from harvardbincat import HBC

        self.HBCfile = HBCfile

        self.hbc = HBC(fileArg=self.HBCfile)

        self.offset = -1

        self.sqls = ( """INSERT INTO %(prefix)srtree VALUES (?,?,?,?,?,?,?)""" % dikt
                    , """INSERT INTO %(prefix)sdata VALUES (?,?,?,?)""" % dikt
                    ,)

      def __iter__(self): return self
      def getSelectStatements(self): return self.sqls
      def getOffset(self): return self.offset

      def next(self):

        ### Increment offset and read one record from
        ### catalog file; offset is relative to first record

        self.offset += 1
        if self.offset < self.hbc.absStarn:

          self.hbc.getData(self.offset, fileArg=self.HBCfile)

          ### Convert RA,DEC (radians) to unit vector
          cosra, sinra, cosdec, sindec = math.cos(self.hbc.sra0), math.sin(self.hbc.sra0), math.cos(self.hbc.sdec0), math.sin(self.hbc.sdec0)

          ### Build record of offset,unit vector XYZ components, RA,DEC (degrees) , magnitude
          return (self.offset, cosra*cosdec, sinra*cosdec, sindec, self.hbc.sra0Deg, self.hbc.sdec0Deg, self.hbc.mag[0])

        self.HBCfile.close()
        raise StopIteration


    ### Build insert command

    reciter = IterCat(open(dikt['hbcatalog'],'rb'))

    sqlRtree,sqlData = reciter.getSelectStatements()

    sys.stderr.write( "%s\n%s\n" % (sqlRtree,sqlData,) )

    cu.execute("""BEGIN TRANSACTION""")
    for offset,x,y,z,ra,dec,mag in reciter:
      cu.execute(sqlRtree,(offset,ra,ra,dec,dec,mag,mag,))
      cu.execute(sqlData,(offset,x,y,z,))
    cn.commit()

    dikt['offset'] = reciter.getOffset()

    reciter = None

    sys.stderr.write('%(offset)d records written for %(prefix)s\n' % dikt )
    sys.stderr.flush()

    ### Create INDEXes for RA, DEC and magnitude
    cu.execute("""BEGIN TRANSACTION""")
    for field in ''.split():
      dikt['field2idx'] = field
      s ="""CREATE INDEX IF NOT EXISTS %(prefix)sdata_%(field2idx)s ON %(prefix)sdata (%(field2idx)s)""" % dikt
      sys.stderr.write( "%s\n" % (s,) )
      cu.execute(s)
    cn.commit()

    ### Close DB connection
    cn.close()


  ######################################################################
  ### Option test:  list all *possible* stars from 56<RA<58, 23<DEC<25, mag<6.5 (Pleiades)
  if dikt['test'] or dikt['testplot']:

    ### HiMag, LoRA, HiRA, LoDEC, HiDEC
    arg5=[6.5,56.0,58.0,23.0,25.0,]

    for arg in sys.argv[1:]:
      i=0
      for name in 'himag lora hira lodec hidec'.split():
        pfx = '--%s=' % (name,)
        lenPfx = len(pfx)
        if arg[:lenPfx]==pfx: arg5[i] = float(arg[lenPfx:])
        i += 1

    import math
    dpr = 180.0 / math.pi
    print("TableName  Offset      X      Y      Z   Magn.")
    cn = sl3.connect(dikt['sqlite3db'])
    cu = cn.cursor()

    cu.execute("""
SELECT %(prefix)sdata.offset
      ,%(prefix)sdata.x ,%(prefix)sdata.y ,%(prefix)sdata.z
      ,%(prefix)srtree.lomag
FROM %(prefix)srtree
INNER JOIN %(prefix)sdata
   ON %(prefix)srtree.offset=%(prefix)sdata.offset
WHERE %(prefix)srtree.offset=%(prefix)sdata.offset
  AND %(prefix)srtree.lomag<?
  AND %(prefix)srtree.hira>?
  AND %(prefix)srtree.lora<?
  AND %(prefix)srtree.hidec>?
  AND %(prefix)srtree.lodec<?
ORDER BY %(prefix)srtree.lomag asc;
  """ % dikt, arg5)

    def recra(row,isTestplot):
      if isTestplot: return (' %7.3f %7.3f' % (dpr*math.atan2(row[2],row[1]), dpr*math.asin(row[3]),),)
      return ('',)
    rows = [ (dikt['prefix'],) + row + recra(row,dikt['testplot']) for row in cu.fetchall()]
    for row in rows: print( "%9s %7d %6.3f %6.3f %6.3f %7.3f%s" % row )

    if len(rows) and dikt['testplot']:
      import matplotlib.pyplot as plt
      RAs = [ dpr*math.atan2(row[3],row[2]) for row in rows]
      DECs = [ dpr*math.asin(row[4]) for row in rows]

      plt.plot(RAs, DECs, 'o', label=dikt["prefix"])
      plt.xlabel("RA, deg")
      plt.ylabel("DEC, deg")
      plt.title("%(prefix)s Pleiades (Messier 45)" % dikt)
      plt.gca().invert_xaxis()
      plt.legend()
      plt.show()
