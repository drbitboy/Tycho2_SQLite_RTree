#!/usr/bin/env python
"""
Harvard Binary Catalog Format c.f.

  http://tdc-www.harvard.edu/catalogs/catalogsb.html

Binary catalogs use the internal representation of the computer on which
they are used. They are currently expected to be in B1950 equatorial
coordinates unless NMAG is negated, though skymap can be told to expect
J2000 instead. All angular variables are in radians so they don't have to
be converted to work in Fortran trigonometric functions. We provide the SAO
Catalog, the PPM Catalog, and the IRAS Point Source Catalog in this format.

The catalog header tells the program what to expect in each The first 28
bytes of each file contains the following information:

Integer*4 STAR0	Subtract from star number to get sequence number
Integer*4 STAR1	First star number in file
Integer*4 STARN	Number of stars in file
		If negative, coordinates are J2000 instead of B1950
Integer*4 STNUM	0 if no star ID numbers are present
		1 if star ID numbers are in catalog file
		2 if star ID numbers are region nnnn (GSC)
		3 if star ID numbers are region nnnnn (Tycho)
		4 if star ID numbers are integer*4 not real*4
		<0 No ID number, but object name of -STNUM characters
		   at end of entry
Integer*4 MPROP	0 if no proper motion is included
		1 if proper motion is included
		2 if radial velocity is included
Integer*4 NMAG	Number of magnitudes present (0-10)
		If negative, coordinates are J2000 instead of B1950
Integer*4 NBENT	Number of bytes per star entry

Each entry in the catalog contains the following information:

Real*4 XNO		Catalog number of star [optional]
Real*8 SRA0		B1950 Right Ascension (radians)
Real*8 SDEC0		B1950 Declination (radians)
Character*2 ISP		Spectral type (2 characters)
Integer*2 MAG(NMAG)	V Magnitude * 100 [0-10 may be present]
Real*4 XRPM		R.A. proper motion (radians per year) [optional]
Real*4 XDPM		Dec. proper motion (radians per year) [optional]
Real*8 SVEL		Radial velocity in kilometers per second (optional)
Character*(-STNUM)	Object name [optional, precludes catalog number]

Catalog numbers may be omitted to save space if they are monotonically
increasing integers. Proper motions may be omitted if they are not known.
There may be up to 10 magnitudes.

Last updated 24 September 2012 [...]
"""

import os
import sys
import math
import struct

try:
  import spice
  b2jMtx = spice.pxform('B1950','J2000',0.0)
  dpr = spice.dpr()
  vdot = spice.vdot
  mxv = spice.mxv
  recrad = spice.recrad
  radrec = spice.radrec
  vhat = spice.vhat
  vscl = spice.vscl
  ucrss = spice.ucrss
  vadd = spice.vadd
  
except:

  if "DEBUG" in os.environ:
    import traceback
    traceback.print_exc()

  ### From WCStools
  X2jMtx = ( (  0.9999256795,            -0.0111814828,            -0.0048590040,           )
           , (  0.0111814828,             0.9999374849,            -0.0000271557,           )
           , (  0.0048590039,            -0.0000271771,             0.9999881946,           )
           ,)

  ### From NAIF/SPICE toolkit
  b2jMtx = ( (  9.99925707952362908e-01, -1.11789381377701350e-02, -4.85900381535927031e-03,)
           , (  1.11789381264276906e-02,  9.99937513349988705e-01, -2.71625947142470413e-05,)
           , (  4.85900384145442846e-03, -2.71579262585107768e-05,  9.99988194602374203e-01,)
           ,)
  dpr = 180.0 / math.pi
  def vdot(va,vb): return va[0]*vb[0] + va[1]*vb[1] + va[2]*vb[2]
  def mxv(m,v): return tuple([vdot(vm,v) for vm in m])
  def recrad(v):
    rsq = vdot(v,v)
    if rsq == 0.0: return (0.0,0.0,0.0,)
    import math
    r = math.sqrt(rsq)
    dec = math.asin( v[2] / r )
    ra = math.atan2( v[1], v[0] )
    if ra < 0.0: ra += 2.0 * math.pi
    return r,ra,dec

  def radrec(r,ra,dec):
    cra,sra,cdec,sdec = math.cos(ra), math.sin(ra), math.cos(dec), math.sin(dec)
    return r * cra * cdec, r * sra * cdec, r*sdec

  def vscl(x,v): return tuple( [x*i for i in v] )

  def vhat(v):
    rsq = vdot(v,v)
    if rsq == 0.0: return (0.0,0.0,0.0,)
    return vscl( 1.0/math.sqrt(rsq), v )

  def vcrss(va,vb): return tuple( [ va[(i+1)%3]*vb[(i+2)%3] -  vb[(i+1)%3]*va[(i+2)%3] for i in (0,1,2,) ] )
  def ucrss(va,vb): return vhat( vcrss(va,vb) )
  def vadd(va,vb): return tuple( [ va[i]+vb[i] for i in (0,1,2,) ] )


### Convert RA,DEC,dRA/dY,dDEC/dY in B1950 frame and epoch
### to J2000 frame and epoch (approximation)
def b2j(bra,bdec,dbra,dbdec):
  ### Convert to Cartesian unit vector in B1950 frame
  buv = radrec(1.0,bra,bdec)
  ### get direction of dRA/dY and dDEC/dY as unit vectors in B1950 frame
  buvdrady = ucrss((0.,0.,1.,),buv)
  buvddecdy = ucrss(buv,buvdrady)
  ### scale and combine to get velocity wrt tip of unit vector
  buvddy = vadd( vscl(dbra*math.cos(bdec),buvdrady), vscl(dbdec,buvddecdy) )
  ### add proper motion in B1950 frame from B1950 epoch to J2000 epoch
  buv50 = vhat( vadd(buv,vscl(50.0,buvddy)) )
  ### Transform to Cartesian vectors in J2000 frame
  juv = mxv(b2jMtx,buv50)
  juvddy = mxv(b2jMtx,buvddy)
  ### Convert to RA,DEC in J2000
  r,jra,jdec = recrad(juv)
  ### Get direction of dRA/dY and dDEC/dY as unit vectors in J2000 frame
  juvdrady = ucrss((0.,0.,1.,),juv)
  juvddecdy = ucrss(juv,juvdrady)
  ### Get magnitudes of velocity along dRA/DY and dDEC/DY unit vectors
  djra = vdot(juvdrady,juvddy) / math.cos(jdec)
  djdec = vdot(juvddecdy,juvddy)

  ### return RA,DEC, dRA/dY, dDEC/dY in J2000 frame
  return jra,jdec,djra,djdec


class HBC:

  lsb, msb, fmts = "<", ">", "llllllL"

  try: SEEK_SET = os.SEEK_SET
  except: SEEK_SET = 0

  ######################################################################
  ### Parse first 28 bytes of Harvard Binary Catalog
  def __init__(s,first28Arg=None,fileArg=None):

    if first28Arg is None:

      ### If first28Arg is not present, get first 28 bytes from file arg
      if type(fileArg) is str:

        ### If fileArg is string => treat fileArg as filepath
        s.first28 = open(fileArg,'rb').read(28)

      else:

        ### Otherwise, treat fileArg as open file
        fileArg.seek( 0, HBC.SEEK_SET )
        s.first28 = fileArg.read(28)

    else:
      ### first28Arg is present, get first 28 bytes from it
      s.first28 = first28Arg[:28]

    ### Read in header info from 28-byte header assuming LSB-first
    s.isLsb = True
    s.star0, s.star1, s.starn, s.stnum, s.mprop, s.nmag, s.nbent = struct.unpack( HBC.lsb + HBC.fmts, s.first28 )

    if s.stnum == 0 and s.mprop == 0 and s.nmag == 0:

      ### With all optionals turned off, the number of bytes per star entry
      ### should be 8(RA) + 8(DEC) + 2(ISP) = 18.  Assume MSB-first if that
      ### does not match s.nbent as read above
      s.isLsb = s.nbent == 18

    else:

      ### At least one optional field is non-zero:
      ### - abs(.nmag) must be 10 or less
      ### - .mprop must be 0, 1 or 2; allow for -1 also
      ### - .stnum must be 4 or less (may be arbitrarily negative)
      s.isLsb = abs(s.nmag) < 11 and (s.mprop > -2 and s.mprop < 3) and s.stnum < 5

    ### Invert byte order if suggested by tests above
    if not s.isLsb:
      s.star0, s.star1, s.starn, s.stnum, s.mprop, s.nmag, s.nbent = struct.unpack( HBC.msb + HBC.fmts, s.first28 )

    ### Hack mprop == -1 (0xffffffff) to 1
    if s.mprop == -1: s.mprop = 1

    def calcNbent():
      ### Calculate nbent (number of bytes per entry) from options
      nbent = 18
      if s.stnum > 0: nbent += 4          ### ObjectNumber 4-byte int
      elif s.stnum < 0: nbent -= s.stnum  ### ObjectName(-s.stnum)
      if s.mprop > 0: nbent += 8          ### Proper motion:  2 x single
      if s.mprop > 1: nbent += 8          ### Radial velocity:  1 x double
      nbent += 2 * s.nmag                 ### # of mags:  s.nmag * 2-byte int
      return nbent

    if s.isLsb and calcNbent() != s.nbent:
      s.isLsb = False
      s.star0, s.star1, s.starn, s.stnum, s.mprop, s.nmag, s.nbent = struct.unpack( HBC.msb + HBC.fmts, s.first28 )
      ### Hack mprop == -1 (0xffffffff) to 1
      if s.mprop == -1: s.mprop = 1

    if "DEBUG" in os.environ and calcNbent() != s.nbent: print( vars(s) )

    assert calcNbent() == s.nbent

    s.absStarn = abs(s.starn)
    s.absNmag = abs(s.nmag)

    s.pfx = s.isLsb and HBC.lsb or HBC.msb


  ######################################################################
  ### Get data from one record, offset from first record
  def getData(s,offset,fileArg=None,stringArg=None):

    ####################################################################
    ### read from string if present, otherwise offset records from file

    if stringArg is None:

      ### If fileArg is a string, it is a filepath; open it
      if type(fileArg) is str: fyle = open(fileArg,'rb')
      ### Othewise it is a file
      else                   : fyle = fileArg

      ### Seek to record offset and ready bytes
      fyle.seek(28 + (offset*s.nbent), HBC.SEEK_SET)
      s.string = fyle.read(s.nbent)

      ### if file was opened above, close it here
      if type(fileArg) is str: fyle.close()

    else:
      s.string = stringArg[:s.nbent]

    ### Quickie unpack helper, using s.sofar
    def unpack( fmt, L):
      tmp = s.sofar
      s.sofar += L
      if fmt is None: return s.string[tmp:s.sofar]
      return struct.unpack( s.pfx + fmt, s.string[tmp:s.sofar] )

    ### Work through string per options set in __init__() above
    ### - s.sofar keeps track of bytes read so far; usd in unpack above
    s.sofar = 0

    ### Field 1) Optional star/region ID; integer if stnum==4
    s.ino = None
    s.xno = None
    if s.stnum == 4:
      s.ino = unpack( 'l', 4 )[0]
    elif s.stnum > 0:
      s.xno = unpack( 'f', 4 )[0]
      s.ino = int(s.xno)
    else:
      s.ino = s.star1 + offset

    ### Fields 2,3) RA, DEC
    s.sra0,s.sdec0, = unpack( 'dd', 16 )

    ### Field 4:  Spectral type
    s.isp = unpack( None, 2)

    s.mag = [ unpack('h',2)[0] / 100.0 for i in range(s.absNmag)]

    ### Fields 5,6,7) Optional Proper motion (RA, DEC); Radial velocity
    s.xrpm = None
    s.xdpm = None
    s.svel = None
    if s.mprop > 0:
      s.xrpm, s.xdpm = unpack('ff',8)
      if s.mprop == 2: s.svel = unpack('d',8)[0]

    ### Convert from B1950 to J2000
    if s.starn > 0 and s.nmag > 0:
      s.b1950sra0, s.b1950sdec0, s.b1950xrpm, s.b1950xdpm = s.sra0, s.sdec0, s.xrpm, s.xdpm
      s.b1950sra0Deg, s.b1950sdec0Deg = s.b1950sra0 * dpr, s.b1950sdec0 * dpr
      if s.xrpm is None or s.xdpm is None:
        s.sra0, s.sdec0, tmpXrpm, tmpXdpm = b2j(s.sra0, s.sdec0, 0.0, 0.0 )
      else:
        s.sra0, s.sdec0, s.xrpm, s.xdpm = b2j(s.sra0, s.sdec0, s.xrpm, s.xdpm)
    else:
      s.b1950sra0, s.b1950sdec0, s.b1950xrpm, s.b1950xdpm = (None,) * 4

    ### Fields 8) Optional Object name; if s.stnum<0
    s.objectname = None
    if s.stnum < 0: s.objectname = unpack(None, -s.stnum)

    ### Ensure we read all the bytes
    assert s.sofar == s.nbent

    s.sra0Deg = s.sra0 * dpr
    s.sdec0Deg = s.sdec0 * dpr

    ### Remove temporary variables
    if not ( "DEBUG" in os.environ ):
      del s.sofar
      del s.string


if __name__=="__main__":

  ### Testing, e.g.
  ###
  ###  ./harvardbincat.py BSC5 [offset0[ offset1[ ...]]]
  ###
  ###  ./harvardbincat.py SAO.pc [offset0[ offset1[ ...]]]

  import pprint
  f = open(sys.argv[1])

  hbc=HBC(fileArg=f)

  if sys.argv[2:]:
    ### Get offset(s) from sys.argv[2:]
    for offsetArg in map(int,sys.argv[2:]):
      hbc.getData(fileArg=f, offset=offsetArg)
      pprint.pprint( vars(hbc) )
  else:
    ### If no offsets supplied, get first and last entries
    hbc.getData(fileArg=f, offset=0)
    pprint.pprint( vars(hbc) )

    hbc.getData(fileArg=f, offset=hbc.absStarn-1)
    pprint.pprint( vars(hbc) )

  f.close()
