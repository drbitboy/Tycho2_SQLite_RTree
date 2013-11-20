import sys
import zlib
import urllib2

class httpgunzip:
  def __init__(self, fnUrl, fnOut, initial=None):
    self.fnUrl = fnUrl
    self.fnOut = fnOut
    self.initial = initial
    self.fmt = 'ftp://cdsarc.u-strasbg.fr/cats/I/259/%s%s.gz'

  def go(self,initial):
    if initial is None: mode,mfx = 'wb', ''
    elif not initial  : mode,mfx = 'wb', '.00'
    else              : mode,mfx = 'ab', '.%02d' % (initial,)

    try:
      fn = self.fmt % (self.fnUrl,mfx,)
      sys.stderr.write( 'Starting %s ...\n' % (fn,) )
      sys.stderr.flush()
      self.fZin = urllib2.urlopen(fn)
    except:
      import traceback
      traceback.print_exc()
      return None

    self.fOut = open(self.fnOut,mode)
    dco = zlib.decompressobj(15+32)

    while True:
      dada = self.fZin.read( 10000 )
      if not dada: break
      self.fOut.write( dco.decompress(dada) )

    self.fOut.write( dco.flush() )
    self.fOut.close()

    self.fZin, dco = None,None

    if initial is None: return None

    return self.go(initial+1)


if __name__ == "__main__" and sys.argv[1:]:

  va = sys.argv[1:]
  va.reverse()
  initial = None
  while va:
    fnUrl = va.pop()
    if fnUrl[-4:] == '.dat':
      if fnUrl == 'tyc2.dat': fnOut = 'catalog.dat'
      else                  : fnOut = fnUrl
      hg = httpgunzip(fnUrl, fnOut, initial=initial)
      hg.go(initial)
      initial = None
      continue

    initial = 0
      
"""
>>> f=urllib2.urlopen('ftp://cdsarc.u-strasbg.fr/cats/I/259/index.dat.gz')
while >>> while True:
...   l=f.read(1000)
...   if not l: break

"""
