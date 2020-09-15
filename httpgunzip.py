"""
httpgunzip.py - Get large GZIPped files from FTP server, including those
                that are GZIPped in pieces e.g. tyc2.dat as
                tyc2.dat.00.gz, tyc2.01.gz, ...

Usage:  python httpgunzip.py index.dat suppl_1.dat tyc2.dat ReadMe

N.B. This script was created to download Tycho-2 star catalog and index
     files from Univ. Strasburg in France; the httpgunzip class can do
     other constructs; see the constructor documentation.

"""
import re
import os
import sys
import zlib
### urllib2.urlopen is available in Python 3 as urllib.request.urlopen
try: import urllib2
except: import urllib.request as urllib2


########################################################################
def checkNNgz(fnIn, line):
  """
  Inputs:  base filename; line from FTP listing

  Return filename match from line matching blah.dat as blah.dat.gz or
  blah.dat.NN.gz, or return None

  """
  reObj = re.search( '(^|[\W])%s([.]\d\d)?([.]gz$)' % (fnIn,) ,line)
  if reObj:
    return ''.join( [i for i in reObj.groups()[1:] if not i is None])
  return None


########################################################################
### Default FTP URL is University of Strasburg, to get Tycho-2 data
### catalog and index files

urlPfxDefault = 'ftp://cdsarc.u-strasbg.fr/cats/I/259/'

class httpgunzip:
  """
  Class that implments, for one final file, the following pipe:

    [FTP URL GET] | [Decompression] | [Write file]

  Constructor:  httpgunzip(fnUrlFn, pathOut, urlPfx)

  Arguments:

    fnUrlFn:  basename of file, without .NN or .gz suffixes

    pathOut:  path of output file to be written

    urlPfx:  URL to directory containing file
             - defaults to Univ. Strasburg Tycho2 files

  N.B. File pathOut will be overwritten

  """
  def __init__(self, fnUrlFn, pathOut, urlPfx=urlPfxDefault):
    self.fnUrlFn = fnUrlFn
    self.pathOut = pathOut
    self.urlPfx = urlPfx
    ### Get lines of FTP listing
    ### In Python 2, .read() will return a string that can be .split()
    ### by a binary byte newline, but in Python 3 .read() will return
    ### binary bytes that be split with a binary byte newline but not
    ### with a string newline, so split the .read() data with a binary
    ### byte newline
    lines = urllib2.urlopen(self.urlPfx).read().split(b'\n')
    ### Use .decode() to convert binary bytes to strings in Python 3 ...
    try: self.lines = [line.decode('8859') for line in lines]
    ### ... in Python 2 the variable lines is already a list of strings
    except: self.lines = lines

  def go(self,testOnly=True):
    """
    Routine to decompress all .gz files matching self.fnUrlFn into
    output file

    """
    mode = 'wb'
    for line in self.lines:
      line = line.strip('\r')
      sfx = checkNNgz(self.fnUrlFn, line)
      if sfx is None: continue
      if mode == 'wb':
        sys.stderr.write( 'Starting %s ...\n' % (self.pathOut,) )
      
      url = os.path.join(self.urlPfx,self.fnUrlFn+sfx)
      sys.stderr.write( '  %s\n' % (url,) )
      sys.stderr.flush()

      if testOnly:
        mode = 'ab'
        continue

      fOut = open(self.pathOut,mode)
      mode = 'ab'

      dco = zlib.decompressobj(15+32)

      fZin = urllib2.urlopen(url)

      while True:
        dada = fZin.read( 50000 )
        if not dada: break
        fOut.write( dco.decompress(dada) )
 
      fOut.write( dco.flush() )
      fOut.close()
      fZin, dco = None,None

    return


########################################################################
### Default script is set up to get index.dat, tyc2.dat, suppl_1 and
### ReadMe files from Univ. Strasburg
########################################################################

if __name__ == "__main__" and sys.argv[1:]:

  ### Default path only tests, does not transfer data
  testOnly = True

  va = sys.argv[1:]
  va.reverse()

  while va:
    fnUrlFn = va.pop()

    if fnUrlFn == '--doit':
      testOnly = False
      continue

    if fnUrlFn == 'ReadMe':
      sys.stderr.write( 'Getting %s ...\n' % (fnUrlFn,) )
      if not testOnly:
        open( 'ReadMe', 'wb').write(
          urllib2.urlopen(os.path.join(urlPfxDefault,fnUrlFn)).read()
        )
      continue

    if fnUrlFn[-4:] == '.dat':
      if fnUrlFn == 'tyc2.dat': pathOut = 'catalog.dat'
      else                    : pathOut = fnUrlFn
      hg = httpgunzip(fnUrlFn, pathOut)
      hg.go(testOnly=testOnly)
      continue

