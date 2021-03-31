"""
Script to do two actions:

1) Download ESA/Gaia CSV data, filter by limiting magnitude,
   and convert to pickled data

2) Ingest pickled data into SQLite3 database with R-Tree table


Usage:

  python gaia.py [actions ...] [options ...]


Actions:

  ======================================================================
  getallgaia - get Gaia data from gzipped CSVs on @ESAGAIA web server
  ======================================================================

    Options:

      [--md5sumtxt=]csv/MD5SUM.txt - MD5SUM file of *.csv.gz names
                                     - if the value ends in MD5SUM.TXT,
                                       then the --md5sumtxt= prefix is
                                       not needed

        e.g. wget -nv  http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/MD5SUM.txt -P csv/

      [--overwritemd5] - overwrite MD5SUM file even if it already exists

      [--picklesdir=]gaiapickles - directory where *.gaiapickle files
                                   will be written
                                   - if the value ends in pickles, then
                                     the --...= prefix is not needed

      --nhex=N - default 1; start (16+N-1)/N processes using --select13

      [--select13=]0123456789abcdef - N.B. not normally supplied
                                           via the command line
                                    - See method get_all_gaia(...)

      [--maglimit=]18.0 - upper limit of any G or BP or RP magnitude
                          - the --...= prefix is not needed if the value
                            is a positive number
                          - if any of G or BP or RP magnitudesis lees
                            than or equal to this value, the star will
                            be included in the *.gaiapickle file

  ======================================================================
  buildsqlitedb - load data from gaiapickles/*.gaiapickle into SQLite DB
  ======================================================================

    Options:

      [--sqlitedb=]gaia.sqlite3 - directory where data from *.gaiapickle
                                  files will be written to SQLite DB
                                  - if the value ends in .sqlite3, then
                                     the --...= prefix is not needed
                                  - Light DB will be as specified
                                  - Heavy DB will be *_heavy.sqlite3

      --testfilestride=N - default 1; sub-sampling stride of .gaiapickle
                           files

      --teststride=N - default 1; sub-sampling stride of Gaia rows

      --nomd5sumtxt - do not use --md5sumtxt=... file to build
                      *.gaiapickle filenames; if this option is used,
                      then each gaia pickled file must have its
                      canonical name (GaiaSource_IDLO_IDHI.gaiapickle)


      [--picklesdir=]gaiapickles - see getallgaia options above
      [--md5sumtxt=]csv/MD5SUM.txt - see getallgaia options above
      [--overwritemd5] - see getallgaia options above

"""
import io
import re
try: re.Match
except: re.Match = type(re.compile('^').match(''))
import os
import sys
import glob
import gzip
import pickle
import sqlite3
try   : import urllib3 ; use_urllib3 = True
except: import urllib2_util ; use_urllib3 = False
import traceback

try:  assert isinstance(default_scols,list)
except:
  ### Globals
  ### - Environment variables to control behavior:  debugging; progress;
  ###     whether to force update of existing pickled data
  do_debug = 'DEBUG' in os.environ
  csv_progress = 'CSV_PROGRESS' in os.environ
  do_progress = 'DO_PROGRESS' in os.environ
  no_progress = not do_progress
  skip_update_pickles = not ('FORCE_UPDATE_PICKLES' in os.environ)

  ### - CSV-to-pickle selection criteria:  13th character in MD5
  ###     hexdigest; magnitude limit; column names
  hexmd5all = '0123456789abcdef'.lower()
  default_maglimit = 18.0
  default_scols = [s.strip() for s in """
designation
ra
dec

parallax
pmra
pmdec
phot_g_mean_mag
phot_bp_mean_mag
phot_rp_mean_mag

ra_error
dec_error
parallax_error
pmra_error
pmdec_error
ra_dec_corr
ra_parallax_corr
ra_pmra_corr
ra_pmdec_corr
dec_parallax_corr
dec_pmra_corr
dec_pmdec_corr
parallax_pmra_corr
parallax_pmdec_corr
pmra_pmdec_corr
""".strip().split() if s]

  ### - Shorthand for columns
  (kSourceid,kRa,kDec
  ,kParallax,kPmra,kPmdec
  ,kPhot_g_mean_mag,kPhot_bp_mean_mag,kPhot_rp_mean_mag
  ,kRa_error,kDec_error,kParallax_error
  ,kPmra_error,kPmdec_error,kRa_dec_corr
  ,kRa_parallax_corr,kRa_pmra_corr,kRa_pmdec_corr
  ,kDec_parallax_corr,kDec_pmra_corr,kDec_pmdec_corr
  ,kParallax_pmra_corr,kParallax_pmdec_corr,kPmra_pmdec_corr
  ,) = default_scols

  ### - Regex for parsing either MD5 hexadecimal checksums and filenames
  ###   in MD5SUM.txt, or just filenames
  rgxmd5 = re.compile('^(([\\dA-Fa-f]{32}) +)?(GaiaSource_(\\d+)_\\d+)[.](csv.gz|gaiapickle)\\r?$')

  def rgxparse(md5_csvgzpfx):
    """
For one string

  MD5checksum  GaiaSouce_N_M.{csv.gz,gaiapickle}

If REGEX rgxmd5 matches, return triple of

  int(N),'MD5checksum','GaiaSource_N_M'

Else return False

N.B. presence of MD5checksum is optional, in which case
     'MD5checksum' will be None

Parses either, from MD5SUM.txt file,

  MD5checksum  GaiaSource_N_M.csv.gz

OR, from glob.glob(...),

  GaiaSource_N_M.gaiapickle

"""
    match = rgxmd5.match(md5_csvgzpfx)
    if isinstance(match,re.Match):
      groups = match.groups()
      ### Return N as integer, checksum, csvgzpfx
      return int(groups[3]),groups[1],groups[2]
    return False


########################################################################
class GAIACSV(object):
  """
Encapsulate all data from one ESA/Gaia Data Release 2 CSV file

Attributes; asterisks indicate __init__ arguments
  .hexmd5*       - MD5 checksum from MD5SUM.txt
  .csvgzpfx*     - CSV GZIP filname prefix, GaiaSource_LoSrcID_HiSrcI
  .maglimit*     - Upper magnitude limit of retained rows
  .last_modified - Last-Modified time from EXA Gaia web server of CSV GZ
  .pickled       - True if current data have been pickled
  .rows          - List of Rows of retained row data, each row is a list
  .column_names  - List inidicating order of data in each row
  .filtered_rows - Count of rows excluded with magnitude > .maglimit

Methods:  url_get; pickle; get_httppool; __repr__.

Static attributes:  csv_url_hostname; csv_url_dir; HTTPPOOL.

"""
  csv_url_hostname = 'cdn.gea.esac.esa.int'
  csv_url_dir = '/Gaia/gdr2/gaia_source/csv'
  HTTPPOOL = None

  def get_httppool(self):
    """Retrieve or create singleton GAIACSV.HTTPPOOL"""

    if use_urllib3:
      try: assert isinstance(GAIACSV.HTTPPOOL,urllib3.HTTPConnectionPool)
      except:
        GAIACSV.HTTPPOOL = urllib3.HTTPConnectionPool(GAIACSV.csv_url_hostname
                                                     ,maxsize=1
                                                     ,timeout=urllib3.util.Timeout(10)
                                                     )
      return GAIACSV.HTTPPOOL

    return urllib2_util.URLLIB2_WRAPPER(GAIACSV.csv_url_hostname
                                       ,maxsize=1
                                       ,timeout=10
                                       )

  def __init__(self,hexmd5,csvgzpfx,maglimit):
    """Store MD5 checksum, filename prefix, magnitude limit; no data"""

    (self.hexmd5,self.csvgzpfx,self.last_modified,self.maglimit
    ,) = hexmd5,csvgzpfx,'',maglimit
    self.pickled = False
    return

  def url_get(self):
    """Retrieve and parse data CSV GZ file"""

    ### Build CSV GZ URL, retrieve last-modified time from HEAD data
    url = '{0}/{1}.csv.gz'.format(GAIACSV.csv_url_dir,self.csvgzpfx)
    csvhead = self.get_httppool().request('HEAD',url)
    try: last_modified = csvhead.headers['Last-Modified']
    except: last_modified = csvhead.headers['last-modified']
    del csvhead

    if csv_progress:
      ### Write CSV info to STDOUT if CSV progress info was called for
      sys.stdout.write('{0}\n'.format(dict(hdrlastmod=last_modified,last_modified=self.last_modified,url=url)))
      sys.stdout.flush()

    if self.last_modified != last_modified or self.pickled == False:
      ### Retrieve and parse CSV GZ data if either last-modified has
      ### changed or data have not been pickled
      self.pickled = False

      ### Put gzipped data into byte stream
      csvreq = self.get_httppool().request('GET',url)
      gzbytes = io.BytesIO(csvreq.data)
      del csvreq

      ### Unzip & parse CSV data, delete byte stream, set .last_modified
      ###with gzip.open(gzbytes,'rb') as funzipcsv:
      with gzip.GzipFile(fileobj=gzbytes) as funzipcsv:
        (self.rows,self.column_names,self.filtered_rows
        ,) = gaia_read_csv(funzipcsv
                          ,scols=default_scols
                          ,maglimit=self.maglimit
                          )
      del gzbytes
      self.last_modified = last_modified

    return self

  def pickle(self,gaia_pickle_dir):
    """Pickle instance"""

    ### Do not re-pickle; do not pickle if there are no .rows yet
    if self.pickled: return self
    if not hasattr(self,'rows'): return self

    ### Build path to pickle file
    picklepath = os.path.join(gaia_pickle_dir
                             ,'{0}.gaiapickle'.format(self.csvgzpfx)
                             )

    ### Pickle with .pickled assigned as True; reverse if pickling fails
    try:
      self.pickled = True
      with open(picklepath,'wb') as fpickle: pickle.dump(self,fpickle)
    except:
      self.pickled = False
      raise

  def __repr__(self):
    """Use vars(self) with row_count replacing .rows"""
    d = dict(row_count=len(self.rows))
    d.update(vars(self))
    del d['rows']
    return str(d)

  ### End of class GAIACSV(object):
  ######################################################################


########################################################################
def gaia_read_pickle(picklepath):
  """Wrapper for pickle.load"""
  with open(picklepath,'rb') as fpickle: gp = gaia.pickle.load(fpickle)
  return gp


########################################################################
def flt(s):
  """Parse CSVed strings; return None for exceptions"""
  try   : return float(s)
  except: return None


########################################################################
def gaia_read_csv(fcsv,scols=default_scols,maglimit=default_maglimit):
  """Read and parse CSV data into rows of selected columns; filter by
magnitude limit; first selected column must be "designation"

Return:  (rows, selected column headers, number of rows filtered out,)
"""

  ### Initialize empty list of rows, filtered row count.  Parse first
  ### line a CSVed column headers, get offsets of selected columns,
  ### get offsets columns that end in _mean_mag i.e. magnitudes, get
  ### offset of first column (designation)
  rows,filtered_rows = list(),0
  hdr = fcsv.readline().decode('8859').strip().split(',')
  icols = [hdr.index(s) for s in scols]
  icolmags = [hdr.index(s) for s in scols if s.lower().endswith('_mean_mag')]
  icol0 = icols[0]

  ### Loop over CSV rows
  for row in fcsv:

    ### Parse a row of data; raise exception unless from Data Release 2
    toks = row.decode('8859').strip().split(',')
    dsgntn = toks[icol0]
    assert 'Gaia DR2 ' == dsgntn[:9]

    ### If magnitudes are included, filter out any rows that do not have
    ### any magnitude less than the magnitude limit
    if icolmags:
      magskip = True
      for icolmag in icolmags:
        if not toks[icolmag]: continue
        mag = flt(toks[icolmag])
        if None is mag: continue
        if mag > maglimit: continue
        magskip = False
        break
      if magskip:
        filtered_rows += 1
        continue

    ### Append non-filtered row to list of rows
    rows.append([int(dsgntn[9:])] + [flt(toks[i]) for i in icols[1:]])

  return rows,scols,filtered_rows


########################################################################
def my_makedirs(subdir,exist_ok=False):
  try:
    try                  : os.makedirs(subdir,exist_ok=exist_ok)
    except TypeError as e: os.makedirs(subdir)
  except OSError as e: pass


########################################################################
def get_some_gaia(argv):
  """Read MD5SUM.txt to get MD% checksums and basenames of CSV GZ files,
Select and get files, converting each CSV GZ into a .gaiapickle file

Method argument argv is the same as sys.argv[1:], with a string of
one or more hexadecimal characters prepended

"""

  ### Default input parameters
  md5sum_path = 'csv/MD5SUM.txt'
  gaia_pickle_dir = 'gaiapickles'
  hextoget = hexmd5all
  maglimit = default_maglimit

  ### Process argument list
  for arg in argv:

    if not len(arg): continue

    if arg.startswith('--md5sumtxt='):
      md5sum_path = arg[12:]
      continue
    if arg.endswith('MD5SUM.txt'):
      md5sum_path = arg
      continue

    if arg.startswith('--picklesdir='):
      gaia_pickle_dir = arg[13:]
      continue
    if arg.endswith('pickles'):
      gaia_pickle_dir = arg
      continue

    if len(arg) == len([None
                        for s in arg.lower()
                        if s in hexmd5all
                       ]):
      hextoget = arg.lower()
      continue
    if arg.startswith('--select13='):
      hextoget = arg[11:].lower()
      continue

    try:
      new_maglimit = float(arg)
      assert new_maglimit > 10.0
      maglimit = new_maglimit
    except: pass
    if arg.startswith('--maglimit='):
      maglimit = float(arg[11:])
      continue

  ### Create destination directory for pickle files
  my_makedirs(gaia_pickle_dir,exist_ok=True)

  ### Do not check for the existence of MD5SUM file here as there may be
  ### multiple forks of this code running simultaneously

  with open(md5sum_path,'r') as fmd5:

    n = 0
    for triple in map(rgxparse,fmd5):

      if not triple: continue
      hexmd5,csvgzpfx = triple[1:3]
      if not (hexmd5[13].lower() in hextoget): continue

      try:
        picklepath = os.path.join(gaia_pickle_dir
                                 ,'{0}.gaiapickle'.format(csvgzpfx)
                                 )
        with open(picklepath, 'rb') as fpickle:
          if skip_update_pickles:
            if do_debug: sys.stderr.write('Skipping overwrite of {0}\n'.format(picklepath))
            continue
          gaiacsv = pickle.load(fpickle)
      except:
        if do_debug: traceback.print_exc()
        gaiacsv = gaia.GAIACSV(hexmd5,csvgzpfx,maglimit)

      gaiacsv.url_get().pickle(gaia_pickle_dir)

      if no_progress or (n%999): continue
      sys.stderr.write('.')
      sys.stderr.flush()
      n += 1


########################################################################
def get_all_gaia(argv):
  """Wrapper for forks of calls to method get_some_gaia(...)

Each fork will have a unique set hexadecimal specifiers that it will
use to determine which Gaia CSV GZ files to retrieve:  if character at
offset 13 in the 32-character MD5 hexadecimal checksum is in the set of
hex specifiers passed to the forked process, then the file will be
retrieved by that process.

Method argument is typically sys.argv[1:]

"""

  ### Count of hex specifiers, out of hexmd5all, for each fork to search
  nhex = ([1] + [int(arg[7:])
                 for arg in argv
                 if arg.startswith('--nhex=')
                ]).pop()
  assert nhex > 0

  ### Start with full string of hex specifiers, and empty dict of PIDs
  hexmd5left = hexmd5all
  pids = dict()

  ### Ensure MD5SUM file exists
  md5sum_get(argv=argv)

  while hexmd5left:
    ### Select first nhex hex specifers, remove from string
    hextoget = hexmd5left[:nhex]
    hexmd5left = hexmd5left[nhex:]

    pid = os.fork()
    if 0==pid:
      ### The forked child process calls get_some_gaia and exits
      get_some_gaia([hextoget]+argv)
      exit(0)

    ### The parent process saves the PID of child in dict pids
    pids[pid] = hextoget
    sys.stderr.write('{0}\n'.format(dict(Started=pid,hextoget=hextoget)))
    sys.stderr.flush()

    ### Determine whether a hex specifier, --select13=... or e.g. 89abc
    ### is in argv; if it is, then it will supersede every selection by
    ### this while loop, so break out of this loop on the first pass
    if [None for s in argv if s.startswith('--select13=')]: break
    if [None for s in argv if s and [None for ss in s if s in hexmd5all]]:
      break

  while len(pids):
    ### Wait for all forked children to complete
    fpid,fstatus = os.wait()
    sys.stderr.write('{0}\n'.format(dict(Finished=fpid,hextoget=pids[fpid],status=fstatus)))
    sys.stderr.flush()
    del pids[fpid]


class GAIAROW(object):
  """N.B. this class is no longer used

Encapsulate one row of EXA GAIA CSV files

Usage:

  gaiarow = gaia.GAIAROW(n,dict(zip(gcsv.column_names,gcsv.rows[n])))

  - where gcsv is a GAIACSV instance

"""

  def __init__(self,idoffset,dikt):
    """Load zip(gcsv.column_names,gcsv.rows[idoffset])"""

    self.idoffset = idoffset

    g = dikt.get
    self.source_id = g(kSourceid,None)
    self.ra = g(kRa,None)
    self.dec = g(kDec,None)
    self.parallax = g(kParallax,None)
    self.pmra = g(kPmra,None)
    self.pmdec = g(kPmdec,None)
    self.phot_g = g(kPhot_g_mean_mag,None)
    self.phot_bp = g(kPhot_bp_mean_mag,None)
    self.phot_rp = g(kPhot_rp_mean_mag,None)
    self.ra_err = g(kRa_error,None)
    self.dec_err = g(kDec_error,None)
    self.par_err = g(kParallax_error,None)
    self.pmra_err = g(kPmra_error,None)
    self.pmdec_err = g(kPmdec_error,None)
    self.ra_dec_corr = g(kRa_dec_corr,None)
    self.ra_par_corr = g(kRa_parallax_corr,None)
    self.ra_pmra_corr = g(kRa_pmra_corr,None)
    self.ra_pmdec_corr = g(kRa_pmdec_corr,None)
    self.dec_par_corr = g(kDec_parallax_corr,None)
    self.dec_pmra_corr = g(kDec_pmra_corr,None)
    self.dec_pmdec_corr = g(kDec_pmdec_corr,None)
    self.parallax_pmra_corr = g(kParallax_pmra_corr,None)
    self.parallax_pmdec_corr = g(kParallax_pmdec_corr,None)
    self.pmra_pmdec_corr = g(kPmra_pmdec_corr,None)

  def set_hilo(self):
    """Set high and low magnitudes from G and/or BP and or RP bands"""

    self.ralo = self.rahi = self.ra
    self.declo = self.dechi = self.dec
    mags = [mag
            for mag in [self.phot_g,self.phot_bp,sefl.phot_rp]
            if not (mag is None)
           ]
    self.lomag = min(mags)
    self.himag = max(mags)

    return self


########################################################################
class MPITER:
  """Create iterator over stars for a set of .gaiapickle files

The iterator will return a row of star data per .next/.__next__ call

The rows returned will be ordered by Gaia source_id, as extracted from
the designation column in the CSV data by the GAIACSV class

The intializer arguments include options for non-unit striding over
the list of .gaiapickle files and over rows within .gaiapickle files

The gaia_pickle_dir is required, and the default is to use glob.glob to
find all GaiaSource_idlow_idhigh.gaiapickle files in that directory.  By
specifying, or using the default, path to MD5SUM.txt and also passing
keyword argument [,no_md5sumtxt=False], an MD5SUM.txt file can be used
to select the .gaiapickle files.

The .gaiapickle file prefix will be sorted by idlow using the
rgxparse(...) method of this [gaia] module.

"""
  def __init__(self
              ,md5sum_path='csv/MD5SUM.txt'
              ,gaia_pickle_dir='gaiapickles'
              ,stride=1
              ,filestride=1
              ,no_md5sumtxt=True
              ,overwritemd5=False
              ):
    """Start iterator by locating and sorting the list of .gaiapickle
filepathss

"""

    ### Save input parameters
    self.stride,self.gaia_pickle_dir = stride,gaia_pickle_dir

    ### Find or parse all .gaiapickle files
    if no_md5sumtxt:
      wildcard = 'GaiaSource_*_*.gaiapickle'
      wcpath = os.path.join(gaia_pickle_dir,wildcard)
      lt = [rgxparse(os.path.basename(s)) for s in glob.glob(wcpath)]
    else:
      ### Ensure MD5SUM file exists, then open it
      md5sum_get(argv=['--md5sumtxt={0}'.format(md5sum_path)
                      ,overwritemd5 and '--overwritemd5' or ''
                      ])
      with open(md5sum_path,'r') as fmd5:
        lt = [m for m in [rgxparse(s.strip()) for s in fmd5] if m]

    ### Sort the list of files by source_id, as returned by rgxparse
    lt.sort(reverse=True)

    ### Extract the sorted prefixes by stride, and start with empty rows
    ### and an offset of -1
    self.pfxs = [triple[-1] for triple in lt[::filestride]]
    self.rows = []
    self.idoffset = -1

  def next(self):
    """Get the next row in this iterator"""
    if not self.rows:
      ### Stop iteration when there are no more file prefixes
      if not self.pfxs: raise StopIteration
      ### Ingest the next .gaiapickle file's data, stride and sort rows
      basename = '{0}.gaiapickle'.format(self.pfxs.pop())
      picklepath = os.path.join(self.gaia_pickle_dir,basename)
      gp = gaia_read_pickle(picklepath)
      self.gpcols = gp.column_names[::]
      ### - Find any default column names that are not in these data
      self.dt_none_cols = dict([(s,None,)
                                for s in default_scols
                                if not (s in self.gpcols)
                               ])

      ### - Get any magnitude columns
      self.mag_cols = [s for s in self.gpcols if s.endswith('_mean_mag')]

      self.rows = gp.rows[::self.stride]
      self.rows.sort(reverse=True)

    ### Increment offset, make dictionary of next row via .pop()
    self.idoffset += 1
    rtn = dict(idoffset=self.idoffset)
    rtn.update(self.dt_none_cols)
    rtn.update(zip(self.gpcols,self.rows.pop()))

    ### - Find low and high magnitudes, excluding None values
    if 1 < len(self.mag_cols):
      mags = [m for m in [rtn[s] for s in self.mag_cols] if not (None is m)]
      rtn.update(dict(lomag=min(mags),himag=max(mags)))
    else:
      mag = rtn[self.dt_none_cols[0]]
      rtn.update(dict(lomag=mag,himag=mag))

    return rtn

  ### Iterator methods; .__next__ (Python 3) duplicates .next (Python 2)
  def __next__(self): return self.next()
  def __iter__(self): return self


########################################################################
def build_sqlitedb(argv):
  """Ingest data from .gaiapickle files to SQLIte3 databases

Light database will contain two tables:  R-Tree table of ra, dec, and
magnitude; table of parallex and proper motion.

Heavy database will contain errors and correlations

All tables will have the same number of rows, one per star.  All tables
will have an integer offset column as the primary key, starting at 0,
which can be used for JOINs between tables

Method argument is typically sys.argv[1:]

"""

  ### Initialize parameters
  md5sum_path = 'csv/MD5SUM.txt'
  gaia_pickle_dir = 'gaiapickles'
  sqlitedb = 'gaia.sqlite3'
  stride = 1
  filestride = 1
  no_md5sumtxt = False
  overwritemd5 = False

  ### Parse argument list items
  for arg in argv:

    if not len(arg): continue

    if arg == '--overwritemd5':
      overwritemd5 = True
      continue

    if arg == '--nomd5sumtxt':
      no_md5sumtxt = True
      continue

    if arg.startswith('--teststride='):
      stride = int(arg[13:])
      continue

    if arg.startswith('--testfilestride='):
      filestride = int(arg[17:])
      continue

    if arg.startswith('--sqlitedb='):
      sqlitedb = arg[11:]
      continue

    if arg.startswith('--md5sumtxt='):
      no_md5sumtxt = False
      md5sum_path = arg[12:]
      continue
    if arg.endswith('MD5SUM.txt'):
      no_md5sumtxt = False
      md5sum_path = arg
      continue

    if arg.startswith('--picklesdir='):
      gaia_pickle_dir = arg[13:]
      continue
    if arg.endswith('pickles'):
      gaia_pickle_dir = arg
      continue

  ### Check light and heavy database names, and connect to them
  assert sqlitedb.endswith('.sqlite3')
  assert not sqlitedb.endswith('_heavy.sqlite3')

  cn = sqlite3.connect(sqlitedb)
  cnheavy = sqlite3.connect('{0}_heavy.sqlite3'.format(sqlitedb[:-8]))

  cu,cuheavy = cn.cursor(),cnheavy.cursor()

  ### Initial SQLite setup
  cu.execute("""PRAGMA synchronous = OFF""")
  cu.execute("""PRAGMA journal_mode = MEMORY""")
  cuheavy.execute("""PRAGMA synchronous = OFF""")
  cuheavy.execute("""PRAGMA journal_mode = MEMORY""")

  cu.execute("""drop table if exists gaiartree""")
  cu.execute("""drop table if exists gaialight""")
  cuheavy.execute("""drop table if exists gaiaheavy""")
  cn.commit()
  cnheavy.commit()

  cu.execute("""
CREATE VIRTUAL TABLE gaiartree using rtree(idoffset,ralo,rahi,declo,dechi,lomag,himag);"""
)
  cu.execute("""
CREATE TABLE gaialight
(idoffset INT PRIMARY KEY
,ra REAL NOT NULL
,dec REAL NOT NULL
,parallax REAL DEFAULT NULL
,pmra REAL DEFAULT NULL
,pmdec REAL DEFAULT NULL
,phot_g_mean_mag REAL NOT NULL
,phot_bp_mean_mag REAL DEFAULT NULL
,phot_rp_mean_mag REAL DEFAULT NULL
);""")

  cuheavy.execute("""
CREATE TABLE gaiaheavy
(idoffset INT PRIMARY KEY
,source_id BIGINT NOT NULL
,ra_error REAL DEFAULT NULL
,dec_error REAL DEFAULT NULL
,parallax_error REAL DEFAULT NULL
,pmra_error REAL DEFAULT NULL
,pmdec_error REAL DEFAULT NULL
,ra_dec_corr REAL DEFAULT NULL
,ra_parallax_corr REAL DEFAULT NULL
,ra_pmra_corr REAL DEFAULT NULL
,ra_pmdec_corr REAL DEFAULT NULL
,dec_parallax_corr REAL DEFAULT NULL
,dec_pmra_corr REAL DEFAULT NULL
,dec_pmdec_corr REAL DEFAULT NULL
,parallax_pmra_corr REAL DEFAULT NULL
,parallax_pmdec_corr REAL DEFAULT NULL
,pmra_pmdec_corr REAL DEFAULT NULL
);""")

  cn.commit()
  cnheavy.commit()

  ### Initialize iterator for stars in .gaiapickle files
  multipickles_iter = MPITER(md5sum_path=md5sum_path
                            ,gaia_pickle_dir=gaia_pickle_dir
                            ,stride=stride
                            ,filestride=filestride
                            ,no_md5sumtxt=no_md5sumtxt
                            ,overwritemd5=overwritemd5
                            )

  ### SQL INSERT statement for the three tables
  ### N.B designation in .gaiapickle file is source_id from .gaiapickle
  sqlRtree = "INSERT INTO gaiartree VALUES (:idoffset,:ra,:ra,:dec,:dec,:lomag,:himag)"
  sqlLight = "INSERT INTO gaialight VALUES (:idoffset,:ra,:dec,:parallax,:pmra,:pmdec,:phot_g_mean_mag,:phot_rp_mean_mag,:phot_rp_mean_mag)"
  sqlHeavy = "INSERT INTO gaiaheavy VALUES (:idoffset,:designation,:ra_error,:dec_error,:parallax_error,:pmra_error,:pmdec_error,:ra_dec_corr,:ra_parallax_corr,:ra_pmra_corr,:ra_pmdec_corr,:dec_parallax_corr,:dec_pmra_corr,:dec_pmdec_corr,:parallax_pmra_corr,:parallax_pmdec_corr,:pmra_pmdec_corr)"

  ### Initialze list of dicts and its length, L
  dts = list()
  L = 0

  ### Loop over stars in all .gaiapickle files,
  for gaiarow in multipickles_iter:

    if 0==L:
      ### Start transactions when list is empty
      cu.execute("""BEGIN TRANSACTION""")
      cuheavy.execute("""BEGIN TRANSACTION""")

    ### Append row and increment counter
    dts.append(gaiarow)
    L += 1

    ### Show progress if so specified
    idoffset = gaiarow['idoffset']
    if do_progress and not (16777215&idoffset):
      sys.stderr.write('{0}/'.format(idoffset))
      sys.stderr.flush()

    ### - Do INSERTs and COMMITs every 16,384 rows
    if 16383==L:
      cu.executemany(sqlRtree,dts)
      cu.executemany(sqlLight,dts)
      cuheavy.executemany(sqlHeavy,dts)
      cn.commit()
      cnheavy.commit()
      ### Reset for next 16k rows
      dts = list()
      L = 0

  if 0<L:
    ### - Do any leftover INSERTs and COMMITs
    cu.executemany(sqlRtree,dts)
    cu.executemany(sqlLight,dts)
    cuheavy.executemany(sqlHeavy,dts)
    cn.commit()
    cnheavy.commit()

  return gaiarow


########################################################################
def md5sum_get(argv=sys.argv[1:]):
  """Retrieve MD5SUM.txt from Gaia DR2 and write data as local file

Input argument is a list that defaults to sys.argv[1:]


"""

  ### Defaults:  path to write; whether to overwrite
  md5sum_path = 'csv/MD5SUM.txt'
  overwritemd5 = False

  for arg in argv:

    if not arg: continue

    if '--overwritemd5' == arg:
      overwritemd5 = True
      continue

    if arg.startswith('--md5sumtxt='):
      md5sum_path = arg[12:]
      continue
    if arg.endswith('MD5SUM.txt'):
      md5sum_path = arg
      continue

  ### Do not overwrite existing file if not asked to do so
  if os.path.isfile(md5sum_path) and not overwritemd5: return

  ### Get file from web server, write local copy
  url = '{0}/MD5SUM.txt'.format(GAIACSV.csv_url_dir)
  gcsv = GAIACSV(None,None,None)
  md5req =  gcsv.get_httppool().request('GET',url)
  my_makedirs(os.path.dirname(md5sum_path),exist_ok=True)
  with open(md5sum_path,'wb') as fmd5sumtxt:
    fmd5sumtxt.write(md5req.data)
  md5req.close()
  del md5req
  del gcsv
  if do_progress: sys.stderr.write('Wrote {0}\n'.format(md5sum_path))


########################################################################
### Import gaia module into itself so pickling works better
try   : gaia
except: import gaia


########################################################################
########################################################################
### Command-line interface
if "__main__" == __name__ and sys.argv[1:]:

  ### Process command-line arguments
  tmpargv = sys.argv[1:]
  while tmpargv:

    tmparg = tmpargv.pop(0)

    ### Respond to action arguments, otherwise break and exit

    if 'getallgaia' == tmparg:
      get_all_gaia(tmpargv)

    elif 'buildsqlitedb' == tmparg:
      sys.stderr.write('{0}\n'.format(build_sqlitedb(tmpargv)))

    else:
      break
### End of module gaia
########################################################################
