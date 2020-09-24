import os
import sys
import struct
import socket
import signal
import sqlite3
import traceback as tb

do_debug = 'DEBUG' in os.environ
do_extra_debug = 'EXTRA_DEBUG' in os.environ
do_data_debug = 'DATA_DEBUG' in os.environ
DEFAULT_PORT = int(''.join(['{0:01x}'.format(15&ord(c)) for c in 'gaia']),16)

def handleSIGCHLD(*args):
  while True:
    try:
      pid,status = os.waitpid(-1,os.WNOHANG)
      if 0==pid and 0==status: raise ChildProcessError
      if do_debug:
        sys.stderr.write("Child [{0}] exited with status [{1}]\n".format(pid,status))
        sys.stderr.flush()
    except ChildProcessError as e:
      return

########################################################################
def do_main(gaialightdb,gaiahostname='',port=DEFAULT_PORT):
  server_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
  server_sock.bind((gaiahostname,port,))
  server_sock.listen(50)
  signal.signal(signal.SIGCHLD, handleSIGCHLD)
  try:
    while True:
      ### Create (accept) client socket
      client_sock,client_addr_info = server_sock.accept()
      ### Fork a child
      pid = os.fork()
      if 0 == pid:
        ### Child does the work and exits
        try:
          get_gaia_data(gaialightdb,client_sock,client_addr_info)
        except:
          if do_extra_debug:
            tb.print_exc()
            sys.stderr.flush()
        exit(0)
      ### Parent closes its copy of the client socket & continues
      client_sock.close()
      if do_debug:
        sys.stderr.write("Child {0} forked\n".format(pid))
        sys.stderr.flush()
  except SystemExit as e: pass
  except:
    if do_extra_debug:
      tb.print_exc()
      sys.stderr.flush()
    try:
      if do_debug:
        sys.stderr.write('GaiaLib server closing\n')
        sys.stderr.flush()
      server.close()
    except:pass


########################################################################
def get_gaia_data(gaialightdb,client_sock,client_addr_info):
  """
Method called/performed by child process forked by server parent process
in do_main(...) method above.

This method receives the request from the client, which defines
i) the byte order used by the client and the data groups requested for
   each star:
  - Base group:  RA; DEC; G Magnitude.
  - Light group:  parallax; proper motion; BP & RP magnitudes.
  - Heavy group:  source_id; errors; covariances.
  The Base group is required; the Light and Heavy groups are optional.
ii) the magnitude, RA, and Dec limits of the request

Socket client_socket receives 6 doubles, as 48 bytes, from client:

  0) An odd negative integer, as a double, between -1.0 & -7.0 inclusive
  0.1) This value sets both client byte order and the data requested:
  0.1.0) -1.0:  Base group only
  0.1.1) -3.0:  Base and Light groups
  0.1.2) -5.0:  Base and Heavy groups
  0.1.3) -7.0:  Base and Light and Heavy groups

  1) The maximum magnitude
  2) The minimum RA
  3) The maximum RA
  4) The minimum Dec
  5) The maximum Dec

"""

  """
  querydict is a dictionary
  - Keys are the first double of the 6-double request
  - Values are 3-tuples:
    0) Whether Light group data are requested
    1) Whether Heavy group data are requested
    2) The query string
"""

  querydict = { -1.0:(False,False,"""
SELECT gaiartree.idoffset
     , gaialight.ra
     , gaialight.dec
     , gaialight.phot_g_mean_mag

FROM gaiartree
INNER JOIN gaialight
ON gaiartree.idoffset=gaialight.idoffset

WHERE gaiartree.lomag<:himag
  AND gaiartree.rahi>:ralo
  AND gaiartree.ralo<:rahi
  AND gaiartree.dechi>:declo
  AND gaiartree.declo<:dechi
  AND gaialight.phot_g_mean_mag<:himag

ORDER BY gaialight.phot_g_mean_mag ASC
;""",)

, -3.0:(True,False,"""
SELECT gaiartree.idoffset
     , gaialight.ra
     , gaialight.dec
     , gaialight.phot_g_mean_mag

     , gaialight.parallax, gaialight.pmra, gaialight.pmdec
     , gaialight.phot_bp_mean_mag, gaialight.phot_bp_mean_mag

FROM gaiartree
INNER JOIN gaialight
ON gaiartree.idoffset=gaialight.idoffset

WHERE gaiartree.lomag<:himag
  AND gaiartree.rahi>:ralo
  AND gaiartree.ralo<:rahi
  AND gaiartree.dechi>:declo
  AND gaiartree.declo<:dechi
  AND gaialight.phot_g_mean_mag<:himag

ORDER BY gaialight.phot_g_mean_mag ASC
;""",)

, -5.0:(False,True,"""
SELECT gaiartree.idoffset
     , gaialight.ra
     , gaialight.dec
     , gaialight.phot_g_mean_mag

     , heavytbl.source_id
     , heavytbl.ra_error, heavytbl.dec_error, heavytbl.parallax_error, heavytbl.pmra_error, heavytbl.pmdec_error
     , heavytbl.ra_dec_corr, heavytbl.ra_parallax_corr, heavytbl.ra_pmra_corr, heavytbl.ra_pmdec_corr
     , heavytbl.dec_parallax_corr, heavytbl.dec_pmra_corr, heavytbl.dec_pmdec_corr
     , heavytbl.parallax_pmra_corr, heavytbl.parallax_pmdec_corr
     , heavytbl.pmra_pmdec_corr

FROM gaiartree
INNER JOIN heavydb.gaiaheavy heavytbl
ON gaiartree.idoffset=heavytbl.idoffset

WHERE gaiartree.lomag<:himag
  AND gaiartree.rahi>:ralo
  AND gaiartree.ralo<:rahi
  AND gaiartree.dechi>:declo
  AND gaiartree.declo<:dechi
  AND gaialight.phot_g_mean_mag<:himag

ORDER BY gaialight.phot_g_mean_mag ASC
;""",)

, -7.0:(True,True,"""
SELECT gaiartree.idoffset
     , gaialight.ra
     , gaialight.dec
     , gaialight.phot_g_mean_mag

     , gaialight.parallax, gaialight.pmra, gaialight.pmdec
     , gaialight.phot_bp_mean_mag, gaialight.phot_bp_mean_mag

     , heavytbl.source_id
     , heavytbl.ra_error, heavytbl.dec_error, heavytbl.parallax_error, heavytbl.pmra_error, heavytbl.pmdec_error
     , heavytbl.ra_dec_corr, heavytbl.ra_parallax_corr, heavytbl.ra_pmra_corr, heavytbl.ra_pmdec_corr
     , heavytbl.dec_parallax_corr, heavytbl.dec_pmra_corr, heavytbl.dec_pmdec_corr
     , heavytbl.parallax_pmra_corr, heavytbl.parallax_pmdec_corr
     , heavytbl.pmra_pmdec_corr

FROM gaiartree
INNER JOIN gaialight
ON gaiartree.idoffset=gaialight.idoffset
INNER JOIN heavydb.gaiaheavy heavytbl

ON gaiartree.idoffset=heavytbl.idoffset
WHERE gaiartree.lomag<:himag
  AND gaiartree.rahi>:ralo
  AND gaiartree.ralo<:rahi
  AND gaiartree.dechi>:declo
  AND gaiartree.declo<:dechi
  AND gaialight.phot_g_mean_mag<:himag

ORDER BY gaialight.phot_g_mean_mag ASC
;""")
}

  ### Receive 48 bytes of request from client
  received,L = b'',0
  while L < 48:
    new_recv = client_sock.recv(48 - L)
    assert len(new_recv) > 0
    received += new_recv
    L = len(received)

  ### Parse those 48 bytes into doubles, while also detecting byte order
  ### - N.B. this will only correct for byte order, not floating format
  ###         e.g. IEEE-754 vs. IBM vs. VAX/VMS vs. etc.
  try:
    ### - Assume LSB first ...
    byteorder = '<'
    (negint,himag,ralo,rahi,declo,dechi,) = inputs = struct.unpack(byteorder + '6d',received)
    ### This will throw a KeyError exception if data are MSB:
    get_light, get_heavy, query = querydict[negint]
  except:
    ### - Assume MSB if LSB failed ...
    byteorder = '>'
    (negint,himag,ralo,rahi,declo,dechi,) = inputs = struct.unpack(byteorder + '6d',received)
    get_light, get_heavy, query = querydict[negint]

  if do_debug:
    sys.stderr.write('Child Inputs:  {0}\n'.format(inputs))
    if do_extra_debug:
      sys.stderr.write('(get_light,get_heavy,)={0}\n'.format((get_light,get_heavy,)))
      sys.stderr.write('### QUERY:\n{0}\n### END QUERY\n'.format(query))

  ### Set up database(s)
  cn = sqlite3.connect(gaialightdb)
  cu = cn.cursor()

  if get_heavy:
    assert gaialightdb.endswith('.sqlite3')
    assert not ("'" in gaialightdb)
    attach_query = """ATTACH DATABASE '{0}_heavy.sqlite3' as heavydb ;""".format(gaialightdb[:-8])
    if do_debug and do_extra_debug:
      sys.stderr.write('### QUERY:\n{0}\n### END QUERY\n'.format(attach_query))
      sys.stderr.flush()
    cu.execute(attach_query)

  ### Make the query
  cu.execute(query,dict(himag=himag,ralo=ralo,rahi=rahi,declo=declo,dechi=dechi))

  ### Iterate over the returned rows
  for row in cu:

    lrow = list(row)
    if do_data_debug and do_extra_debug:
      sys.stderr.write('Child Output row:  {0}\n'.format(lrow))
      sys.stderr.flush()

    ### Base group:  idoffset; RA; Dec; Magnitude.
    left_col = 0
    right_col = 4
    base_cols = lrow[left_col:right_col]
    if do_data_debug:
      sys.stderr.write('Child base_cols:  {0}\n'.format(base_cols))
      sys.stderr.flush()
    client_sock.send(struct.pack(byteorder+'I3d',*base_cols))

    if get_light:
      ### Light group:  parallax; proper motions; BP and RP magnitudes.
      left_col = right_col
      right_col += 5
      light_cols = lrow[left_col:right_col]
      if do_data_debug and do_extra_debug:
        sys.stderr.write('Child light_cols:  {0}\n'.format(light_cols))
        sys.stderr.flush()
      ### - Set "Is NULL" bits for any NULL values
      bit = 1
      bits = 0
      for val in light_cols:
        if None is val: bits |= bit
        bit <<= 1

      ### Send the data
      client_sock.send(struct.pack(byteorder+'I5d',bits,*light_cols))

    if get_heavy:
      ### Heavy group:  source_id; errors; covariances
      left_col = right_col
      right_col += 16
      heavy_cols = lrow[left_col:right_col]
      if do_data_debug and do_extra_debug:
        sys.stderr.write('Child heavy_cols:  {0}\n'.format(heavy_cols))
        sys.stderr.flush()
      ### - Set "Is NULL" bits for any NULL values
      bit = 1
      bits = 0
      ### Column source_id, heavy_cols[0], is excluded
      for val in heavy_cols[1:]:
        if None is val: bits |= bit
        bit <<= 1

      ### Send the data
      client_sock.send(struct.pack(byteorder+'IQ15d',bits,*heavy_cols))


########################################################################
### Usage:  python gaialib_server.py gaia_subset.sqlite3 [--host=''] [--port=29073]

if "__main__" == __name__ and sys.argv[1:] and sys.argv[1].endswith('.sqlite3'):

  hostname,portno = '',DEFAULT_PORT
  for arg in sys.argv[2:]:

    if arg.startswith('--host='):
      hostname = arg[7:]
      continue

    if arg.startswith('--port='):
      portno = int(arg[7:])
      continue

    assert False,'Invalid command-line argument [{0}]'.format(arg)

  do_main(sys.argv[1],gaiahostname=hostname,port=portno)
