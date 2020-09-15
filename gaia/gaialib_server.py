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
  query = """
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
;"""

  received,L = b'',0
  while L < 48:
    new_recv = client_sock.recv(48 - L)
    assert len(new_recv) > 0
    received += new_recv
    L = len(received)

  try:
    byteorder = '<'
    (m1,himag,ralo,rahi,declo,dechi,) = inputs = struct.unpack(byteorder + '6d',received)
    assert -1.0 == m1
  except:
    byteorder = '>'
    (m1,himag,ralo,rahi,declo,dechi,) = inputs = struct.unpack(byteorder + '6d',received)
    assert -1.0 == m1

  if do_debug: sys.stderr.write('Child Inputs:  {0}\n'.format(inputs))

  cn = sqlite3.connect(gaialightdb)
  cu = cn.cursor()
  cu.execute(query,dict(himag=himag,ralo=ralo,rahi=rahi,declo=declo,dechi=dechi))
  for row in cu:
    if do_data_debug:
      sys.stderr.write('Child Output row:  {0}\n'.format(list(row)))
      sys.stderr.flush()
    client_sock.send(struct.pack(byteorder+'I3d',*list(row)))

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
