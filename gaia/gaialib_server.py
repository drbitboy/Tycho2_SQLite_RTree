import os
import sys
import struct
import socket
import signal
import sqlite3
import traceback as tb

do_debug = 'DEBUG' in os.environ
do_extra_debug = 'EXTRA_DEBUG' in os.environ
DEFAULT_PORT = int(''.join(['{0:01x}'.format(15&ord(c)) for c in 'gaia']),16)

def handleSIGCHLD(*args):
  while True:
    try:
      pid,status = os.waitpid(-1,os.WNOHANG)
      if 0==pid and 0==status: raise ChildProcessError
      if do_debug:
        sys.stderr.write("Child [{0}] exited with status [{1}]\n".format(pid,status))
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
          if do_extra_debug: tb.print_exc()
        exit(0)
      ### Parent closes its copy of the client socket & continues
      client_sock.close()
      if do_debug: sys.stderr.write("Child {0} forked\n".format(pid))
  except SystemExit as e: pass
  except:
    if do_extra_debug: tb.print_exc()
    try:
      if do_debug: sys.stderr.write('GaiaLib server closing\n')
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

  if do_debug: sys.stderr.write('Inputs:  {0}\n'.format(inputs))

  cn = sqlite3.connect(gaialightdb)
  cu = cn.cursor()
  cu.execute(query,dict(himag=himag,ralo=ralo,rahi=rahi,declo=declo,dechi=dechi))
  for row in cu:
    if do_debug: sys.stderr.write('Output row:  {0}\n'.format(list(row)))
    client_sock.send(struct.pack(byteorder+'I3d',*list(row)))
