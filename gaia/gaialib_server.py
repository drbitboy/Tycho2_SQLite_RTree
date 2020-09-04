import os
import sys
import struct
import socket
import sqlite3
import traceback as tb

DEFAULT_PORT = int(''.join(['{0:01x}'.format(15&ord(c)) for c in 'gaia']),16)
do_debug = 'DEBUG' in os.environ


########################################################################
def do_main(gaialightdb,gaiahostname='',port=DEFAULT_PORT):
  server_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
  server_sock.bind((gaiahostname,port,))
  server_sock.listen(50)
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
          if do_debug: tb.print_exc()
        exit(0)
      ### Parent closes its copy of the client socket & continues
      client_sock.close()
  except SystemExit as e: pass
  except:
    if do_debug: tb.print_exc()
    try:
      sys.stderr.write('GaiaLib server closing\n')
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
  AND gaialight.phot_g_mean_mag<?
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
    (m1,himag,ralo,rahi,declo,dechi,) = struct.unpack(byteorder + '6d',received)
    assert -1.0 == m1
  except:
    byteorder = '>'
    (m1,himag,ralo,rahi,declo,dechi,) = struct.unpack(byteorder + '6d',received)
    assert -1.0 == m1

  cn = sqlite3.connect(sqlitedb)
  cu = cn.cursor()
  cu.execute(query,dict(himag=himag,ralo=ralo,rahi=rahi,declo=declo,dechi=dechi))
  for row in cu:
    client_sock.send(struct.pack(byteorder+'I3d',*list(row)))
