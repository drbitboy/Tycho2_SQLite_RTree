"""
Experimenting with os.fork

- Parent process spawns 4 (default) children, each with a different
  environment variable
- Children processes sleep per found environment variable(s)
- Children processes waits for all children to exit, then exits itself

"""
import os
import sys
import time
import urllib3

### Make environment variable name Q[x]_ONLY, where [x] is sleep time
mevn = lambda i:'Q{0}_ONLY'.format(i)

########################################################################
### Class to hold static variables HTTPPOOL and PID
class HPOOL:

  ### Static variables of class HPOOL
  HTTPPOOL = None
  PID = None

  def __init__(self): pass
  def httppool(self):
    """Return HPOOL.HTTPPOOL object; create if needed"""
    try:
      ### Look for existing static HPOOL.HTTPPOOL
      assert isinstance(HPOOL.HTTPPOOL,urllib3.HTTPConnectionPool)
      print('{0} found existing HTTPPOOL'.format(os.getpid()))
    except:
      ### Create if existing HPOOL.HTTPPOOL was not found
      print('{0} creating HTTPPOOL'.format(os.getpid()))
      HPOOL.HTTPPOOL = urllib3.HTTPConnectionPool('cdn.gea.esac.esa.int'
                                                 ,maxsize=1
                                                 ,timeout=urllib3.util.Timeout(10)
                                                 )
    ### Set HPOOL.PID to current process' PID, and return static var
    HPOOL.PID = os.getpid()
    return HPOOL.HTTPPOOL


########################################################################
### Routine that forked children will run
def vorked():
  global n

  ### Initialize sleep time and list to hold sleep times
  sleep_s,lt = 1,list()

  ### Loop over possible sleep times
  while sleep_s <= n:

    ### If environment variable with sleep time exists,
    ### then add that sleep time
    if mevn(sleep_s) in os.environ: lt.append(sleep_s)
    sleep_s += 1

  ### Print found sleep times to STDOUT
  print(dict(childpid=os.getpid(),lt=lt))

  ### Loop over any found sleep times
  while lt:
    ### Create or show static content of HPOOL class
    print((os.getpid(),HPOOL().httppool(),HPOOL.PID,))
    sleep_s = lt.pop()
    time.sleep(2.5*sleep_s)

  ### Create or show static content of HPOOL class
  print((os.getpid(),HPOOL().httppool(),HPOOL.PID,))

  ### Exit with sleep time as exit status
  exit(sleep_s)

### Globals:  n is range fo sleep times; d is dict of child PIDs
n = int(([4]+sys.argv[1:]).pop())
d = dict()

### Parent process loops over sleep times
for i in range(1,n+1):
  ### Create environment variable
  evn = mevn(i)
  os.environ[evn]=''

  ### Fork a childe
  pid = os.fork()

  ### Child process sees pid==0, then calls, and exits from, vorked()
  if pid==0: vorked()

  ### Parent process sees pid!=0, saves pid to dict d
  d[pid] = (pid,i)

  ### Delete environment variable
  del os.environ[evn]

### Child processes have been forked, parent waits for them to exit
while d:

  ### Wait for next child process to exit, print results
  fpid,fstatus = os.wait()
  print(dict(pid_i=d[fpid],fpid=fpid,fstatus=fstatus>>8))

  ### Delete pid from dict; eventual d will be empty and loop will exit
  del d[fpid]
