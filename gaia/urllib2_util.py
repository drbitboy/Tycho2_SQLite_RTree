import urllib2

def hdr2pair(shdr):
  toks = shdr.split(':')
  key = toks.pop(0)
  return (key,':'.join(toks).strip(),)

########################################################################
class HEAD_REQUEST(urllib2.Request):
  def get_method(self): return 'HEAD'

class URLLIB2_WRAPPER(object):

  def __init__(self,hostname,maxsize=1,timeout=10):
    self.hostname,self.maxsize,self.timeout = hostname,maxsize,timeout

  def request(self,sreq,path):
    url = 'http://{0}{1}'.format(self.hostname,path)
    if 'HEAD'==sreq:
      response = urllib2.urlopen(HEAD_REQUEST(url),timeout=self.timeout)
    else:
      assert 'GET'==sreq
      response = urllib2.urlopen(urllib2.Request(url),timeout=self.timeout)
      self.data = response.read()

    self.headers = dict(map(hdr2pair,response.headers))

    response.close()

    return self

  def close(self,sreq,path): pass
