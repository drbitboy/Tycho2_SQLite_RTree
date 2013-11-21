import re

def cvt1(arg):
  if type(arg) is str: return "'%s'" % (arg,)
  if arg is None: return ''
  return str(arg)

def cvt(args):
  return [cvt1(arg) for arg in args]
  
class sqlite3GhostClass:
  def __init__(self,db):
    self.intransaction = False
    print( "-- sqlite3GhostClass:  Open DB %s" % (db,) )
    return

  def close(self): return

  def cursor(self): return self

  def fetchall(self): return ()

  def commit(self):
    if self.intransaction:
      self.intransaction = False
      self.execute( "END TRANSACTION" )

  def execute(self,stmt,args=()):
    if not args:
      if re.match( "^BEGIN  *TRANSACTION$", stmt.upper().strip().strip(';')): self.intransaction = True
      print("%s;"%(stmt,))
      return
    stmtList = stmt.split('?')
    print( "%s;" % ( ''.join( [ i+j for i,j in zip( stmt.split('?'), cvt(tuple(args)+(None,)) ) ] ) ,) )

  def executemany(self,stmt,argIter):
    try:
      while True:
        arg = argIter.next()
        self.execute(stmt, arg)
    except:
      pass


def connect(db):
  return sqlite3GhostClass(db)
