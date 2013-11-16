#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <sqlite3.h>

static char* stmts[] = 
{ "SELECT tyc2catalog_uvs.offset"
, "      ,tyc2catalog_uvs.x ,tyc2catalog_uvs.y ,tyc2catalog_uvs.z ,tyc2catalog_uvs.mag"
, "FROM tyc2indexrtree"
, "INNER JOIN tyc2index"
, "   ON tyc2indexrtree.offset=tyc2index.offset"
, "INNER JOIN tyc2catalog_uvs"
, "   ON tyc2index.tyc2start<=tyc2catalog_uvs.offset"
, "  AND tyc2index.tyc2end>tyc2catalog_uvs.offset"
, "  AND tyc2catalog_uvs.mag<?"
, "WHERE tyc2indexrtree.offset=tyc2index.offset"
, "  AND tyc2indexrtree.hira>?"
, "  AND tyc2indexrtree.lora<?"
, "  AND tyc2indexrtree.hidec>?"
, "  AND tyc2indexrtree.lodec<?"
, "ORDER BY tyc2catalog_uvs.mag asc;"
, 0
};
static double dflt5[] = {6.5,56.0,58.0,23.0,25.0};


int main() {
char* stmt;
char* pstmt;
int L;
char** ps;
sqlite3 *pDb = 0;
sqlite3_stmt *pStmt = 0;
int rtn;
int i;

  /* open DB */
  if (SQLITE_OK != sqlite3_open_v2( "tyc2.sqlite3", &pDb, SQLITE_OPEN_READONLY, (char *) 0)) return 1;

  /* Build statement string */
  for ( ps=stmts, L=0; *ps; ++ps) L += 1 + strlen(*ps);

  stmt = (char *) malloc(1 + (L * sizeof(char)));

  for ( ps=stmts, pstmt=stmt; *ps; ++ps) {
    strcpy(pstmt, *ps);
    pstmt += strlen(pstmt);
    strcpy(pstmt, " ");
    ++pstmt;
  }
  if (getenv("DEBUG")) printf( "%d %d %s\n", L, (int)strlen(stmt), stmt);

  if (SQLITE_OK != sqlite3_prepare_v2( pDb, stmt, L+1, &pStmt, 0)) {
    sqlite3_close(pDb);
    free(stmt);
    return 2;
  }

  for (i=0; i<5; ++i) {
    if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, dflt5[i])) {
      sqlite3_close(pDb);
      free(stmt);
      return i+3;
    }
  }

  printf(" Offset      X      Y      Z   Magn.\n");
  while ( SQLITE_ROW == (rtn = sqlite3_step(pStmt)) ) {
    for (i=0; i<5; ++i) {
      if (i < 1) {
        printf( "%7d", sqlite3_column_int(pStmt, i));
      } else {
        printf( i < 4 ? " %6.3f" : " %7.3f", sqlite3_column_double(pStmt, i));
      }
    }
    printf("\n");
  }
  sqlite3_reset(pStmt);
  sqlite3_finalize(pStmt);
  sqlite3_close(pDb);
  free(stmt);
  return (rtn == SQLITE_DONE) ? 0 : 8;
  /*
  print(" Offset      X      Y      Z   Magn.")
  for row in cu: print( "%7d %6.3f %6.3f %6.3f %7.3f" % row )
  */
}
/*
### Option test:  list all *possible* stars from 56<RA<58, 23<DEC<25, mag<6.5 (Pleiades)
if 'test' in sys.argv[1:]:

  ### HiMag, LoRA, HiRA, LoDEC, HiDEC
  dflt5=[6.5,56.0,58.0,23.0,25.0,]

  for arg in sys.argv[1:]:
    i=0
    for name in 'himag lora hira lodec hidec'.split():
      pfx = '--%s=' % (name,)
      lenPfx = len(pfx)
      if arg[:lenPfx]==pfx: dflt5[i] = float(arg[lenPfx:])
      i += 1

  cn = sl3.connect("tyc2.sqlite3")
  cu = cn.cursor()
  cu.execute("""
SELECT tyc2catalog_uvs.offset
      ,tyc2catalog_uvs.x ,tyc2catalog_uvs.y ,tyc2catalog_uvs.z ,tyc2catalog_uvs.mag
FROM tyc2indexrtree
INNER JOIN tyc2index
   ON tyc2indexrtree.offset=tyc2index.offset
INNER JOIN tyc2catalog_uvs
   ON tyc2index.tyc2start<=tyc2catalog_uvs.offset
  AND tyc2index.tyc2end>tyc2catalog_uvs.offset
  AND tyc2catalog_uvs.mag<?
WHERE tyc2indexrtree.offset=tyc2index.offset
  AND tyc2indexrtree.hira>?
  AND tyc2indexrtree.lora<?
  AND tyc2indexrtree.hidec>?
  AND tyc2indexrtree.lodec<?
ORDER BY tyc2catalog_uvs.mag asc;
""", dflt5)

  print(" Offset      X      Y      Z   Magn.")
  for row in cu: print( "%7d %6.3f %6.3f %6.3f %7.3f" % row )
*/
