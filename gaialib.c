#include <math.h>
#include "localmalloc.h"
#include "gaialib.h"

static char gaiaSelectStmt[] = { "SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag FROM gaiartree INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset WHERE lomag<? AND rahi>? AND ralo<? AND dechi>?  AND declo<?  ORDER BY gaialight.phot_g_mean_mag ASC ;" };


#define HARDLIMIT 1024

int
gaiaRDMselect(char* gaiaSQLfilename
             ,double himag
             ,double ralo, double rahi
             ,double declo, double dechi
             ,pTYC2rtn *pRtn
             ) {

static double rpd = atan(1.0) / 45.0;

double arg5[5] = { himag, ralo, rahi, declo, dechi };

sqlite3_stmt *pStmt = NULL;        // Pointer to repared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB

int i;
int count;
int rtn = 0;
int failRtn = 0;

pTYC2rtn ptr;                      // pointer to strucs in linked list

  *pRtn = NULL;

  /* open DB; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_open_v2( gaiaSQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
    if (pDb) sqlite3_close(pDb);
    return failRtn;
  }

  /* malloc large array of return structures; return on fail */
  *pRtn = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );

  /* Assume non-zero count; SQL-SELECT records & place in linked list */

  /* prepare statement; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_prepare_v2( pDb
                                     , gaiaSelectStmt
                                     , strlen(gaiaSelectStmt)+1
                                     , &pStmt, 0
                                     )
     ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  /* bind parameters; return on fail */
  for (i=0; i<5; ++i) {
    --failRtn;
    if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, arg5[i])) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }
  }

  --failRtn;
  if (!*pRtn) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  /* Loop over rows, save results */
  for (ptr = *pRtn+(count=0)
      ; count < HARDLIMIT && SQLITE_ROW == (rtn = sqlite3_step(pStmt))
      ; ++count, ptr=ptr->next
      ) {
  double rarad;
  double decrad;
  double cosra;
  double sinra;
  double cosdec;
  double sindec;
    ptr->seqNum = count;
    ptr->offset = sqlite3_column_int(pStmt, 0);
    ptr->ra     = sqlite3_column_double(pStmt, 1);
    ptr->dec    = sqlite3_column_double(pStmt, 2);
    ptr->mag    = sqlite3_column_double(pStmt, 3);
    rarad = rpd * ptr->ra;
    decrad = rpd * ptr->dec;
    cosra = cos(rarad);
    sinra = sin(rarad);
    cosdec = cos(decrad);
    sindec = sin(decrad);
    ptr->_xyz[0] = cosdec * cosra;
    ptr->_xyz[1] = cosdec * sinra;
    ptr->_xyz[2] = sindec;
    /* ->catalogORsuppl1 and ->catline:  not used */
    *ptr->catalogORsuppl1 = *ptr->catline = '\0';
    ptr->next = ptr + 1;   // ->next pointer even though this is an array
  }

  /* Force rtn to be SQLITE_DONE if hard limit was reached */
  if (count==HARDLIMIT) {
    rtn = SQLITE_DONE;
  }

  /* Set NULL ->next pointer in last structure in linked list
   * - If there was at least one item returned
   * - If last step returned SQLITE_DONE
   */
  if (count>0 && rtn == SQLITE_DONE) {
    (ptr-1)->next = NULL;
  }

  /* cleanup ... */
  sqlite3_finalize(pStmt);
  sqlite3_close(pDb);

  /* ... and return count on success */
  if (rtn == SQLITE_DONE) {
    return count;
  }
  /* On failure (last step did not return SQLITE_ROW),
   * cleanup and return negative count
   */
  free((void*) *pRtn);
  *pRtn = NULL;
  return --failRtn;
}
