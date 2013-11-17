#include <ctype.h>
#include <string.h>
#include <malloc.h>
#include "tyc2lib.h" // includes sqlite3.h

/* Select on Mag
 * Return non-negative number of records on success
 * Return negative number on failure
 * - himag in magnitude units
 * - lo/hi ra/dec in degrees
 */
int tyc2RDMselect( char* tyc2SQLfilename
                 , double himag
                 , double lora, double hira
                 , double lodec, double hidec
                 , char *catalogORsuppl1
                 , pTYC2rtn *pRtn
                 ) {
/* N.B. lora/lodec are compared against .hira and .hidec, and vice-versa
   ...
   tyc2catalog_uvs.mag<?           <= ? replace by himag
   WHERE ...
   AND tyc2indexrtree.hira>?       <= ? replaced by lora
   AND tyc2indexrtree.lora<?       <= ? replaced by hira
   AND tyc2indexrtree.hidec>?      <= ? replaced by lodec
   AND tyc2indexrtree.lodec<?      <= ? replaced by hidec
   ...
   - put in array here to allow binding via loop below
 */
double arg5[5] = { himag, lora, hira, lodec, hidec };

int isCatalog = tolower(*catalogORsuppl1) == 'c';  // catalog.dat or suppl_1.dat

/* Choose SQL SELECT statements for catalog.dat or suppl_1.dat */
char *countStmt = isCatalog ? catalogCountStmt : suppl1CountStmt;
char *stmt = isCatalog ? catalogStmt : suppl1Stmt;

sqlite3_stmt *pStmt = NULL;        // Pointer to repared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB

int rtn, i, count, failRtn = 0;

pTYC2rtn ptr;                      // pointer to strucs in linked list

  *pRtn = NULL;

  /* open DB; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_open_v2( tyc2SQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
    if (pDb) sqlite3_close(pDb);
  }
  /* prepare statement to return count of stars matching parameters; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_prepare_v2( pDb, countStmt, strlen(countStmt)+1, &pStmt, 0)) {
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
  /* read the one record; return on fail */
  --failRtn;
  if ( SQLITE_ROW != (rtn = sqlite3_step(pStmt)) ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  /* extract count from record; destroy prepared statement */
  count = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  pStmt = NULL;

  /* check that count is non-negative */
  if (count < 0) {
    sqlite3_close(pDb);
    return failRtn;
  }
  if (!count) {
    /* manually set results for for zero count */
    rtn = SQLITE_DONE;
    i = count;

  } else {

    /* Positive count; SQL-SELECT records and place in linked list */

    /* prepare statement; return on fail */
    --failRtn;
    if (SQLITE_OK != sqlite3_prepare_v2( pDb, stmt, strlen(stmt)+1, &pStmt, 0)) {
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

    /* malloc array of return structures; return on fail */
    *pRtn = (pTYC2rtn) malloc( count * sizeof(TYC2rtn) );

    --failRtn;
    if (!*pRtn) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }

    /* Loop over rows, save results */
    for (ptr = *pRtn+(i=0); i<count; ++i, ptr=ptr->next) {
      if ( SQLITE_ROW != (rtn = sqlite3_step(pStmt)) ) break;
      ptr->seqNum = i;
      ptr->offset = sqlite3_column_int(pStmt, 0);
      ptr->xyz[0] = sqlite3_column_double(pStmt, 1);
      ptr->xyz[1] = sqlite3_column_double(pStmt, 2);
      ptr->xyz[2] = sqlite3_column_double(pStmt, 3);
      ptr->mag = sqlite3_column_double(pStmt, 4);
      ptr->ra =
      ptr->dec = 0.0;
      ptr->next = ptr + 1;   // ->next pointer even though this is an array
    }
    /* Set NULL ->next pointer in last structure in linked list
     * - do one more sqlite3_step() call, which should return SQLITE_DONE
     */
    if (rtn == SQLITE_ROW && i==count) {
      (ptr-1)->next = NULL;
      rtn = sqlite3_step(pStmt);
    }
  }

  /* cleanup ... */
  sqlite3_finalize(pStmt);
  sqlite3_close(pDb);

  /* ... and return count on success */
  if (rtn == SQLITE_DONE && i==count) {
    return count;
  }
  /* On failure (last step did not return SQLITE_ROW),
   * cleanup and return negative count
   */
  free((void*) *pRtn);
  *pRtn = NULL;
  return --failRtn;
}
