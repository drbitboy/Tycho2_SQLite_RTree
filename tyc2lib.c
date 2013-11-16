#include <ctype.h>
#include <string.h>
#include <malloc.h>
#include "tyc2lib.h" // includes sqlite3.h

// Select on Mag
// Return non-negative number of records on success
// Return negative number on failure
int tyc2RDMselect( char* tyc2SQLfilename
                 , double himag
                 , double lora, double hira
                 , double lodec, double hidec
                 , char *catalogORsuppl1
                 , pTYC2rtn *pRtn
                 ) {
/* N.B. lora/lodec are compared against .hira and .hidec, and vice-versa
   ...
   tyc2catalog_uvs.mag<?
   WHERE tyc2indexrtree.offset=tyc2index.offset
   AND tyc2indexrtree.hira>?       <= ? replaced by lora
   AND tyc2indexrtree.lora<?       <= ? replaced by hira
   AND tyc2indexrtree.hidec>?      <= ? replaced by lodec
   AND tyc2indexrtree.lodec<?      <= ? replaced by hidec
   ...
 */
double arg5[5] = { himag, lora, hira, lodec, hidec };

int isCatalog = tolower(*catalogORsuppl1) == 'c';

char *countStmt = isCatalog ? catalogCountStmt : suppl1CountStmt;
char *stmt = isCatalog ? catalogStmt : suppl1Stmt;
sqlite3_stmt *pStmt = NULL;
sqlite3 *pDb = NULL;
int L, rtn, i, count;
int failRtn = 0;
pTYC2rtn ptr;

  *pRtn = NULL;

  /* open DB; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_open_v2( tyc2SQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
    if (pDb) sqlite3_close(pDb);
  }
  /* prepare statement; return on fail */
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

  --failRtn;
  if ( SQLITE_ROW != (rtn = sqlite3_step(pStmt)) ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  count = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  pStmt = NULL;

  if (count < 0) {
    sqlite3_close(pDb);
    return failRtn;
  }
  if (!count) {
    rtn = SQLITE_DONE;
  } else {
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

    /* malloc return structures */
    *pRtn = (pTYC2rtn) malloc( count * sizeof(TYC2rtn) );

    --failRtn;
    if (!*pRtn) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }

    /* Loop over rows, save results */
    for (ptr = *pRtn+(i=0); i<count; ++i,++ptr) {
      if ( SQLITE_ROW != (rtn = sqlite3_step(pStmt)) ) break;
      ptr->seqNum = i;
      ptr->offset = sqlite3_column_int(pStmt, 0);
      ptr->xyz[0] = sqlite3_column_double(pStmt, 1);
      ptr->xyz[1] = sqlite3_column_double(pStmt, 2);
      ptr->xyz[2] = sqlite3_column_double(pStmt, 3);
      ptr->mag = sqlite3_column_double(pStmt, 4);
      ptr->ra =
      ptr->dec = 0.0;
      ptr->next = ptr + 1;
    }
    if (rtn == SQLITE_ROW && i==count) {
      (ptr-1)->next = NULL;
      rtn = sqlite3_step(pStmt);
    }
  }

  /* cleanup and return count on success */
  sqlite3_finalize(pStmt);
  sqlite3_close(pDb);
  if (rtn == SQLITE_DONE && i==count) return count;

  /* cleanup and return on failure (last step did not return SQLITE_ROW) */
  free((void*) *pRtn);
  *pRtn = NULL;
  return --failRtn;
}
