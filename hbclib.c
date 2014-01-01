#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include "localmalloc.h"
#include "config.h"
#include "hbclib.h" // includes sqlite3.h, tyc2lib.h

/* Query <hbcpfx>rtree and <hbcpfx>data TABLEs
 * - lo/hi mag in magnitude units
 * - lo/hi ra/dec in degrees
 * Return non-negative number of records on success
 * Return negative number on failure
 */
int hbcRDMselect( char* hbcSQLfilename
                 , char *hbcpfx                  // e.g. "bsc5"
                 , double lomag, double himag
                 , double lora, double hira
                 , double lodec, double hidec
                 , pSTARrtn *pRtn
                 ) {
/* N.B. lomag/lora/lodec arguments are compared against .himag/.hira/.hidec
        fields, and vice-versa
   ...
   WHERE starrtree.himag>?         <= replace by lomag argument
     AND starrtree.lomag<?         <= replace by himag argument
     AND starrtree.hira>?          <= replace by lora argument
     AND starrtree.lora<?          <= replace by hira argument
     AND starrtree.hidec>?         <= replace by lodec argument
     AND starrtree.lodec<?         <= replace by hidec argument
     ...

   - put in array here to allow binding via loop below
 */
#define STAR_ARGN_COUNT 6
double argN[STAR_ARGN_COUNT] = { lomag, himag, lora, hira, lodec, hidec };

/* Pointer to malloc'ed memory to which to write SQL SELECT statements */
char *countStmt;
char *stmt;

sqlite3_stmt *pStmt = NULL;        // Pointer to SQLite3 prepared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite3 DB

int rtn, i, count;
int failRtn = 0;

pTYC2rtn ptr;                      // pointer to structs in linked list

  *pRtn = NULL;

  /* open DB; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_open_v2( hbcSQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
# else
  if (SQLITE_OK != sqlite3_open( hbcSQLfilename, &pDb)) {
# endif
    if (pDb) sqlite3_close(pDb);
    return failRtn;
  }
  /* prepare text statement */
  --failRtn;
  countStmt = (char *) malloc( strlen( hbcCountStmtFmt ) + strlen(hbcpfx) + 1 );
  if (!countStmt) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  sprintf( countStmt, hbcCountStmtFmt, hbcpfx );
  /* prepare statement to return count of stars matching parameters; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_prepare_v2( pDb, countStmt, strlen(countStmt)+1, &pStmt, 0)) {
# else
  if (SQLITE_OK != sqlite3_prepare( pDb, countStmt, strlen(countStmt)+1, &pStmt, 0)) {
# endif
    free(countStmt);
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  free(countStmt);
  /* bind parameters; return on fail */
  for (i=0; i<STAR_ARGN_COUNT; ++i) {
    --failRtn;
    if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, argN[i])) {
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

    /* prepare text statement */
    --failRtn;
    stmt = (char *) malloc( strlen( hbcStmtFmt ) + 2 * strlen(hbcpfx) + 1 );
    if ( !stmt ) {
      sqlite3_close(pDb);
      return failRtn;
    }
    sprintf( stmt, hbcStmtFmt, hbcpfx, hbcpfx );
    /* prepare statement; return on fail */
    --failRtn;
# ifdef SQLITE3_HAS_V2S
    if (SQLITE_OK != sqlite3_prepare_v2( pDb, stmt, strlen(stmt)+1, &pStmt, 0)) {
#   else
    if (SQLITE_OK != sqlite3_prepare( pDb, stmt, strlen(stmt)+1, &pStmt, 0)) {
#   endif
      free(stmt);
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }
    free(stmt);
    /* bind parameters; return on fail */
    for (i=0; i<STAR_ARGN_COUNT; ++i) {
      --failRtn;
      if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, argN[i])) {
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
      ptr->_xyz[0] = sqlite3_column_double(pStmt, 1);
      ptr->_xyz[1] = sqlite3_column_double(pStmt, 2);
      ptr->_xyz[2] = sqlite3_column_double(pStmt, 3);
      ptr->mag = sqlite3_column_double(pStmt, 4);
      ptr->catalogORsuppl1[0] =
      *ptr->catline = '\0';
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

int
hbc_getCatline( char* hbcSQLfilename
              , pSTARrtn starList
              ) {
sqlite3_stmt *pStmt = NULL;        // Pointer to repared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB
char *pPath;
const char *rtnPtr;
char path[1024];;
int isCatalog;
int rtn;
int failRtn = -1;
long lineLength;
long cInt;
FILE *f;

  /* Return on null filename or pointer starList */
  if ( !hbcSQLfilename || !starList) return failRtn;

  *starList->catline = '\0';

  /* set path and isCatalog */
  isCatalog = (tolower(*starList->catalogORsuppl1) == 'c');
  pPath = isCatalog ? STATIC_CATALOG : STATIC_SUPPL1;

  /* open DB; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_open_v2( hbcSQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
# else
  if (SQLITE_OK != sqlite3_open( hbcSQLfilename, &pDb)) {
# endif
    if (pDb) sqlite3_close(pDb);
    return failRtn;
  }
  /* prepare statement to return count of stars matching parameters; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_prepare_v2( pDb, pathLookupStmt, strlen(pathLookupStmt)+1, &pStmt, 0)) {
# else
  if (SQLITE_OK != sqlite3_prepare( pDb, pathLookupStmt, strlen(pathLookupStmt)+1, &pStmt, 0)) {
# endif
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  /* bind parameters; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_bind_text( pStmt, 1, pPath, (int) strlen(pPath), SQLITE_STATIC)) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  /* read the one record; return on fail */
  --failRtn;
  if ( SQLITE_ROW != (rtn = sqlite3_step(pStmt)) ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }
  /* extract path from record and copy it to the path variable */
  --failRtn;
  if ( !(rtnPtr = sqlite3_column_text(pStmt, 0)) ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  strncpy( path, rtnPtr, 249);
  path[249] = '\0';

  /* read one more record; should return SQLITE_DONE */
  --failRtn;
  if ( SQLITE_DONE != (rtn = sqlite3_step(pStmt)) ) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  /* cleanup DB */
  sqlite3_finalize(pStmt);
  sqlite3_close(pDb);

  /* Open file */
  --failRtn;
  if ( !(f = fopen( path, "r")) ) {
    return failRtn;
  }

  /* set length w/o line termination; will be offset to that termination */
  lineLength = isCatalog ? 206 : 122;

  fseek(f,lineLength,SEEK_SET);
  cInt = fgetc(f);
  --failRtn;
  if (cInt == 13) {   /* cInt may be a carriage return */
    ++lineLength;
    cInt = fgetc(f);
  }
  --failRtn;
  if (cInt != 10) {   /* to here, cInt must be a linefeed */
    fclose(f);
    return failRtn;
  }

  ++lineLength;        /* line length including line termination char(s) */

  --failRtn;
  if ( fseek(f, lineLength * (long) starList->offset, SEEK_SET) ) {
    fclose(f);
    return failRtn;
  }

  --failRtn;
  if ( starList->catline != fgets(starList->catline, sizeof starList->catline, f) ) {
    fclose(f);
    return failRtn;
  }

  fclose(f);

  starList->catline[(sizeof starList->catline) - 1] = '\0';

  lineLength = strlen(starList->catline) - 1;

  if (lineLength >= 0 && starList->catline[lineLength] == 10) {
    starList->catline[lineLength--] = '\0';
  }
  if (lineLength >= 0 && starList->catline[lineLength] == 13) {
    starList->catline[lineLength] = '\0';
  }
  return 0;
}

#ifndef SKIP_SIMBAD
/* Simple URL call to SIMBAD to get basic info written to STDERR
 * 
 * This research has made use of the SIMBAD database,
 *     operated at CDS, Strasbourg, France
 */

/* Structure to keep track of (Prefix,Designator,FirstRecord)
   PDI => Prefix, Designator, ID of first record
   - prefix      - describes catalog (e.g. bsc5),
   - designator  - prefix of star ID passed to SIMBAD
   - firstRecord - added to offset to record ID; typically 1
 */
typedef struct StarPDIstr {
  char pfx[32];
  char designator[32];
  int firstRecord;
} StarPDI;

static StarPDI pdis[] = { { "bsc5", "HR",   1 }
                        , { "sao",  "SAO",  1 }
                        , { "",     "",    -1 }  // Guard at end of list
                        };

/* Base URL call; append star ID */
static char urlBase[1024] = { "http://simweb.u-strasbg.fr/simbad/sim-script?submit=submit+script&script=format+object+%22MAIN_ID:%20%20%25MAIN_ID%5Cn%20%20RA,DEC:%20%20%25COO%28d2%3BAD%29%5Cn%20%20OBJTYP:%20%20%25OTYPE%28V%29%5Cn%20%20FLXLIST:%20%20%25FLUXLIST%28V%29%5Cn%5Cn%22%0A" };
static char urlStarIDFmt[] = { "%s+%d" };  // e.g. HR+1142, SAO+76131; + becomes space
static char* urlPtr = (char*) NULL;

int hbc_simbad( FILE* stdFile   // Where to write data from SIMBAD
              , char *hbcpfx    // Catalog e.g. bsc5
              , int offset      // Offset from first record; add to firstRecord to get ID
              ) {
CURLcode* ch;
int rtn = 1;

  /* Pointer at which to sprintf(urlPtr, urStarIDlFmt, designator, recordID); */
  if ( !urlPtr ) urlPtr = urlBase + strlen(urlBase);
  
  while (1) {
  int tycN[3];
  StarPDI *pPDI;
  int iPfx;

    ch = 0;

    // Find Prefix that matches prefix
    for ( pPDI=pdis+(iPfx=0); pPDI->firstRecord > -1 && strcmp(pPDI->pfx,hbcpfx); pPDI=pdis+ ++iPfx) ;
    // quit if no matching prefix found
    if (  pPDI->firstRecord < 0 ) break;

    sprintf( urlPtr, urlStarIDFmt, pPDI->designator, offset+1 );

#   define IFBK(A) if (A) break
    IFBK( !(ch = curl_easy_init()) );
    IFBK( CURLE_OK != curl_easy_setopt(ch, CURLOPT_VERBOSE, 0) );
    IFBK( CURLE_OK != curl_easy_setopt(ch, CURLOPT_HEADER, 0) );
    IFBK( CURLE_OK != curl_easy_setopt(ch, CURLOPT_WRITEFUNCTION, NULL) );
    IFBK( CURLE_OK != curl_easy_setopt(ch, CURLOPT_WRITEDATA, stdFile) );
    IFBK( CURLE_OK != curl_easy_setopt(ch, CURLOPT_URL, urlBase) );
    IFBK( CURLE_OK != curl_easy_perform(ch) );

    rtn = 0; // success
    break;
  } // while 1

  if (ch) curl_easy_cleanup(ch);

  return rtn;
}
#endif //ifndef SKIP_SIMBAD
