#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include "localmalloc.h"
#include "config.h"
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
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_open_v2( tyc2SQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
# else
  if (SQLITE_OK != sqlite3_open( tyc2SQLfilename, &pDb)) {
# endif
    if (pDb) sqlite3_close(pDb);
  }
  /* prepare statement to return count of stars matching parameters; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_prepare_v2( pDb, countStmt, strlen(countStmt)+1, &pStmt, 0)) {
# else
  if (SQLITE_OK != sqlite3_prepare( pDb, countStmt, strlen(countStmt)+1, &pStmt, 0)) {
# endif
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
# ifdef SQLITE3_HAS_V2S
    if (SQLITE_OK != sqlite3_prepare_v2( pDb, stmt, strlen(stmt)+1, &pStmt, 0)) {
# else
    if (SQLITE_OK != sqlite3_prepare( pDb, stmt, strlen(stmt)+1, &pStmt, 0)) {
# endif
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
      ptr->_xyz[0] = sqlite3_column_double(pStmt, 1);
      ptr->_xyz[1] = sqlite3_column_double(pStmt, 2);
      ptr->_xyz[2] = sqlite3_column_double(pStmt, 3);
      ptr->mag = sqlite3_column_double(pStmt, 4);
      ptr->ra =
      ptr->dec = 0.0;
      strncpy( ptr->catalogORsuppl1, isCatalog ? STATIC_CATALOG : STATIC_SUPPL1, (sizeof ptr->catalogORsuppl1) - 1);
      ptr->catalogORsuppl1[(sizeof ptr->catalogORsuppl1) - 1] =
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
tyc2_getCatline( char* tyc2SQLfilename
          , pTYC2rtn tyc2
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

  /* Return on null filename or pointer tyc2*/
  if ( !tyc2SQLfilename || !tyc2) return failRtn;

  *tyc2->catline = '\0';

  /* set path and isCatalog */
  isCatalog = (tolower(*tyc2->catalogORsuppl1) == 'c');
  pPath = isCatalog ? STATIC_CATALOG : STATIC_SUPPL1;

  /* open DB; return on fail */
  --failRtn;
# ifdef SQLITE3_HAS_V2S
  if (SQLITE_OK != sqlite3_open_v2( tyc2SQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
# else
  if (SQLITE_OK != sqlite3_open( tyc2SQLfilename, &pDb)) {
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
  if ( !(rtnPtr = (const char *) sqlite3_column_text(pStmt, 0)) ) {
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
  if ( fseek(f, lineLength * (long) tyc2->offset, SEEK_SET) ) {
    fclose(f);
    return failRtn;
  }

  --failRtn;
  if ( tyc2->catline != fgets(tyc2->catline, sizeof tyc2->catline, f) ) {
    fclose(f);
    return failRtn;
  }

  fclose(f);

  tyc2->catline[(sizeof tyc2->catline) - 1] = '\0';

  lineLength = strlen(tyc2->catline) - 1;

  if (lineLength >= 0 && tyc2->catline[lineLength] == 10) {
    tyc2->catline[lineLength--] = '\0';
  }
  if (lineLength >= 0 && tyc2->catline[lineLength] == 13) {
    tyc2->catline[lineLength] = '\0';
  }
  return 0;
}

#ifndef SKIP_SIMBAD
/* Simple URL call to SIMBAD to get basic info written to STDERR
 * 
 * This research has made use of the SIMBAD database,
 *     operated at CDS, Strasbourg, France
 */
static char urlBase[1024] = { "http://simweb.u-strasbg.fr/simbad/sim-script?submit=submit+script&script=format+object+%22MAIN_ID:%20%20%25MAIN_ID%5Cn%20%20RA,DEC:%20%20%25COO%28d2%3BAD%29%5Cn%20%20OBJTYP:%20%20%25OTYPE%28V%29%5Cn%20%20FLXLIST:%20%20%25FLUXLIST%28V%29%5Cn%5Cn%22%0Atyc+" };
static char urlFmt[] = { "%04d-%05d-%01d" };
static char* urlPtr = (char*) NULL;

int tyc2_simbad(FILE* stdFile, char *tyc2Record, int* tycNarg) {
CURLcode* ch;
int rtn = 1;
  if ( !urlPtr ) urlPtr = urlBase + strlen(urlBase);
  
  while (1) {
  int tycN[3];

    ch = 0;

    if (tyc2Record) {
    char r13[13];
      strncpy( r13, tyc2Record, 12);
      r13[12] = '\0';
      if ( 3 != sscanf(r13, "%d %d %d", tycN+0, tycN+1, tycN+2)) break;
    } else if (tycNarg) {
      memcpy(tycN, tycNarg, sizeof tycN);
    } else {
      break;
    }
    sprintf( urlPtr, urlFmt, tycN[0], tycN[1], tycN[2] );

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
