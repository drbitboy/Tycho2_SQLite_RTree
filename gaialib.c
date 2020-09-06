#include <math.h>
#include "gaialib.h"
#include "localmalloc.h"
#include "get_client_socket_fd.h"

static char gaiaSelectStmt[] = { "SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag FROM gaiartree INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;" };


#define HARDLIMIT 1024


/***********************************************************************
 * Use SQLite3 database; gaiaSQLfilename is gaia.sqlite3 or similar
 */
int
gaiaRDMselect_sql(char* gaiaSQLfilename
                 ,double himag
                 ,double ralo, double rahi
                 ,double declo, double dechi
                 ,pTYC2rtn *pRtn
                 ) {

double rpd = atan(1.0) / 45.0;

double arg6[6] = { himag, ralo, rahi, declo, dechi, himag };

sqlite3_stmt *pStmt = NULL;        // Pointer to repared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB

int i;
int count;
int rtn = 0;
int failRtn = 0;

pTYC2rtn ptr;                      // pointer to a struct in linked list

  *pRtn = NULL;

  /* open DB; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_open_v2( gaiaSQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
    if (pDb) sqlite3_close(pDb);
    return failRtn;
  }

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
  for (i=0; i<6; ++i) {
    --failRtn;
    if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, arg6[i])) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }
  }

  /* malloc large array of return structures; return on fail */
  --failRtn;
  *pRtn = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
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

/***********************************************************************
 * Use network server; see gaia/gaialib_server.py;
 * gaiaSQLfilename is gaia:host[:port]
 */
int
gaiaRDMselect_net(char* gaiaSQLfilename
                 ,double himag
                 ,double ralo, double rahi
                 ,double declo, double dechi
                 ,pTYC2rtn *pRtn
                 ) {

double rpd = atan(1.0) / 45.0;

double arg6[6] = { -1.0, himag, ralo, rahi, declo, dechi };

int sockfd;

int i;
int count;
int rtn = 0;
int failRtn = 0;

enum { GAIA_DONE=0
     , GAIA_ROW
     , GAIA_ERROR
     };

pTYC2rtn ptr;                      // pointer to a struct in linked list

  *pRtn = NULL;

  /* Create socket; return on fail */
  --failRtn;
  if ( 0 > (sockfd = get_client_socket_fd(gaiaSQLfilename,"gaia"))) {
    return failRtn;
  }

  /* Send parameters */
  --failRtn;
  if (48 != write(sockfd,(void*)arg6,48)) {
    (void)close(sockfd);
    return failRtn;
  }

  /* malloc large array of return structures; return on fail */
  --failRtn;
  *pRtn = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
  if (!*pRtn) {
    (void)close(sockfd);
    return failRtn;
  }

  /* Loop over rows, save results */
  --failRtn;
  for (ptr = *pRtn+(count=0)
      ; count < HARDLIMIT
      ; ++count, ptr=ptr->next
      ) {
  double p_doubles[4];
  int* p_start_int = ((int*) p_doubles);
  int* p_offset_int = p_start_int + 1;
  void* p_void_start = (void*) p_offset_int;
  void* p_void_stop = p_void_start + 28;
  void* p_void_next;
  double rarad;
  double decrad;
  double cosra;
  double sinra;
  double cosdec;
  double sindec;
  int L;
    p_void_next = p_void_start + (L=0);
    while (p_void_next < p_void_stop) {
      L = read(sockfd, p_void_next, (int)(p_void_stop - p_void_next));
      if (1>L) {
        rtn = (L==0 && p_void_next == p_void_start) ? GAIA_DONE : GAIA_ERROR;
        break;
      }
      p_void_next += L;
    }
    if (1>L) break;
    ptr->seqNum = count;
    ptr->offset = *p_offset_int;
    ptr->ra     = p_doubles[1];
    ptr->dec    = p_doubles[2];
    ptr->mag    = p_doubles[3];
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

  /* Force rtn to be GAIA_DONE if hard limit was reached */
  if (count==HARDLIMIT) {
    rtn = GAIA_DONE;
  }

  /* Set NULL ->next pointer in last structure in linked list
   * - If there was at least one item returned
   * - If last step returned GAIA_DONE
   */
  if (count>0 && rtn == GAIA_DONE) {
    (ptr-1)->next = NULL;
  }

  /* cleanup ... */
  (void)close(sockfd);

  /* ... and return count on success */
  if (rtn == GAIA_DONE) {
    return count;
  }
  /* On failure (last step did not return GAIA_DONE),
   * cleanup and return negative count
   */
  free((void*) *pRtn);
  *pRtn = NULL;
  return --failRtn;
}

int
gaiaRDMselect(char* gaiaSQLfilename
             ,double himag
             ,double ralo, double rahi
             ,double declo, double dechi
             ,pTYC2rtn *pRtn
             ) {
  if (strncmp(gaiaSQLfilename,"gaia:",5)) {
    return gaiaRDMselect_sql(gaiaSQLfilename
                            ,himag
                            ,ralo, rahi
                            ,declo, dechi
                            ,pRtn
                            );
  }
  return gaiaRDMselect_net(gaiaSQLfilename
                          ,himag
                          ,ralo, rahi
                          ,declo, dechi
                          ,pRtn
                          );
}
