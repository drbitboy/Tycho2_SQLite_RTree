#include <math.h>
#include "gaialib.h"
#include "localmalloc.h"
#include "get_client_socket_fd.h"

static char gaiaSelectStmtNeither[] = { "\
SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;\
" };

/* Add light columns not in gaiaSelectStmtNeither statement above
 * - parallax, pmra, pmdec
 * - phot_bp_mean_mag, phot_rp_mean_mag
 */

static char gaiaSelectStmtLight[] = { "\
SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag \
, gaialight.parallax, gaialight.pmra, gaialight.pmdec, gaialight.phot_bp_mean_mag, gaialight.phot_rp_mean_mag \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;\
" };

/* Add heavy columns to gaiaSelectStmtNeither statement above
 * - source_id
 * - Errors
 * - Correlations
 */

static char gaiaSelectStmtHeavy[] = { "\
SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag \
, gaiaheavy.source_id \
, gaiaheavy.ra_error, gaiaheavy.dec_error, gaiaheavy.parallax_error, gaiaheavy.pmra_error, gaiaheavy.pmdec_error \
, gaiaheavy.ra_dec_corr, gaiaheavy.ra_parallax_corr, gaiaheavy.ra_pmra_corr, gaiaheavy.ra_pmdec_corr \
, gaiaheavy.dec_parallax_corr, gaiaheavy.dec_pmra_corr, gaiaheavy.dec_pmdec_corr \
, gaiaheavy.parallax_pmra_corr, gaiaheavy.parallax_pmdec_corr \
, gaiaheavy.pmra_pmdec_corr \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
INNER JOIN gaiaheavy ON gaiartree.idoffset=gaiaheavy.idoffset \
WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;\
" };

/* Add both light abd heavy columns, light first */

static char gaiaSelectStmtBoth[] = { "\
SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag \
, gaialight.parallax, gaialight.pmra, gaialight.pmdec, gaialight.phot_bp_mean_mag, gaialight.phot_rp_mean_mag \
, gaiaheavy.source_id \
, gaiaheavy.ra_error, gaiaheavy.dec_error, gaiaheavy.parallax_error, gaiaheavy.pmra_error, gaiaheavy.pmdec_error \
, gaiaheavy.ra_dec_corr, gaiaheavy.ra_parallax_corr, gaiaheavy.ra_pmra_corr, gaiaheavy.ra_pmdec_corr \
, gaiaheavy.dec_parallax_corr, gaiaheavy.dec_pmra_corr, gaiaheavy.dec_pmdec_corr \
, gaiaheavy.parallax_pmra_corr, gaiaheavy.parallax_pmdec_corr \
, gaiaheavy.pmra_pmdec_corr \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
INNER JOIN gaiaheavy ON gaiartree.idoffset=gaiaheavy.idoffset \
WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;\
" };

#define HARDLIMIT 1024


/***********************************************************************
 * Use SQLite3 database; gaiaSQLfilename is gaia.sqlite3 or similar
 */
int
gaiaRDMselect_sql(char* gaiaSQLfilename
                 ,double himag
                 ,double ralo, double rahi
                 ,double declo, double dechi
                 ,pTYC2rtn *pTyc2
                 ,pGAIAlightrtn *pGAIAlight
                 ,pGAIAheavyrtn *pGAIAheavy
                 ) {

double rpd = atan(1.0) / 45.0;

double arg6[6] = { himag, ralo, rahi, declo, dechi, himag };

sqlite3_stmt *pStmt = NULL;        // Pointer to repared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB

int i;
int count;
int rtn = 0;
int failRtn = 0;

char* gaiaSelectStmt = pGAIAlight ? (pGAIAheavy ? gaiaSelectStmtBoth : gaiaSelectStmtLight)
                                  : (pGAIAheavy ? gaiaSelectStmtHeavy : gaiaSelectStmtNeither)
                                  ;

pTYC2rtn ptrBASE;                      // pointer to a struct in linked list
pGAIAlightrtn ptrGAIAlight;            // pointer to a struct in linked list
pGAIAheavyrtn ptrGAIAheavy;            // pointer to a struct in linked list

  *pTyc2 = NULL;
  if (pGAIAlight) { *pGAIAlight = NULL; }
  if (pGAIAheavy) { *pGAIAheavy = NULL; }

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

  /* malloc arrays of size HARDLIMIT of return structures; return on fail */
  --failRtn;
  *pTyc2 = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
  if (!*pTyc2) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  --failRtn;
  if (pGAIAlight) {
    *pGAIAlight = (pGAIAlightrtn) malloc( HARDLIMIT * sizeof(GAIAlightrtn) );
    if (!*pGAIAlight) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      free((void*)*pTyc2);
      return failRtn;
    }
    /* Set loop pointer to beginning of list */
    ptrGAIAlight = *pGAIAlight;
  }

  --failRtn;
  if (pGAIAheavy) {
    *pGAIAheavy = (pGAIAheavyrtn) malloc( HARDLIMIT * sizeof(GAIAheavyrtn) );
    if (!*pGAIAheavy) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      if (pGAIAlight) { free((void*)*pGAIAlight); }
      free((void*)*pTyc2);
      return failRtn;
    }
    /* Set loop pointer to beginning of list */
    ptrGAIAheavy = *pGAIAheavy;
  }

  /* Loop over rows, save results */
  for (ptrBASE = *pTyc2+(count=0)
      ; count < HARDLIMIT && SQLITE_ROW == (rtn = sqlite3_step(pStmt))
      ; ++count, ptrBASE=ptrBASE->next
      ) {
  double rarad;
  double decrad;
  double cosra;
  double sinra;
  double cosdec;
  double sindec;
  int icolumn;
    ptrBASE->seqNum = count;
    icolumn = 0;
    ptrBASE->offset = sqlite3_column_int(pStmt, icolumn++);
    ptrBASE->ra     = sqlite3_column_double(pStmt, icolumn++);
    ptrBASE->dec    = sqlite3_column_double(pStmt, icolumn++);
    ptrBASE->mag    = sqlite3_column_double(pStmt, icolumn++);
    rarad = rpd * ptrBASE->ra;
    decrad = rpd * ptrBASE->dec;
    cosra = cos(rarad);
    sinra = sin(rarad);
    cosdec = cos(decrad);
    sindec = sin(decrad);
    ptrBASE->_xyz[0] = cosdec * cosra;
    ptrBASE->_xyz[1] = cosdec * sinra;
    ptrBASE->_xyz[2] = sindec;
    /* ->catalogORsuppl1 and ->catline:  not used for GAIA */
    *ptrBASE->catalogORsuppl1 = *ptrBASE->catline = '\0';

#   define GETCOLDOUBLE(PTR_MBR) GETCOL(PTR_MBR,sqlite3_column_double)

#   define GETCOL(PTR_MBR,COLFUNC) \
    PTR_MBR ## _is_null = (SQLITE_NULL==sqlite3_column_type(pStmt, icolumn)); \
    PTR_MBR = COLFUNC(pStmt,icolumn++)

    /* Retrieve GAIA light data, if requested */
    /* , gaialight.parallax, gaialight.pmra, gaialight.pmdec, gaialight.phot_bp_mean_mag, gaialight.phot_rp_mean_mag */
    if (pGAIAlight) {

      /* Get items from ptrBASE */
      ptrGAIAlight->ra = ptrBASE->ra;
      ptrGAIAlight->dec = ptrBASE->dec;
      ptrGAIAlight->phot_g_mean_mag = ptrBASE->mag;

      GETCOLDOUBLE(ptrGAIAlight->parallax);
      GETCOLDOUBLE(ptrGAIAlight->pmra);
      GETCOLDOUBLE(ptrGAIAlight->pmdec);
      GETCOLDOUBLE(ptrGAIAlight->phot_bp_mean_mag);
      GETCOLDOUBLE(ptrGAIAlight->phot_rp_mean_mag);

      ptrGAIAlight->next = ptrGAIAlight + 1;
      ++ptrGAIAlight;
    }

    /* Retrieve GAIA heavy data, if requested */
    /* , gaiaheavy.source_id \
     * , gaiaheavy.ra_error, gaiaheavy.dec_error, gaiaheavy.parallax_error, gaiaheavy.pmra_error, gaiaheavy.pmdec_error \
     * , gaiaheavy.ra_dec_corr, gaiaheavy.ra_parallax_corr, gaiaheavy.ra_pmra_corr, gaiaheavy.ra_pmdec_corr \
     * , gaiaheavy.dec_parallax_corr, gaiaheavy.dec_pmra_corr, gaiaheavy.dec_pmdec_corr \
     * , gaiaheavy.parallax_pmra_corr, gaiaheavy.parallax_pmdec_corr \
     * , gaiaheavy.pmra_pmdec_corr \
     */
    if (pGAIAheavy) {

      GETCOL(ptrGAIAheavy->source_id,sqlite3_column_int64);
      GETCOLDOUBLE(ptrGAIAheavy->ra_error);
      GETCOLDOUBLE(ptrGAIAheavy->dec_error);
      GETCOLDOUBLE(ptrGAIAheavy->parallax_error);
      GETCOLDOUBLE(ptrGAIAheavy->pmra_error);
      GETCOLDOUBLE(ptrGAIAheavy->pmdec_error);
      GETCOLDOUBLE(ptrGAIAheavy->ra_dec_corr);
      GETCOLDOUBLE(ptrGAIAheavy->ra_parallax_corr);
      GETCOLDOUBLE(ptrGAIAheavy->ra_pmra_corr);
      GETCOLDOUBLE(ptrGAIAheavy->ra_pmdec_corr);
      GETCOLDOUBLE(ptrGAIAheavy->dec_parallax_corr);
      GETCOLDOUBLE(ptrGAIAheavy->dec_pmra_corr);
      GETCOLDOUBLE(ptrGAIAheavy->dec_pmdec_corr);
      GETCOLDOUBLE(ptrGAIAheavy->parallax_pmra_corr);
      GETCOLDOUBLE(ptrGAIAheavy->parallax_pmdec_corr);
      GETCOLDOUBLE(ptrGAIAheavy->pmra_pmdec_corr);

      ptrGAIAheavy->next = ptrGAIAheavy + 1;
      ++ptrGAIAheavy;
    }

    ptrBASE->next = ptrBASE + 1;   // ->next pointer even though this is an array
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
    (ptrBASE-1)->next = NULL;
    if (pGAIAlight) { (ptrGAIAlight-1)->next = NULL; }
    if (pGAIAheavy) { (ptrGAIAheavy-1)->next = NULL; }
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
  free((void*) *pTyc2);
  free((void*) *pTyc2);
  *pTyc2 = NULL;
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
                 ,pTYC2rtn *pTyc2
                 ,pGAIAlightrtn *pGAIAlight
                 ,pGAIAheavyrtn *pGAIAheavy
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

  *pTyc2 = NULL;

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
  *pTyc2 = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
  if (!*pTyc2) {
    (void)close(sockfd);
    return failRtn;
  }

  /* Loop over rows, save results */
  --failRtn;
  for (ptr = *pTyc2+(count=0)
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
  free((void*) *pTyc2);
  *pTyc2 = NULL;
  return --failRtn;
}

/***********************************************************************
 *** Convenience wrappers for routines above                         ***
 **********************************************************************/
int
gaiaRDMselect3(char* gaiaSQLfilename
              ,double himag
              ,double ralo, double rahi
              ,double declo, double dechi
              ,pTYC2rtn *pTyc2
              ,pGAIAlightrtn *pGAIAlight
              ,pGAIAheavyrtn *pGAIAheavy
              ) {
  if (!gaiaSQLfilename) { return 0; }
  if (strncmp(gaiaSQLfilename,"gaia:",5)) {
    /* Filename is not in network server form (gaia:<host>[/<port>),
     * assume it is a local file, use libsqlite3
     */
    return gaiaRDMselect_sql(gaiaSQLfilename
                            ,himag
                            ,ralo, rahi
                            ,declo, dechi
                            ,pTyc2
                            ,pGAIAlight
                            ,pGAIAheavy
                            );
  }
  /* Filename is in network client/server form, gaia:<host>[/<port>],
   * call server
   */
  return gaiaRDMselect_net(gaiaSQLfilename
                          ,himag
                          ,ralo, rahi
                          ,declo, dechi
                          ,pTyc2
                          ,pGAIAlight
                          ,pGAIAheavy
                          );
}
