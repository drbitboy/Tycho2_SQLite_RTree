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
, heavytbl.source_id \
, heavytbl.ra_error, heavytbl.dec_error, heavytbl.parallax_error, heavytbl.pmra_error, heavytbl.pmdec_error \
, heavytbl.ra_dec_corr, heavytbl.ra_parallax_corr, heavytbl.ra_pmra_corr, heavytbl.ra_pmdec_corr \
, heavytbl.dec_parallax_corr, heavytbl.dec_pmra_corr, heavytbl.dec_pmdec_corr \
, heavytbl.parallax_pmra_corr, heavytbl.parallax_pmdec_corr \
, heavytbl.pmra_pmdec_corr \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
INNER JOIN heavydb.gaiaheavy heavytbl ON gaiartree.idoffset=heavytbl.idoffset \
WHERE gaiartree.lomag<? AND gaiartree.rahi>? AND gaiartree.ralo<? AND gaiartree.dechi>?  AND gaiartree.declo<?  and gaialight.phot_g_mean_mag<? ORDER BY gaialight.phot_g_mean_mag ASC ;\
" };

/* Add both light abd heavy columns, light first */

static char gaiaSelectStmtBoth[] = { "\
SELECT gaiartree.idoffset, gaialight.ra, gaialight.dec, gaialight.phot_g_mean_mag \
, gaialight.parallax, gaialight.pmra, gaialight.pmdec, gaialight.phot_bp_mean_mag, gaialight.phot_rp_mean_mag \
, heavytbl.source_id \
, heavytbl.ra_error, heavytbl.dec_error, heavytbl.parallax_error, heavytbl.pmra_error, heavytbl.pmdec_error \
, heavytbl.ra_dec_corr, heavytbl.ra_parallax_corr, heavytbl.ra_pmra_corr, heavytbl.ra_pmdec_corr \
, heavytbl.dec_parallax_corr, heavytbl.dec_pmra_corr, heavytbl.dec_pmdec_corr \
, heavytbl.parallax_pmra_corr, heavytbl.parallax_pmdec_corr \
, heavytbl.pmra_pmdec_corr \
FROM gaiartree \
INNER JOIN gaialight ON gaiartree.idoffset=gaialight.idoffset \
INNER JOIN heavydb.gaiaheavy heavytbl ON gaiartree.idoffset=heavytbl.idoffset \
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
                 ,ppTYC2rtn ppTyc2
                 ,ppGAIAlightrtn ppGAIAlight
                 ,ppGAIAheavyrtn ppGAIAheavy
                 ) {

double rpd = atan(1.0) / 45.0;

double arg6[6] = { himag, ralo, rahi, declo, dechi, himag };

sqlite3_stmt *pStmt = NULL;        // Pointer to prepared statement
sqlite3 *pDb = NULL;               // Pointer to connection to SQLite2 DB

int i;
int count;
int rtn = 0;
int failRtn = 0;

char* gaiaSelectStmt = ppGAIAlight ? (ppGAIAheavy ? gaiaSelectStmtBoth : gaiaSelectStmtLight)
                                   : (ppGAIAheavy ? gaiaSelectStmtHeavy : gaiaSelectStmtNeither)
                                   ;

pTYC2rtn ptrBASE;                      // pointer to a struct in linked list
pGAIAlightrtn ptrGAIAlight;            // pointer to a struct in linked list
pGAIAheavyrtn ptrGAIAheavy;            // pointer to a struct in linked list

  *ppTyc2 = NULL;
  if (ppGAIAlight) { *ppGAIAlight = NULL; }
  if (ppGAIAheavy) { *ppGAIAheavy = NULL; }

  /* open DB; return on fail */
  --failRtn;
  if (SQLITE_OK != sqlite3_open_v2( gaiaSQLfilename, &pDb, SQLITE_OPEN_READONLY, (char *) 0)) {
    if (pDb) sqlite3_close(pDb);
    return failRtn;
  }

  /* If heavy data needed, then attach heavy database
   * - Heavy database filename replaces ".sqlite3" at end of light
   *   filename (gaiaSQLfilename dummy argument) with "_heavy.sqlite3"
   */
  --failRtn;
  if (ppGAIAheavy) {
  char attach_fmt[] = { "ATTACH DATABASE '%s_heavy.sqlite3' AS heavydb ;" };
  int len_fmt = strlen(attach_fmt);
  char* attach_stmt;
  int len_filename = strlen(gaiaSQLfilename);
  char* heavy_prefix = (char*) 0;
  sqlite3_stmt *pAttachStmt = NULL;        // Pointer to prepared statement
    /* Gaia filename must end with ".sqlite3" in last 8 chars */
    if (len_filename < 8) {
      sqlite3_close(pDb);
      return failRtn;
    }
    if (strcmp(".sqlite3",gaiaSQLfilename+len_filename-8)) {
      sqlite3_close(pDb);
      return failRtn;
    }

    if (!(attach_stmt=(char*)malloc((len_filename + len_fmt)* sizeof(char)))) {
      sqlite3_close(pDb);
      return failRtn;
    }

    /* Duplicate filename excluding ".sqlite3" (8 chars) on the end */
    if (!(heavy_prefix=strndup(gaiaSQLfilename, len_filename-8))) {
      free(attach_stmt);
      sqlite3_close(pDb);
      return failRtn;
    }

    sprintf(attach_stmt, attach_fmt, heavy_prefix);
    free(heavy_prefix);

    rtn = sqlite3_exec(pDb, attach_stmt, 0, 0, 0);
    free(attach_stmt);

    /* Check for failure */
    if (SQLITE_OK!=rtn) {
      sqlite3_close(pDb);
      return failRtn;
    }
  } /* if (ppGAIAheavy) */

  /* Assume non-zero count; SQL-SELECT records & place in linked list */
  /* - prepare statement; return on fail */
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
  /* - bind parameters; return on fail */
  for (i=0; i<6; ++i) {
    --failRtn;
    if (SQLITE_OK != sqlite3_bind_double( pStmt, i+1, arg6[i])) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      return failRtn;
    }
  }

  /* - malloc arrays of size HARDLIMIT of return structures; return on fail */
  /*   - Base values in TYC2rtn structure */
  --failRtn;
  *ppTyc2 = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
  if (!*ppTyc2) {
    sqlite3_finalize(pStmt);
    sqlite3_close(pDb);
    return failRtn;
  }

  /*   - Allocate GAIAlightrtn structures */
  --failRtn;
  if (ppGAIAlight) {
    *ppGAIAlight = (pGAIAlightrtn) malloc( HARDLIMIT * sizeof(GAIAlightrtn) );
    if (!*ppGAIAlight) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      free((void*)*ppTyc2);
      return failRtn;
    }
    /* Set loop pointer to beginning of list */
    ptrGAIAlight = *ppGAIAlight;
  }

  /*   - Allocate GAIAheavyrtn structures */
  --failRtn;
  if (ppGAIAheavy) {
    *ppGAIAheavy = (pGAIAheavyrtn) malloc( HARDLIMIT * sizeof(GAIAheavyrtn) );
    if (!*ppGAIAheavy) {
      sqlite3_finalize(pStmt);
      sqlite3_close(pDb);
      if (ppGAIAlight) { free((void*)*ppGAIAlight); }
      free((void*)*ppTyc2);
      return failRtn;
    }
    /* Set loop pointer to beginning of list */
    ptrGAIAheavy = *ppGAIAheavy;
  }

  /* - Loop over rows, save results */
  for (ptrBASE = *ppTyc2+(count=0)
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
    GETCOLNOISNULL(PTR_MBR,COLFUNC)

#   define GETCOLNOISNULL(PTR_MBR,COLFUNC) \
    PTR_MBR = COLFUNC(pStmt,icolumn++)

    /* Retrieve GAIA light data, if requested */
    /* , gaialight.parallax, gaialight.pmra, gaialight.pmdec, gaialight.phot_bp_mean_mag, gaialight.phot_rp_mean_mag */
    if (ppGAIAlight) {

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
    /* , heavytbl.source_id \
     * , heavytbl.ra_error, heavytbl.dec_error, heavytbl.parallax_error, heavytbl.pmra_error, heavytbl.pmdec_error \
     * , heavytbl.ra_dec_corr, heavytbl.ra_parallax_corr, heavytbl.ra_pmra_corr, heavytbl.ra_pmdec_corr \
     * , heavytbl.dec_parallax_corr, heavytbl.dec_pmra_corr, heavytbl.dec_pmdec_corr \
     * , heavytbl.parallax_pmra_corr, heavytbl.parallax_pmdec_corr \
     * , heavytbl.pmra_pmdec_corr \
     */
    if (ppGAIAheavy) {

      GETCOLNOISNULL(ptrGAIAheavy->source_id,sqlite3_column_int64);
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
    if (ppGAIAlight) { (ptrGAIAlight-1)->next = NULL; }
    if (ppGAIAheavy) { (ptrGAIAheavy-1)->next = NULL; }
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
  free((void*) *ppTyc2);
  *ppTyc2 = NULL;
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
                 ,ppTYC2rtn ppTyc2
                 ,ppGAIAlightrtn ppGAIAlight
                 ,ppGAIAheavyrtn ppGAIAheavy
                 ) {

double rpd = atan(1.0) / 45.0;

double arg6[6] = { -1.0 
                   - ((ppGAIAlight?2.0:0.0)+(ppGAIAheavy?4.0:0.0))
                 , himag, ralo, rahi, declo, dechi };

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
pGAIAlightrtn ptrGAIAlight;        // pointer to a struct in linked list
pGAIAheavyrtn ptrGAIAheavy;        // pointer to a struct in linked list

  *ppTyc2 = NULL;
  if (ppGAIAlight) { *ppGAIAlight = NULL; }
  if (ppGAIAheavy) { *ppGAIAheavy = NULL; }

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

  /* Malloc large arrays of return structures; return on fail */
  /* - TYC2 - base data */
  --failRtn;
  *ppTyc2 = (pTYC2rtn) malloc( HARDLIMIT * sizeof(TYC2rtn) );
  if (!*ppTyc2) {
    (void)close(sockfd);
    return failRtn;
  }

  /* - GAIA light */
  --failRtn;
  if (ppGAIAlight) {
    *ppGAIAlight = (pGAIAlightrtn) malloc( HARDLIMIT * sizeof(GAIAlightrtn) );
    if (!*ppGAIAlight) {
      free(*ppTyc2);
      (void)close(sockfd);
      return failRtn;
    }
    ptrGAIAlight = *ppGAIAlight;
  }

  /* - GAIA heavy */
  --failRtn;
  if (ppGAIAlight) {
    *ppGAIAheavy = (pGAIAheavyrtn) malloc( HARDLIMIT * sizeof(GAIAheavyrtn) );
    if (!*ppGAIAheavy) {
      free(*ppGAIAlight);
      free(*ppTyc2);
      (void)close(sockfd);
      return failRtn;
    }
    ptrGAIAheavy = *ppGAIAheavy;
  }

  /* Loop over rows, save results */
  --failRtn;
  for (ptr = *ppTyc2+(count=0)
      ; count < HARDLIMIT
      ; ++count, ptr=ptr->next
      ) {
  double p_doubles[50];
  sqlite_int64* p_int64 = (sqlite_int64*) p_doubles;
  int* p_offset_int = ((int*) p_doubles) + 1;
  void* p_void_start;
  void* p_void_stop;
  void* p_void_next;
  double rarad;
  double decrad;
  double cosra;
  double sinra;
  double cosdec;
  double sindec;
  int L;

    /* All cases:  4-byte integer offset; six doubles => 28 bytes */
    p_void_start = (void*) p_offset_int;
    p_void_stop = p_void_start + 28;
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
    /* Convert RA,Dec to unit Cartesian vector */
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

    if (ppGAIAlight) {
    int lightbits;
      /* GAIA light data are expected:  4-byte int; five doubles => 44 bytes */
      p_void_start = (void*) p_offset_int;
      p_void_stop = p_void_start + 44;
      p_void_next = p_void_start + (L=0);
      while (p_void_next < p_void_stop) {
        L = read(sockfd, p_void_next, (int)(p_void_stop - p_void_next));
        if (1>L) {
          rtn = GAIA_ERROR;
          break;
        }
        p_void_next += L;
      }
      if (1>L) break;

      lightbits = *p_offset_int;

      ptrGAIAlight->parallax_is_null         = (lightbits&1) ? true : false;
      ptrGAIAlight->pmra_is_null             = (lightbits&2) ? true : false;
      ptrGAIAlight->pmdec_is_null            = (lightbits&4) ? true : false;
      ptrGAIAlight->phot_bp_mean_mag_is_null = (lightbits&8) ? true : false;
      ptrGAIAlight->phot_rp_mean_mag_is_null = (lightbits&16) ? true : false;

      ptrGAIAlight->parallax         = p_doubles[1];
      ptrGAIAlight->pmra             = p_doubles[2];
      ptrGAIAlight->pmdec            = p_doubles[3];
      ptrGAIAlight->phot_g_mean_mag  = ptr->mag;
      ptrGAIAlight->phot_bp_mean_mag = p_doubles[4];
      ptrGAIAlight->phot_rp_mean_mag = p_doubles[5];

      ++ptrGAIAlight;
      (ptrGAIAlight-1)->next = ptrGAIAlight;
    } /* if (ppGAIAlight) */

    if (ppGAIAheavy) {
    int heavybits;
      /* GAIA heavy data are expected:  4-byte int; 8-byte int (source_id); 15 doubles => 132 bytes */
      p_void_start = (void*) p_offset_int;
      p_void_stop = p_void_start + 132;
      p_void_next = p_void_start + (L=0);
      while (p_void_next < p_void_stop) {
        L = read(sockfd, p_void_next, (int)(p_void_stop - p_void_next));
        if (1>L) {
          rtn = GAIA_ERROR;
          break;
        }
        p_void_next += L;
      }
      if (1>L) break;

      heavybits = *p_offset_int;

      ptrGAIAheavy->ra_error_is_null            = (heavybits&1) ? true : false;
      ptrGAIAheavy->dec_error_is_null           = (heavybits&2) ? true : false;
      ptrGAIAheavy->parallax_error_is_null      = (heavybits&4) ? true : false;
      ptrGAIAheavy->pmra_error_is_null          = (heavybits&8) ? true : false;
      ptrGAIAheavy->pmdec_error_is_null         = (heavybits&16) ? true : false;
      ptrGAIAheavy->ra_dec_corr_is_null         = (heavybits&32) ? true : false;
      ptrGAIAheavy->ra_parallax_corr_is_null    = (heavybits&64) ? true : false;
      ptrGAIAheavy->ra_pmra_corr_is_null        = (heavybits&128) ? true : false;
      ptrGAIAheavy->ra_pmdec_corr_is_null       = (heavybits&256) ? true : false;
      ptrGAIAheavy->dec_parallax_corr_is_null   = (heavybits&512) ? true : false;
      ptrGAIAheavy->dec_pmra_corr_is_null       = (heavybits&1024) ? true : false;
      ptrGAIAheavy->dec_pmdec_corr_is_null      = (heavybits&2048) ? true : false;
      ptrGAIAheavy->parallax_pmra_corr_is_null  = (heavybits&4096) ? true : false;
      ptrGAIAheavy->parallax_pmdec_corr_is_null = (heavybits&8192) ? true : false;
      ptrGAIAheavy->pmra_pmdec_corr_is_null     = (heavybits&16384) ? true : false;

      ptrGAIAheavy->ra_error            = p_doubles[2];
      ptrGAIAheavy->dec_error           = p_doubles[3];
      ptrGAIAheavy->parallax_error      = p_doubles[4];
      ptrGAIAheavy->pmra_error          = p_doubles[5];
      ptrGAIAheavy->pmdec_error         = p_doubles[6];
      ptrGAIAheavy->ra_dec_corr         = p_doubles[7];
      ptrGAIAheavy->ra_parallax_corr    = p_doubles[8];
      ptrGAIAheavy->ra_pmra_corr        = p_doubles[9];
      ptrGAIAheavy->ra_pmdec_corr       = p_doubles[10];
      ptrGAIAheavy->dec_parallax_corr   = p_doubles[11];
      ptrGAIAheavy->dec_pmra_corr       = p_doubles[12];
      ptrGAIAheavy->dec_pmdec_corr      = p_doubles[13];
      ptrGAIAheavy->parallax_pmra_corr  = p_doubles[14];
      ptrGAIAheavy->parallax_pmdec_corr = p_doubles[15];
      ptrGAIAheavy->pmra_pmdec_corr     = p_doubles[16];

      ++ptrGAIAheavy;
      (ptrGAIAheavy-1)->next = ptrGAIAheavy;
    } /* if (ppGAIAheavy) */

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
    if (ppGAIAlight) { (ptrGAIAlight-1)->next = NULL; }
    if (ppGAIAheavy) { (ptrGAIAheavy-1)->next = NULL; }
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
  free((void*) *ppTyc2);
  if (ppGAIAlight) { free((void*) *ppGAIAlight); }
  if (ppGAIAheavy) { free((void*) *ppGAIAheavy); }
  *ppTyc2 = NULL;
  return --failRtn;
} /* gaiaRDMselect_net(...) */

/***********************************************************************
 *** Convenience wrappers for routines above                         ***
 **********************************************************************/
int
gaiaRDMselect3(char* gaiaSQLfilename
              ,double himag
              ,double ralo, double rahi
              ,double declo, double dechi
              ,ppTYC2rtn ppTyc2
              ,ppGAIAlightrtn ppGAIAlight
              ,ppGAIAheavyrtn ppGAIAheavy
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
                            ,ppTyc2
                            ,ppGAIAlight
                            ,ppGAIAheavy
                            );
  }
  /* Filename is in network client/server form, gaia:<host>[/<port>],
   * call server
   */
  return gaiaRDMselect_net(gaiaSQLfilename
                          ,himag
                          ,ralo, rahi
                          ,declo, dechi
                          ,ppTyc2
                          ,ppGAIAlight
                          ,ppGAIAheavy
                          );
}
