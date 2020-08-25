#ifndef __GAIALIB__
#define __GAIALIB__
#include <sqlite3.h>
#include "tyc2lib.h"

int gaiaRDMselect( char* gaiaSQLfilename, double himag
                 , double lora, double hira
                 , double lodec, double hidec
                 , pTYC2rtn *pRtn);

/* The following item ar not yet used ca. 2020-08-25 */

typedef struct gaia_query_str {
  double ralo;
  double rahi;
  double declo;
  double dechi;
  double lomag;
  double himag;

  int use_ralo;
  int use_rahi;
  int use_declo;
  int use_dechi;
  int use_lomag;
  int use_himag;
} GAIA_QUERY, *pGAIA_QUERY;

typedef struct gaia_data_str {
  sqlite_int64 idoffset;
  double ra;
  double dec;
  double phot_g;
  double phot_bp;
  double phot_rp;
  double parallax;
  double pmra;
  double pmdec;

  double ra_corrected;
  double dec_corrected;
  double _uvstar[3];

  sqlite_int64 source_id;
  double ra_error;
  double dec_error;
  double parallax_error;
  double pmra_error;
  double pmdec_error;
  double ra_dec_corr;
  double ra_parallax_corr;
  double ra_pmra_corr;
  double ra_pmdec_corr;
  double dec_parallax_corr;
  double dec_pmra_corr;
  double dec_pmdec_corr;
  double parallax_pmra_corr;
  double parallax_pmdec_corr;
  double pmra_pmdec_corr;
} GAIA_DATA, *pGAIA_DATA;
#endif /* __GAIALIB__ */
