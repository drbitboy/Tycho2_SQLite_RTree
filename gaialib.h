#ifndef __GAIALIB__
#define __GAIALIB__
#include <sqlite3.h>
#include <stdbool.h>
#include "tyc2lib.h"

typedef struct GAIAlightrtnStruct {
  double ra;
  double dec;
  double parallax;
  double pmra;
  double pmdec;
  double phot_g_mean_mag;
  double phot_bp_mean_mag;
  double phot_rp_mean_mag;

  bool parallax_is_null;
  bool pmra_is_null;
  bool pmdec_is_null;
  bool phot_bp_mean_mag_is_null;
  bool phot_rp_mean_mag_is_null;

  struct GAIAlightrtnStruct* next;
} GAIAlightrtn, *pGAIAlightrtn, **ppGAIAlightrtn;

typedef struct GAIAheavyrtnStruct {
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

  bool ra_error_is_null;
  bool dec_error_is_null;
  bool parallax_error_is_null;
  bool pmra_error_is_null;
  bool pmdec_error_is_null;
  bool ra_dec_corr_is_null;
  bool ra_parallax_corr_is_null;
  bool ra_pmra_corr_is_null;
  bool ra_pmdec_corr_is_null;
  bool dec_parallax_corr_is_null;
  bool dec_pmra_corr_is_null;
  bool dec_pmdec_corr_is_null;
  bool parallax_pmra_corr_is_null;
  bool parallax_pmdec_corr_is_null;
  bool pmra_pmdec_corr_is_null;

  struct GAIAheavyrtnStruct* next;
} GAIAheavyrtn, *pGAIAheavyrtn, **ppGAIAheavyrtn;

#define gaiaRDMselect(gaiaSQLfilename, himag, lora, hira, lodec, hidec, pRtn) \
       gaiaRDMselect3(gaiaSQLfilename, himag, lora, hira, lodec, hidec, pRtn, (ppGAIAlightrtn)0, (ppGAIAheavyrtn)0)

int
gaiaRDMselect3(char* gaiaSQLfilename
              ,double himag
              ,double ralo, double rahi
              ,double declo, double dechi
              ,ppTYC2rtn ppTyc2
              ,ppGAIAlightrtn ppGAIAlight
              ,ppGAIAheavyrtn ppGAIAheavy
              );
#endif /* __GAIALIB__ */
