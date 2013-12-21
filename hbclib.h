#ifndef __STARLIB_H__
#define __STARLIB_H__

#include "sqlite3.h"
#include "tyc2lib.h"

typedef TYC2rtn STARrtn;
typedef pTYC2rtn pSTARrtn;

/*
typedef struct TYC2rtnStruct {
  int seqNum;
  long offset;
  double _xyz[3];
  double mag;
  double ra;
  double dec;
  char catalogORsuppl1[10];
  char catline[250];
  struct TYC2rtnStruct *next;
} TYC2rtn, *pTYC2rtn;
 */

static char* hbcStmtFmt = { "SELECT starrtree.offset ,stardata.x ,stardata.y ,stardata.z ,starrtree.lomag FROM %srtree AS starrtree INNER JOIN %sdata AS stardata ON stardata.offset=starrtree.offset WHERE starrtree.himag>? AND starrtree.lomag<? AND starrtree.hira>? AND starrtree.lora<? AND starrtree.hidec>? AND starrtree.lodec<? ORDER BY starrtree.lomag ASC;" };

static char* hbcCountStmtFmt = { "SELECT count(*) FROM %srtree AS starrtree WHERE starrtree.himag>? AND starrtree.lomag<? AND starrtree.hira>? AND starrtree.lora<? AND starrtree.hidec>? AND starrtree.lodec<?;" };

static char* hbcPathLookupStmtFmt = { "SELECT fullpath FROM %spaths WHERE key=? LIMIT 1;" };

int hbcRDMselect( char* hbcSQLfilename
                 , char *hbcpfx
                 , double lomag, double himag
                 , double lora, double hira
                 , double lodec, double hidec
                 , pSTARrtn *pRtn);

int hbc_getCatline( char* hbcSQLfilename, pSTARrtn starList);

#ifndef SKIP_SIMBAD
#include <stdio.h>
#include <string.h>
#include <curl/curl.h>

/* Simple URL call to SIMBAD to get basic info written to STDERR
 * 
 * This research has made use of the SIMBAD database,
 *     operated at CDS, Strasbourg, France
 */
int hbc_simbad( FILE* stdFile   // Where to write data from SIMBAD
              , char *hbcpfx    // Catalog e.g. bsc5
              , int offset      // Offset from first record; add to firstRecord to get ID
              );
#endif // ifndef SKIP_SIMBAD
#endif // ifndef __STARLIB_H__
