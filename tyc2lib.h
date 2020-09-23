#ifndef __TYC2LIB_H__
#define __TYC2LIB_H__

#include "sqlite3.h"

static char STATIC_CATALOG[] = { "catalog" };
static char STATIC_SUPPL1[] = { "suppl1" };

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
} TYC2rtn, *pTYC2rtn, **ppTYC2rtn;

static char* catalogStmt = { "SELECT tyc2catalog_uvs.offset ,tyc2catalog_uvs.x ,tyc2catalog_uvs.y ,tyc2catalog_uvs.z ,tyc2catalog_uvs.mag FROM tyc2indexrtree INNER JOIN tyc2index ON tyc2indexrtree.offset=tyc2index.offset INNER JOIN tyc2catalog_uvs ON tyc2index.catalogstart<=tyc2catalog_uvs.offset AND tyc2index.catalogend>tyc2catalog_uvs.offset AND tyc2catalog_uvs.mag<? WHERE tyc2indexrtree.offset=tyc2index.offset AND tyc2indexrtree.hira>? AND tyc2indexrtree.lora<? AND tyc2indexrtree.hidec>? AND tyc2indexrtree.lodec<? ORDER BY tyc2catalog_uvs.mag asc;" };

static char* catalogCountStmt = { "SELECT count(0) FROM tyc2indexrtree INNER JOIN tyc2index ON tyc2indexrtree.offset=tyc2index.offset INNER JOIN tyc2catalog_uvs ON tyc2index.catalogstart<=tyc2catalog_uvs.offset AND tyc2index.catalogend>tyc2catalog_uvs.offset AND tyc2catalog_uvs.mag<? WHERE tyc2indexrtree.offset=tyc2index.offset AND tyc2indexrtree.hira>? AND tyc2indexrtree.lora<? AND tyc2indexrtree.hidec>? AND tyc2indexrtree.lodec<? ORDER BY tyc2catalog_uvs.mag asc;" };

static char* suppl1Stmt = { "SELECT tyc2suppl1_uvs.offset ,tyc2suppl1_uvs.x ,tyc2suppl1_uvs.y ,tyc2suppl1_uvs.z ,tyc2suppl1_uvs.mag FROM tyc2indexrtree INNER JOIN tyc2index ON tyc2indexrtree.offset=tyc2index.offset INNER JOIN tyc2suppl1_uvs ON tyc2index.suppl1start<=tyc2suppl1_uvs.offset AND tyc2index.suppl1end>tyc2suppl1_uvs.offset AND tyc2suppl1_uvs.mag<? WHERE tyc2indexrtree.offset=tyc2index.offset AND tyc2indexrtree.hira>? AND tyc2indexrtree.lora<? AND tyc2indexrtree.hidec>? AND tyc2indexrtree.lodec<? ORDER BY tyc2suppl1_uvs.mag asc;" };

static char* suppl1CountStmt = { "SELECT count(0) FROM tyc2indexrtree INNER JOIN tyc2index ON tyc2indexrtree.offset=tyc2index.offset INNER JOIN tyc2suppl1_uvs ON tyc2index.suppl1start<=tyc2suppl1_uvs.offset AND tyc2index.suppl1end>tyc2suppl1_uvs.offset AND tyc2suppl1_uvs.mag<? WHERE tyc2indexrtree.offset=tyc2index.offset AND tyc2indexrtree.hira>? AND tyc2indexrtree.lora<? AND tyc2indexrtree.hidec>? AND tyc2indexrtree.lodec<? ORDER BY tyc2suppl1_uvs.mag asc;" };

static char* pathLookupStmt = { "SELECT fullpath FROM tyc2paths WHERE key=? LIMIT 1;" };

int tyc2RDMselect( char* tyc2SQLfilename, double himag
                 , double lora
                 , double hira, double lodec
                 , double hidec
                 , char *catalogORsuppl1, pTYC2rtn *pRtn);

int tyc2_getCatline( char* tyc2SQLfilename, pTYC2rtn tyc2);

#ifndef SKIP_SIMBAD
#include <stdio.h>
#include <string.h>
#include <curl/curl.h>

/* Simple URL call to SIMBAD to get basic info written to STDERR
 * 
 * This research has made use of the SIMBAD database,
 *     operated at CDS, Strasbourg, France
 */
int tyc2_simbad(FILE* stdFile, char *tyc2Record, int* tycNarg);
#endif // ifndef SKIP_SIMBAD
#endif // ifndef __TYC2LIB_H__
