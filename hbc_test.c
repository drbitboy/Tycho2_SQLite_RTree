#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include "hbclib.h"

/*    Limits (Pleiades):  passed to hbcRDMselect */
static double lomag = -9.9;
static double himag =  6.5;
static double lora  = 56.0;
static double hira  = 58.0;
static double lodec = 23.0;
static double hidec = 25.0;

static char sqlite3db[] = { "bsc5.sqlite3" };

/***********************************************************************
 * searcher():  a local subroutine to query SQLite3 DB of Tycho-2 using
 * hbcRDMselect() library routine
 * 
 * - catORsp1 is either "catalog" or "suppl1"
 *   - only first character is relevant, and it's only checked for 'c'
 * 
 *   - returns count of stars found, and
 *   - sets pointer pRtn to malloc'ed linked list of structures
 */
int searcher(char* hbcpfx) {
int i = 0;
pSTARrtn pRtn = NULL;
int count;

  /* Call hbcRDMselect:  BSC5 (Yale Bright Star Catalog) Ra/Dec/Magnitude sql SELECT
   * - available via hbclib.c
   */
  count = hbcRDMselect( sqlite3db
                        , hbcpfx
                        , lomag, himag, lora, hira, lodec, hidec
                        , &pRtn
                        );

  if (count > 0 && pRtn) {
  pSTARrtn ptr;

    /* loop over linked list of structures;
     * - pRtn is pointer to first in linked list
     * - member "->next" points to next structure
     * - ptr->next of last structure is NULL
     */
    for (ptr=pRtn+(i=0); ptr; ++i, ptr=ptr->next) {

      /* print out data, in structure pointed to by ptr, for one star */
      printf( "%9s %7ld %6.3f %6.3f %6.3f %7.3f\n"
            , hbcpfx          // bsc5 or something similar
            , ptr->offset   // Offset of record in catalog.dat or in suppl_1.dat
            , ptr->_xyz[0]   // X-component of unit vector pointing at RA,DEC of start
            , ptr->_xyz[1]   // Y-component  "
            , ptr->_xyz[2]   // Z-component  "
            , ptr->mag      // Magnitude:  B or V or Hp (suppl1)
            );
    }
    if ( getenv("HBC_TEST_DO_SIMBAD") ) {
      for (ptr=pRtn+(i=0); ptr; ++i, ptr=ptr->next) {
        hbc_simbad( stdout, hbcpfx, ptr->offset );
      }
    }
  }
  if (pRtn) free(pRtn);                           // cleanup
  return count < 0 ? count : (i==count ? 0 : 1);  // return 0 for success
}


/***********************************************************************
 * Main routine:  print header; search catalog.dat; search suppl_1.dat
 */
int main() {
int rtn;

  /* Print header */
  printf("TableName  Offset      X      Y      Z   Magn.\n");

  /* Search catalog.dat via searcher() above; will return 0 on success ... */

  rtn = searcher("bsc5");

  return rtn;
}
