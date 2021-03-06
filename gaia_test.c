#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "localmalloc.h"
#include "gaialib.h"

/*    Limits (Pleiades):  passed to gaiaRDMselect */
static double himag = 16.0;
static double lora  = 46.0;
static double hira  = 48.0;
static double lodec = 10.0;
static double hidec = 12.0;


/***********************************************************************
 * searcher():  a local subroutine to query SQLite3 DB of Gaia DR2 using
 * gaiaRDMselect() library routine
 * 
 *   - returns count of stars found, and
 *   - sets pointer pTYC2Rtn to malloc'ed linked list of structures
 */
int searcher() {
int i = 0;
pTYC2rtn pTYC2Rtn = NULL;
pGAIAlightrtn pGAIAlightRtn = NULL;
pGAIAheavyrtn pGAIAheavyRtn = NULL;
int count;
char* gaia_envvar = getenv("GAIA_PATH"); // gaia.sqlite3 or gaia:<hostname>[/<port#>]

  /* Call gaiaRDMselect:  Gaia DR2 Ra/Dec/Magnitude sql SELECT
   * - available via gaialib.c
   */
  count = gaiaRDMselect3( gaia_envvar ? gaia_envvar : "gaia_subset.sqlite3"
                        , himag, lora, hira, lodec, hidec
                        , &pTYC2Rtn
                        , &pGAIAlightRtn
                        , &pGAIAheavyRtn
                        );

  if (count > 0 && pTYC2Rtn && pGAIAlightRtn && pGAIAheavyRtn) {
  pTYC2rtn ptr;
  pGAIAlightrtn ptrlight = pGAIAlightRtn;
  pGAIAheavyrtn ptrheavy = pGAIAheavyRtn;

    /* loop over linked list of structures;
     * - pTYC2Rtn is pointer to first in linked list
     * - member "->next" points to next structure
     * - ptr->next of last structure is NULL
     */
    for (ptr=pTYC2Rtn+(i=0); ptr; ++i, ptr=ptr->next) {

      /* print out data, in structure pointed to by ptr, for one star */
      printf( "  %5d %10ld %6.3f %6.3f %6.3f %7.3f\n"
            , i              // Offset of record in catalog.dat or in suppl_1.dat
            , ptr->offset    // Offset of record in SQLite3 DB
            , ptr->_xyz[0]   // X-component of unit vector pointing at RA,DEC of start
            , ptr->_xyz[1]   // Y-component  "
            , ptr->_xyz[2]   // Z-component  "
            , ptr->mag      // Magnitude:  B or V or Hp (suppl1)
            );
      ptrlight = ptrlight->next;
      ptrheavy = ptrheavy->next;
    }
    if (ptrlight || ptrheavy) {
      printf("FAIL (light or heavy) count = %d\n", count);
      count = -999;
    }
  } else {
    printf("FAIL count = %d\n", count);
  }
  if (pTYC2Rtn) free(pTYC2Rtn);                           // cleanup
  return count < 0 ? count : (i==count ? 0 : 1);  // return 0 for success
}


/***********************************************************************
 * Main routine:  print header; search Gaia
 */
int main() {
int rtn;

  /* Print header */
  printf("  SeqNo     Offset      X      Y      Z   Magn.\n");

  /* Search catalog.dat via searcher() above; will return 0 on success ... */

  rtn = searcher();

  return rtn;
}
