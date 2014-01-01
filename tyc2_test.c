#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "localmalloc.h"
#include "tyc2lib.h"

/*    Limits (Pleiades):  passed to tyc2RDMselect */
static double himag =  6.5;
static double lora  = 56.0;
static double hira  = 58.0;
static double lodec = 23.0;
static double hidec = 25.0;


/***********************************************************************
 * searcher():  a local subroutine to query SQLite3 DB of Tycho-2 using
 * tyc2RDMselect() library routine
 * 
 * - catORsp1 is either "catalog" or "suppl1"
 *   - only first character is relevant, and it's only checked for 'c'
 * 
 *   - returns count of stars found, and
 *   - sets pointer pRtn to malloc'ed linked list of structures
 */
int searcher(char *catORsp1) {
int i = 0;
pTYC2rtn pRtn = NULL;
int count;

  /* Call tyc2RDMselect:  TYCho-2 Ra/Dec/Magnitude sql SELECT
   * - available via tyc2lib.c
   */
  count = tyc2RDMselect( "tyc2.sqlite3"
                         , himag, lora, hira, lodec, hidec, catORsp1
                         , &pRtn
                         );

  if (count > 0 && pRtn) {
  pTYC2rtn ptr;

    /* loop over linked list of structures;
     * - pRtn is pointer to first in linked list
     * - member "->next" points to next structure
     * - ptr->next of last structure is NULL
     */
    for (ptr=pRtn+(i=0); ptr; ++i, ptr=ptr->next) {

      /* print out data, in structure pointed to by ptr, for one star */
      printf( "  %c%7ld %6.3f %6.3f %6.3f %7.3f\n"
            , *catORsp1     // 'c' or 's'; i.e. first char of "catalog" or "suppl1"
            , ptr->offset   // Offset of record in catalog.dat or in suppl_1.dat
            , ptr->_xyz[0]   // X-component of unit vector pointing at RA,DEC of start
            , ptr->_xyz[1]   // Y-component  "
            , ptr->_xyz[2]   // Z-component  "
            , ptr->mag      // Magnitude:  B or V or Hp (suppl1)
            );
      //fprintf( stderr, "%d:%ld:<%s>\n", tyc2_getCatline("tyc2.sqlite3",ptr),ptr->offset,ptr->catline);
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
  printf("C|S Offset      X      Y      Z   Magn.\n");

  /* Search catalog.dat via searcher() above; will return 0 on success ... */

  rtn = searcher("catalog");

  /* ... if that was successful, then serach suppl_1.dat ... */
  if ( !rtn ) {

    himag = 9.0;     // suppl_1.dat has fewer stars:  loosen magnitude limit

    rtn = searcher("suppl1");   // Search suppl_1.dat
  }

  return rtn;
}
