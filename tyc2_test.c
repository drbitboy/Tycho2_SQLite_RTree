#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include "tyc2lib.h"

/*    Limits (Pleiades): */
static double himag =  6.5;
static double lora  = 56.0;
static double hira  = 58.0;
static double lodec = 23.0;
static double hidec = 25.0;

int main() {
int i = 0;
pTYC2rtn pRtn = NULL;
int count = tyc2RDMselect( "tyc2.sqlite3"
                         , himag, lora, hira, lodec, hidec, "catalog"
                         , &pRtn
                         );

  /* Print header, loop over rows printing out results */
  printf(" Offset      X      Y      Z   Magn.\n");
  if (count > 0 && pRtn) {
  pTYC2rtn ptr;
    for (ptr=pRtn+(i=0); ptr; ++i, ptr=ptr->next) {
      printf( "%7d %6.3f %6.3f %6.3f %7.3f\n"
            , ptr->offset
            , ptr->xyz[0], ptr->xyz[1], ptr->xyz[2]
            , ptr->mag
            );
    }
  }
  /* cleanup and return */
  if (pRtn) free(pRtn);
  return count < 0 ? count : (i==count ? 0 : 1);
}
