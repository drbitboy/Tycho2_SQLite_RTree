## Gaia translation of Tycho SQLite R-Tree code

----
### Retrieve ESA/Gaia star catalog data, put same into SQLite3 R-Tree tables

Resulting SQLite3 database files enable fast access by RA, Dec and Magnitude


----
### Usage

    python gaia.py getallgaia buildsqlitedb

* Takes order 1d to complete
* See comments in gaia.py for more options
* Limited to stars brighter than magnitude 18.6 (default) in any of G, BP, or RP bands
  * Includes about 803Mstars i.e. roughly half of the total Gaia data set.
  * This may be changed
* Writes 251GB in two SQLite3 database files in ./gaia*.sqlite3
* Writes 5MB+ file csv/MD5SUM.txt
* Writes 165GB in 61,234 files in ./gaiapickles/
  * These may be deleted after the databases are written




----
### SQLite3 typical query

The design typical query, e.g.  to retrieve all stars darker than 18th magnitude in the spatial "box" defined by

    [123 < RA < 124]

and

    [-45 < Dec < -44]

will be something like this:

    ATTACH 'gaia.sqlite3' AS light;

    ATTACH 'gaia_heavy.sqlite3' AS heavy;

    SELECT light.gaiartree.ralo
         , light.gaiartree.declo

         , light.gaialight.phot_g_mean_mag
         , light.gaialight.phot_bp_mean_mag
         , light.gaialight.phot_rp_mean_mag

         , heavy.gaiaheavy.source_id
         , heavy.gaiaheavy.offset

    FROM light.gaiartree

    INNER JOIN light.gaialight
    ON light.gaiartree.offset=light.gaialight.offset

    INNER JOIN heavy.gaiaheavy
    ON light.gaiartree.offset=heavy.gaiaheavy.offset

    WHERE light.gaiartree.ralo>123.0
      AND light.gaiartree.rahi<124.0
      AND light.gaiartree.declo>-45.0
      AND light.gaiartree.dechi<-44.0
      AND light.gaiartree.lomag>18.0

    ORDER BY light.gaialight.phot_g_mean_mag DESC
           , light.gaialight.phot_bp_mean_mag DESC
           , light.gaialight.phot_rp_mean_mag DESC

    LIMIT 10
    ;

The data are split into two database files (DBs):  light and heavy.  All tables in both DBs contain the same number of rows, i.e. one per star.  All tables have a column called 'offset' that is a 32-bit INTeger and the PRIMARY KEYi for the table; it is intended to be used for JOINs.  The light table can be used on its own, and as the heavy data may not be needed for all applications, it was kept separate to save disk space for those applications.

The light DB is gaia.sqlite3, and contains the R-Tree table for lookups and a light table for additional spatial and magnitude data (parallax, proper motion; magnitude details).  In the R-Tree table, the three data values, RA, Dec and Magnitude are each represented as a pair of low and high values.  In practice the low and high for RA values are the same, as they are for the Dec value.  The magnitudes pair, maglo and maghi, are the minimum and the maximum of the magnitude for each the three bands, G, BP, and RP, although for about 10% of the stars there are no BP or RP data and the single G magnitude is used for the both values of the magnitude pair.i

#### N.B. the R-Tree schema does not include the third dimension, range, implicit in the the three-dimensional nature of the Gaia dataset.

The heavy DB is gaia_heavy.sqlite3, and contains error and correlation data.  As noted above it does not need to be present to use the light DB.

----
### SQLite3 schema

    SELECT  from 

    $ sqlite3 othergaia/gaia.sqlite3 

    SQLite version 3.31.1 2020-01-27 19:55:54

    sqlite> .schema

    CREATE VIRTUAL TABLE gaiartree using rtree(offset,ralo,rahi,declo,dechi,lomag,himag)
    /* gaiartree("offset",ralo,rahi,declo,dechi,lomag,himag) */;
    CREATE TABLE IF NOT EXISTS "gaiartree_rowid"(rowid INTEGER PRIMARY KEY,nodeno);
    CREATE TABLE IF NOT EXISTS "gaiartree_node"(nodeno INTEGER PRIMARY KEY,data);
    CREATE TABLE IF NOT EXISTS "gaiartree_parent"(nodeno INTEGER PRIMARY KEY,parentnode);
    CREATE TABLE gaialight
    (offset INT PRIMARY KEY
    ,parallax REAL DEFAULT NULL
    ,pmra REAL DEFAULT NULL
    ,pmdec REAL DEFAULT NULL
    ,phot_g_mean_mag REAL NOT NULL
    ,phot_bp_mean_mag REAL DEFAULT NULL
    ,phot_rp_mean_mag REAL DEFAULT NULL
    );

    $ sqlite3 othergaia/gaia_heavy.sqlite3 

    sqlite> .schema

    CREATE TABLE gaiaheavy
    (offset INT PRIMARY KEY
    ,source_id BIGINT NOT NULL
    ,ra_error REAL DEFAULT NULL
    ,dec_error REAL DEFAULT NULL
    ,parallax_error REAL DEFAULT NULL
    ,pmra_error REAL DEFAULT NULL
    ,pmdec_error REAL DEFAULT NULL
    ,ra_dec_corr REAL DEFAULT NULL
    ,ra_parallax_corr REAL DEFAULT NULL
    ,ra_pmra_corr REAL DEFAULT NULL
    ,ra_pmdec_corr REAL DEFAULT NULL
    ,dec_parallax_corr REAL DEFAULT NULL
    ,dec_pmra_corr REAL DEFAULT NULL
    ,dec_pmdec_corr REAL DEFAULT NULL
    ,parallax_pmra_corr REAL DEFAULT NULL
    ,parallax_pmdec_corr REAL DEFAULT NULL
    ,pmra_pmdec_corr REAL DEFAULT NULL
    );




----
### Other scripts

* count_gaia.py - Test script of GAIA pickle file data; search GAIA star database using RA/DEC boxes approximately one square degree in size; accumulate count of stars found
* count_pickled_stars_by_mag.py - Test script of SQLite3 database tables; counts stars in table and at different magnitudes
* count_pickled_stars.py - Test script of GAIA pickle file code; sum number of rows in GAIA pickle files to get total
* overlap_check_gaia.py - Test script of GAIA pickle file data; verify that SourceIDs do not overlap between GAIA CSV files
* plot_count_gaia.py - Test script of GAIA pickle file data; parse and plot output from count_gaia.py
* regression_test.py - Test script of GAIA SQLite3 R-Tree database; compare actual and expected results
* timings.py - Download performance metrics; evaluate and plot time it takes for each GAIA CSV file to be downloaded and pickled
* vork.py - Experimenting with os.fork; used to make gaia.py run faster

N.B.
* See in-script comments for usage and more detail



----
### Gaia Data Details

Data Release 2 (DR2) GZIPped CSV data files are here:  http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/

Un-gzipped CSV files look like this:

    solution_id,designation,source_id,random_index,ref_epoch,ra,ra_error,dec,...
    1635721458409799680,Gaia DR2 1000225938242805248,1000225938242805248,1197051105,2015.5,103.4475289523685,0.04109941963375859,56.02202543042615,...
    1635721458409799680,Gaia DR2 1000383512003001728,1000383512003001728,598525552,2015.5,105.1878559403631,0.016977551270711513,56.267982095887305,...
    ...
    1635721458409799680,Gaia DR2 1000261427557282560,1000261427557282560,970423763,2015.5,103.57468883714748,0.5514044545536544,56.43187569659445,...
    1635721458409799680,Gaia DR2 1000289190225324288,1000289190225324288,1316347764,2015.5,103.16378347344589,0.1503843480666058,56.88941759502849,...,33.84311361378403,,,,,,,,,,,,,,,,,

N.B.
* Decimal numbers in [source_id] data column contain each star's Level 12 HEALpix value in bits 35-62, but those HEALpix values are from the initial instrument observation, not from final RA and DEC solution, but those are from the initial instrument observation, not from final RA and DEC
  * Refer to Gaia web site for more data about source_id values
* Some fields are empty (adjacent commas)
* All rows have [Gaia DR2 <source ID>] as second column, [designation]
* All rows have 2015.5 in ref_epoch column


Directory listing there looks like this:

    Index of /Gaia/gdr2/gaia_source/csv/

    ../
    GaiaSource_1000172165251650944_1000424567594791..> 16-Apr-2018 07:32             5347523
    GaiaSource_1000424601954531200_1000677322125743..> 16-Apr-2018 07:32             5024698
    ...
    GaiaSource_999717001796824064_99992236995490496..> 16-Apr-2018 10:19             5240860
    GaiaSource_999922404314639104_10001721265966654..> 16-Apr-2018 10:19             5375567
    MD5SUM.txt                                         22-Jun-2018 13:13             5623335
    _citation.txt                                      22-May-2018 15:39                 171
    _disclaimer.txt                                    22-May-2018 15:39                 921

N.B.
* Decimal numbers in file names comprise each file's range of [source_id] data column values


MD5SUM.txt contents look like this:

    d5b9bacd1fbad8d731176e0d79cf3ae8  _citation.txt
    9d7e93143bbff45c8da4bdd9915d9e41  _disclaimer.txt
    614baf41facb6c07d0ec91e5fe8a9517  GaiaSource_1000172165251650944_1000424567594791808.csv.gz
    cdb7c4000e438bf01530b3618c9b58b4  GaiaSource_1000424601954531200_1000677322125743488.csv.gz
    ...
    9f4f16d35f368bbd51721d27c0946e9d  GaiaSource_999717001796824064_999922369954904960.csv.gz
    0623700d067517f2cd20ddf306000bc8  GaiaSource_999922404314639104_1000172126596665472.csv.gz

