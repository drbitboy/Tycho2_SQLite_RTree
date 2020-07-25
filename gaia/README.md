## Gaia translation of Tycho SQLite R-Tree code

### Retrieve ESA/Gaia star catalog data, put same into SQLite3 R-Tree tables

Usage:

    python gaia.py getallgaia buildsqldb

* See comments in gaia.py for more options
* Limited to stars brighter than magnitude 18.6 in any of G, BP, or RP bands
* Takes of order 1d to complete
* Writes 251GB in two SQLite3 database files in ./gaia*.sqlite3
* Writes 5MB+ file csv/MD5SUM.txt
* Writes 165GB in 61,234 files in ./gaiapickles/
* These may be deleted after the databases are written

#### Gaia Data Details

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
