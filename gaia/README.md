## Gaia translation of Tycho SQLite R-Tree code

### Retrieve ESA/Gaia star catalog data, put same into SQLite3 R-Tree tables

Usage:

    python gaia.py getallgaia buildsqldb

    - See comments in gaia.py for more options
    - Limited to stars brighter than magnitude 18.6 in any of G, BP, or RP bands
    - Takes of order 1d to complete
    - Writes 251GB in two SQLite3 database files in ./gaia*.sqlite3
    - Writes 165GB in 61,234 files in ./gaiapickles/
      - These may be deleted after the databases are written

#### Gaia Data Details

Data Release 2 (DR2) CSV data are here:  http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/

Directory listing there looks like this:

    Index of /Gaia/gdr2/gaia_source/csv/

    ../
    GaiaSource_1000172165251650944_1000424567594791..> 16-Apr-2018 07:32             5347523
    GaiaSource_1000424601954531200_1000677322125743..> 16-Apr-2018 07:32             5024698
    GaiaSource_1000677386549270528_1000959999693425..> 16-Apr-2018 07:32             5976430
    ...
    GaiaSource_999535200126184320_99971696743907443..> 16-Apr-2018 10:19             5795991
    GaiaSource_999717001796824064_99992236995490496..> 16-Apr-2018 10:19             5240860
    GaiaSource_999922404314639104_10001721265966654..> 16-Apr-2018 10:19             5375567
    MD5SUM.txt                                         22-Jun-2018 13:13             5623335
    _citation.txt                                      22-May-2018 15:39                 171
    _disclaimer.txt                                    22-May-2018 15:39                 921


MD5SUM.txt contents look like this:

    d5b9bacd1fbad8d731176e0d79cf3ae8  _citation.txt
    9d7e93143bbff45c8da4bdd9915d9e41  _disclaimer.txt
    614baf41facb6c07d0ec91e5fe8a9517  GaiaSource_1000172165251650944_1000424567594791808.csv.gz
    cdb7c4000e438bf01530b3618c9b58b4  GaiaSource_1000424601954531200_1000677322125743488.csv.gz
    ...
    9f4f16d35f368bbd51721d27c0946e9d  GaiaSource_999717001796824064_999922369954904960.csv.gz
    0623700d067517f2cd20ddf306000bc8  GaiaSource_999922404314639104_1000172126596665472.csv.gz
