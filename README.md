# [nfetl](https://github.com/deschman/nfetl)
## Short Description
A python package for creation and maintenance of an NFL statistics database.

## Long Description
**nfetl** provides users with a straight-forward API for creating and
maintaining their own sqlite database of NFL statistics. Statistics are sourced
from [Pro-Football-Reference](https://www.pro-football-reference.com/) using
webscraping scripts.

## Disclaimer
During development of this package, consideration was taken to prevent users
of this package from overwhelming the source web server. However, It is
recommended users read the liscense before developing using this package and
carefully consider how the planned implementation may impact source
availability.

## Example
```python
from nfetl import DB
import pandas as pd

db: DB = DB()

offense_df: pd.DataFrame = pd.read_sql("SELECT * FROM offense", db.connection)
```
