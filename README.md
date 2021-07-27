# nfl_data_py

nfl_data_py is a Python library for interacting with NFL data sourced from nflfastR (https://github.com/nflverse/nflfastR-data/).

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install nfl_data_py.

```bash
pip install nfl_data_py
```

## Usage

```python
import nfl_data_py as nfl
```

**Working with play-by-play data**
```python
nfl.import_pbp_data(years, columns)
```
Returns play-by-play data for the years and columns specified

years
: required, list of years to pull data for (earliest available is 1999)

columns
: optional, list of columns to pull data for

```python
nfl.see_pbp_cols()
```
returns list of columns available in play-by-play dataset

**Working with weekly data**
```python
nfl.import_weekly_data(years, columns)
```
Returns weekly data for the years and columns specified

years
: required, list of years to pull data for (earliest available is 1999)

columns
: optional, list of columns to pull data for

```python
nfl.see_weekly_cols()
```
returns list of columns available in weekly dataset

**Working with seasonal data**
```python
nfl.import_seasonal_data(years)
```
Returns seasonal data, including various calculated market share stats

years
: required, list of years to pull data for (earliest available is 1999)

**Additional data imports**
```python
nfl.import_rosters(years, columns)
```
Returns roster information for years and columns specified

years
: required, list of years to pull data for (earliest available is 1999)

columns
: optional, list of columns to pull data for

```python
nfl.import_team_desc()
```
Returns dataframe with color/logo/etc information for all NFL team

```python
nfl.import_schedules(years)
```
Returns dataframe with schedule information for years specified

years: required, list of years to pull data for (earliest available is 1999)

**Additional features**
```python
nfl.clean_nfl_data(df)
```
Runs descriptive data (team name, player name, etc.) through various cleaning processes

df
: required, dataframe to be cleaned

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
