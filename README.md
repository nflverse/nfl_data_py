# nfl_data_py

nfl_data_py is a Python library for interacting with NFL data sourced from [nflfastR](https://github.com/nflverse/nflfastR-data/), [nfldata](https://github.com/nflverse/nfldata/), [dynastyprocess](https://raw.githubusercontent.com/dynastyprocess/), and [Draft Scout](https://draftscout.com/).

Includes import functions for play-by-play data, weekly data, seasonal data, rosters, win totals, scoring lines, officials, draft picks, draft pick values, schedules, team descriptive info, combine results and id mappings across various sites.

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
nfl.import_pbp_data(years, columns, downcast=True)
```
Returns play-by-play data for the years and columns specified

years
: required, list of years to pull data for (earliest available is 1999)
columns
: optional, list of columns to pull data for
downcast
: converts float64 columns to float32, reducing memory usage by ~30%. Will slow down initial load speed ~50%

```python
nfl.see_pbp_cols()
```
returns list of columns available in play-by-play dataset

**Working with weekly data**
```python
nfl.import_weekly_data(years, columns, downcast)
```
Returns weekly data for the years and columns specified

years
: required, list of years to pull data for (earliest available is 1999)
columns
: optional, list of columns to pull data for
downcast
: converts float64 columns to float32, reducing memory usage by ~30%. Will slow down initial load speed ~50%

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
nfl.import_win_totals(years)
```
Returns win total lines for years specified

years
: optional, list of years to pull

```python
nfl.import_sc_lines(years)
```
Returns scoring lines for years specified

years
: optional, list of years to pull

```python
nfl.import_officials(years)
```
Returns official information by game for the years specified

years
: optional, list of years to pull

```python
nfl.import_draft_picks(years)
```
Returns list of draft picks for the years specified

years
: optional, list of years to pull

```python
nfl.import_draft_values()
```
Returns relative values by generic draft pick according to various popular valuation methods

```python
nfl.import_team_desc()
```
Returns dataframe with color/logo/etc information for all NFL team

```python
nfl.import_schedules(years)
```
Returns dataframe with schedule information for years specified

years
: required, list of years to pull data for (earliest available is 1999)

```python
nfl.import_combine_data(years, positions)
```
Returns dataframe with combine results for years and positions specified

years: optional, list or range of years to pull data from
positions: optional, list of positions to be pulled (standard format - WR/QB/RB/etc.)

```python
nfl.import_ids(columns, ids)
```
Returns dataframe with mapped ids for all players across most major NFL and fantasy football data platforms

columns: optional, list of columns to return
ids: optional, list of ids to return

**Additional features**
```python
nfl.clean_nfl_data(df)
```
Runs descriptive data (team name, player name, etc.) through various cleaning processes

df
: required, dataframe to be cleaned

## Recognition
I'd like to recognize all of [Ben Baldwin](https://twitter.com/benbbaldwin), [Sebastian Carl](https://twitter.com/mrcaseb), and [Lee Sharpe](https://twitter.com/LeeSharpeNFL) for making this data freely available and easy to access. I'd also like to thank [Tan Ho](https://twitter.com/_TanH), who has been an invaluable resource as I've worked through this project, and Josh Kazan for the resources and assistance he's provided.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
