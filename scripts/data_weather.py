# Source: Météo-France - Monthly climatological data
# https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-mensuelles

import io
import gzip
import requests
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

# Load schema
df = pd.read_parquet('parquets/france_h3_schema.parquet')
print(f"Schema loaded: {len(df)} hexagons")

# Fetch file list from data.gouv.fr API
dataset_id = "donnees-climatologiques-de-base-mensuelles"
api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
resources = requests.get(api_url).json()['resources']

# Filter metropolitan France departments 1950-2024
METRO_DEPTS = [str(i).zfill(2) for i in range(1, 96)] + ['2A', '2B']
files = [
    r for r in resources
    if 'periode_1950-2024' in r['title']
    and 'MENS_departement' in r['title']
    and any(f'departement_{d}_' in r['title'] for d in METRO_DEPTS)
]
print(f"{len(files)} department files found")

# Download and concatenate
dfs = []
for i, f in enumerate(files):
    print(f"  {i+1}/{len(files)} - {f['title'][:50]}")
    r = requests.get(f['url'], timeout=30)
    with gzip.open(io.BytesIO(r.content)) as gz:
        dfs.append(pd.read_csv(gz, sep=';', dtype=str))

raw = pd.concat(dfs, ignore_index=True)
print(f"Raw data: {len(raw)} rows")

# Keep useful columns
cols = ['NUM_POSTE', 'LAT', 'LON', 'AAAAMM',
        'RR', 'TM', 'TN', 'TX', 'INST',
        'NBJRR1', 'NBJGELEE', 'NBJTX30']
raw = raw[cols].copy()

for col in ['LAT', 'LON', 'RR', 'TM', 'TN', 'TX', 'INST', 'NBJRR1', 'NBJGELEE', 'NBJTX30']:
    raw[col] = pd.to_numeric(raw[col], errors='coerce')

raw['year'] = raw['AAAAMM'].astype(str).str[:4].astype(int)
raw['month'] = raw['AAAAMM'].astype(str).str[4:6].astype(int)
raw = raw[raw['year'].isin([2023, 2024])]
print(f"Filtered 2023-2024: {len(raw)} rows")

# Assign seasons
def get_season(month):
    if month in [12, 1, 2]:  return 'winter'
    if month in [3, 4, 5]:   return 'spring'
    if month in [6, 7, 8]:   return 'summer'
    return 'autumn'

raw['season'] = raw['month'].apply(get_season)

# Aggregate by station and season
by_season = raw.groupby(['NUM_POSTE', 'LAT', 'LON', 'season']).agg(
    rainfall=('RR', 'mean'),
    temperature=('TM', 'mean'),
).reset_index()

by_year = raw.groupby(['NUM_POSTE', 'LAT', 'LON']).agg(
    temp_min=('TN', 'min'),
    temp_max=('TX', 'max'),
    sunshine_hours=('INST', 'sum'),
    rainy_days=('NBJRR1', 'sum'),
    frost_days=('NBJGELEE', 'sum'),
    hot_days=('NBJTX30', 'sum'),
).reset_index()

# Replace 0 sunshine with NaN (stations without sensor)
by_year['sunshine_hours'] = by_year['sunshine_hours'].replace(0, np.nan)

# Pivot seasons
pivot = by_season.pivot_table(
    index=['NUM_POSTE', 'LAT', 'LON'],
    columns='season',
    values=['rainfall', 'temperature']
).reset_index()
pivot.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in pivot.columns]

stations = pivot.merge(by_year, on=['NUM_POSTE', 'LAT', 'LON'])
stations = stations.dropna(subset=['LAT', 'LON'])
print(f"{len(stations)} stations with data")

# IDW interpolation
def idw(tree, stations_df, lat, lon, k=8, power=2):
    distances, indices = tree.query([[lat, lon]], k=k)
    distances, indices = distances[0], indices[0]
    if distances[0] == 0:
        return np.ones(k) / k, indices
    weights = 1 / (distances ** power)
    weights /= weights.sum()
    return weights, indices

coords = stations[['LAT', 'LON']].values
tree = cKDTree(coords)

COLUMNS = {
    'rainfall_winter':    'rainfall_winter',
    'rainfall_spring':    'rainfall_spring',
    'rainfall_summer':    'rainfall_summer',
    'rainfall_autumn':    'rainfall_autumn',
    'temperature_winter': 'temperature_winter',
    'temperature_spring': 'temperature_spring',
    'temperature_summer': 'temperature_summer',
    'temperature_autumn': 'temperature_autumn',
    'temp_min':           'temp_min',
    'temp_max':           'temp_max',
    'sunshine_hours':     'sunshine_hours',
    'rainy_days':         'rainy_days',
    'frost_days':         'frost_days',
    'hot_days':           'hot_days',
}

print("Interpolating...")
results = []
for i, row in df.iterrows():
    if i % 1000 == 0:
        print(f"  {i}/{len(df)}...")
    weights, indices = idw(tree, stations, row['lat'], row['lon'])
    result = {'h3_index': row['h3_index']}
    for col_out, col_in in COLUMNS.items():
        vals = stations.iloc[indices][col_in].values
        mask = ~np.isnan(vals)
        if mask.sum() == 0:
            result[col_out] = np.nan
        else:
            w = weights[mask] / weights[mask].sum()
            result[col_out] = round(float(np.sum(w * vals[mask])), 2)
    results.append(result)

df_weather = pd.DataFrame(results)
df_out = df.copy()
for col in df_weather.columns:
    if col != 'h3_index':
        df_out[col] = df_out['h3_index'].map(df_weather.set_index('h3_index')[col])

df_out.to_parquet('parquets/france_h3_weather.parquet', index=False)
print(f"✅ Saved: parquets/france_h3_weather.parquet")