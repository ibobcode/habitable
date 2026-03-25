# Source: Météo-France - Monthly climatological data
# URL: https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-mensuelles

import io
import gzip
import requests
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from schema import SCHEMA

def load_weather_data():
    dataset_id = "donnees-climatologiques-de-base-mensuelles"
    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    resources = requests.get(api_url).json()['resources']

    METRO_DEPTS = [str(i).zfill(2) for i in range(1, 96)] + ['2A', '2B']
    files = [
        r for r in resources
        if 'periode_1950-2024' in r['title']
        and 'MENS_departement' in r['title']
        and any(f'departement_{d}_' in r['title'] for d in METRO_DEPTS)
    ]

    dfs = []
    for f in files:
        r = requests.get(f['url'], timeout=30)
        with gzip.open(io.BytesIO(r.content)) as gz:
            dfs.append(pd.read_csv(gz, sep=';', dtype=str))

    return pd.concat(dfs, ignore_index=True)

def get_season(month):
    if month in [12, 1, 2]: return 'winter'
    if month in [3, 4, 5]:  return 'spring'
    if month in [6, 7, 8]:  return 'summer'
    return 'autumn'

def process_weather_data(raw):
    cols = ['NUM_POSTE', 'LAT', 'LON', 'AAAAMM', 'RR', 'TM', 'TN', 'TX', 'INST', 'NBJRR1', 'NBJGELEE', 'NBJTX30']
    raw = raw[cols].copy()

    numeric_cols = ['LAT', 'LON', 'RR', 'TM', 'TN', 'TX', 'INST', 'NBJRR1', 'NBJGELEE', 'NBJTX30']
    for col in numeric_cols:
        raw[col] = pd.to_numeric(raw[col], errors='coerce')

    raw['year'] = raw['AAAAMM'].astype(str).str[:4].astype(int)
    raw['month'] = raw['AAAAMM'].astype(str).str[4:6].astype(int)
    raw = raw[raw['year'].isin([2023, 2024])]

    raw['season'] = raw['month'].apply(get_season)

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

    by_year['sunshine_hours'] = by_year['sunshine_hours'].replace(0, np.nan)

    pivot = by_season.pivot_table(
        index=['NUM_POSTE', 'LAT', 'LON'],
        columns='season',
        values=['rainfall', 'temperature']
    ).reset_index()

    pivot.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in pivot.columns]

    stations = pivot.merge(by_year, on=['NUM_POSTE', 'LAT', 'LON'])
    return stations.dropna(subset=['LAT', 'LON'])

def apply_idw_vectorized(df, stations, k=8, power=2):
    coords = stations[['LAT', 'LON']].values
    tree = cKDTree(coords)
    target_coords = df[['lat', 'lon']].values

    distances, indices = tree.query(target_coords, k=k)
    distances = np.maximum(distances, 1e-9)
    base_weights = 1.0 / (distances ** power)

    columns_mapping = {
        'rainfall_winter': 'rainfall_winter',
        'rainfall_spring': 'rainfall_spring',
        'rainfall_summer': 'rainfall_summer',
        'rainfall_autumn': 'rainfall_autumn',
        'temperature_winter': 'temperature_winter',
        'temperature_spring': 'temperature_spring',
        'temperature_summer': 'temperature_summer',
        'temperature_autumn': 'temperature_autumn',
        'temp_min': 'temp_min',
        'temp_max': 'temp_max',
        'sunshine_hours': 'sunshine_hours',
        'rainy_days': 'rainy_days',
        'frost_days': 'frost_days',
        'hot_days': 'hot_days',
    }

    for col_out, col_in in columns_mapping.items():
        vals = stations[col_in].values[indices]
        valid_mask = ~np.isnan(vals)

        valid_weights = base_weights * valid_mask
        weight_sums = valid_weights.sum(axis=1, keepdims=True)

        safe_weight_sums = np.where(weight_sums == 0, 1, weight_sums)
        normalized_weights = valid_weights / safe_weight_sums

        safe_vals = np.where(valid_mask, vals, 0)
        interpolated = np.sum(normalized_weights * safe_vals, axis=1)

        interpolated = np.where(weight_sums.flatten() == 0, np.nan, interpolated)
        df[col_out] = np.round(interpolated, 2)

    return df

def main():
    input_file = 'parquets/fr/base_grid.parquet'
    output_file = 'parquets/fr/h3_weather.parquet'

    df = pd.read_parquet(input_file)
    raw_data = load_weather_data()
    stations = process_weather_data(raw_data)

    df = apply_idw_vectorized(df, stations)

    for col in SCHEMA.keys():
        if col in df.columns:
            df[col] = df[col].astype(SCHEMA[col])

    df.to_parquet(output_file, index=False)

if __name__ == "__main__":
    main()
