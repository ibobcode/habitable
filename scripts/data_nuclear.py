# Source: EDF Open Data - Nuclear power plants
# URL: https://opendata.edf.fr/explore/dataset/centrales-de-production-nucleaire-edf

import numpy as np
import pandas as pd
import requests
from schema import SCHEMA

def load_nuclear_plants():
    """Fetch and extract unique nuclear power plant coordinates from EDF API."""
    url = "https://opendata.edf.fr/api/explore/v2.1/catalog/datasets/centrales-de-production-nucleaire-edf/exports/geojson?lang=fr&timezone=Europe%2FParis"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    plants_dict = {}
    for feature in data.get('features', []):
        coords = feature['geometry']['coordinates']
        name = feature['properties']['centrale']
        if name not in plants_dict:
            plants_dict[name] = {'name': name, 'lon': coords[0], 'lat': coords[1]}

    return list(plants_dict.values())

def compute_min_distances_vectorized(df_lats, df_lons, plants):
    """Compute minimal Haversine distance for all grid points using NumPy broadcasting."""
    if not plants:
        raise ValueError("No nuclear plant data available.")

    p_lats = np.array([p['lat'] for p in plants])
    p_lons = np.array([p['lon'] for p in plants])

    df_lats_r = np.radians(df_lats.values)[:, np.newaxis]
    df_lons_r = np.radians(df_lons.values)[:, np.newaxis]
    p_lats_r = np.radians(p_lats)[np.newaxis, :]
    p_lons_r = np.radians(p_lons)[np.newaxis, :]

    dlat = p_lats_r - df_lats_r
    dlon = p_lons_r - df_lons_r

    a = np.sin(dlat / 2.0)**2 + np.cos(df_lats_r) * np.cos(p_lats_r) * np.sin(dlon / 2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371.0 * c

    return np.round(np.min(km, axis=1), 1)

def main():
    input_file = 'parquets/fr/base_grid.parquet'
    output_file = 'parquets/fr/h3_nuclear.parquet'

    df = pd.read_parquet(input_file)
    plants = load_nuclear_plants()
    
    distances = compute_min_distances_vectorized(df['lat'], df['lon'], plants)

    out_df = pd.DataFrame({
        'h3_index': df['h3_index'],
        'nuclear_plant_distance_km': distances
    })

    out_df['h3_index'] = out_df['h3_index'].astype(SCHEMA['h3_index'])
    out_df['nuclear_plant_distance_km'] = out_df['nuclear_plant_distance_km'].astype(SCHEMA['nuclear_plant_distance_km'])

    out_df.to_parquet(output_file, index=False)

if __name__ == "__main__":
    main()
