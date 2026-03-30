# Source: Demandes de Valeurs Foncières (DVF) - data.gouv.fr
# URL: https://files.data.gouv.fr/geo-dvf/latest/csv/

import pandas as pd
import h3
from schema import SCHEMA

DEPARTMENTS = [str(i).zfill(2) for i in range(1, 96) if i != 20] + ['2A', '2B']
BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2023/departements/{}.csv.gz"

COLS = ['valeur_fonciere', 'surface_reelle_bati', 'latitude', 'longitude', 'type_local']
TYPES = ['Appartement', 'Maison']

def fetch_department(dep):
    """Fetch and filter DVF data for one department."""
    try:
        df = pd.read_csv(
            BASE_URL.format(dep),
            usecols=COLS,
            dtype={'valeur_fonciere': str},
            low_memory=False
        )
        df = df[df['type_local'].isin(TYPES)].copy()
        df['valeur_fonciere'] = pd.to_numeric(
            df['valeur_fonciere'].str.replace(',', '.'), errors='coerce'
        )
        df = df.dropna(subset=['valeur_fonciere', 'surface_reelle_bati', 'latitude', 'longitude'])
        df = df[df['surface_reelle_bati'] > 0]
        df['price_per_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        df = df[df['price_per_m2'].between(500, 30000)]
        return df[['latitude', 'longitude', 'price_per_m2']]
    except Exception as e:
        print(f"Skipping {dep}: {e}")
        return None

def assign_h3(df, resolution=6):
    """Assign H3 index to each transaction."""
    df['h3_index'] = df.apply(
        lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], resolution),
        axis=1
    )
    return df

def main():
    input_file = 'parquets/fr/base_grid.parquet'
    output_file = 'parquets/fr/h3_DVF.parquet'

    base_df = pd.read_parquet(input_file)[['h3_index']]

    chunks = []
    for dep in DEPARTMENTS:
        print(f"Fetching {dep}...")
        chunk = fetch_department(dep)
        if chunk is not None and not chunk.empty:
            chunks.append(chunk)

    all_df = pd.concat(chunks, ignore_index=True)
    all_df = assign_h3(all_df)

    median_df = (
        all_df.groupby('h3_index')['price_per_m2']
        .median()
        .round(0)
        .reset_index()
        .rename(columns={'price_per_m2': 'dvf_price_median'})
    )

    out_df = base_df.merge(median_df, on='h3_index', how='left')
    out_df['h3_index'] = out_df['h3_index'].astype(SCHEMA['h3_index'])
    out_df['dvf_price_median'] = out_df['dvf_price_median'].astype(SCHEMA['dvf_price_median'])

    out_df.to_parquet(output_file, index=False)
    print(f"Done: {out_df['dvf_price_median'].notna().sum()} cells with DVF data")

if __name__ == "__main__":
    main()