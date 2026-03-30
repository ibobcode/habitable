import json
import os
import glob
import pandas as pd
import h3
from datetime import datetime, timezone
from schema import SCHEMA

def round_coords(coords, decimals=4):
    """Round coordinate pairs to specified decimal places."""
    return [[round(float(lon), decimals), round(float(lat), decimals)] for lon, lat in coords]

def main():
    parquets_dir = 'parquets/fr'
    output_file = 'france_habitable.geojson'
    base_file = f'{parquets_dir}/base_grid.parquet'

    # Auto-découverte des sources : tous les parquets sauf base_grid
    source_paths = sorted(glob.glob(f'{parquets_dir}/h3_*.parquet'))
    sources = [os.path.basename(p) for p in source_paths]

    df = pd.read_parquet(base_file)

    for filename in sources:
        path = f'{parquets_dir}/{filename}'

        source = pd.read_parquet(path)
        merge_cols = [col for col in source.columns if col != 'h3_index']

        cols_to_drop = [c for c in merge_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        df = df.merge(source[['h3_index'] + merge_cols], on='h3_index', how='left')

    valid_columns = [col for col in df.columns if col in SCHEMA.keys()]
    df = df[valid_columns]

    features = []
    for _, row in df.iterrows():
        boundary = h3.cell_to_boundary(row['h3_index'])
        coords = [[lon, lat] for lat, lon in boundary]
        coords.append(coords[0])

        properties = {
            k: (None if pd.isna(v) else v)
            for k, v in row.to_dict().items()
            if k not in ('lat', 'lon')
        }

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [round_coords(coords)]
            },
            "properties": properties
        })

    geojson = {
        "type": "FeatureCollection",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cell_count": len(features),
        "sources": sources,
        "features": features
    }

    with open(output_file, 'w') as f:
        json.dump(geojson, f, separators=(',', ':'))

    print(f"Done: {len(features)} cells, {len(sources)} sources")

if __name__ == "__main__":
    main()
