# Assembles all parquet layers into a single GeoJSON for the frontend

import h3
import json
import os
import pandas as pd

PARQUETS_DIR = 'parquets'
OUTPUT = 'france_habitable.geojson'

# ⭐ ONLY FILE TO UPDATE WHEN ADDING A NEW LAYER
SOURCES = [
    'france_h3_nuclear.parquet',
    'france_h3_weather.parquet',
    # 'france_h3_floods.parquet',
    # 'france_h3_schools.parquet',
]

# Load schema
df = pd.read_parquet(f'{PARQUETS_DIR}/france_h3_schema.parquet')
print(f"Schema: {df.shape}")

# Merge all sources
for filename in SOURCES:
    path = f'{PARQUETS_DIR}/{filename}'
    if not os.path.exists(path):
        print(f"⚠️  Missing: {filename}")
        continue

    source = pd.read_parquet(path)
    cols = [
        col for col in source.columns
        if col != 'h3_index' and source[col].notna().any()
    ]

    cols_to_replace = [c for c in cols if c in df.columns]
    df = df.drop(columns=cols_to_replace)
    df = df.merge(source[['h3_index'] + cols], on='h3_index', how='left')
    print(f"✅ {filename} — {len(cols)} columns: {cols}")

print(f"\nFinal DataFrame: {df.shape}")
print(df.isnull().sum())

# Export GeoJSON
def round_coords(coords, decimals=4):
    if isinstance(coords[0], (list, tuple)):
        return [round_coords(c, decimals) for c in coords]
    return [round(float(coords[0]), decimals), round(float(coords[1]), decimals)]

print("\nBuilding GeoJSON...")
features = []
for _, row in df.iterrows():
    boundary = h3.cell_to_boundary(row['h3_index'])
    coords = [[lon, lat] for lat, lon in boundary]
    coords.append(coords[0])

    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [round_coords(coords)]
        },
        "properties": {
            k: (None if pd.isna(v) else v)
            for k, v in row.items()
            if k not in ('lat', 'lon')
        }
    })

geojson = {"type": "FeatureCollection", "features": features}

with open(OUTPUT, 'w') as f:
    json.dump(geojson, f, separators=(',', ':'))

size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
print(f"✅ {OUTPUT} — {size_mb:.1f} MB")