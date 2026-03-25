# Source: EDF Open Data - Nuclear power plants
# https://opendata.edf.fr/explore/dataset/centrales-de-production-nucleaire-edf

import math
import pandas as pd
import requests

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def nearest_plant_distance(lat, lon, plants):
    return round(min(haversine(lat, lon, p['lat'], p['lon']) for p in plants), 1)

# Load schema
df = pd.read_parquet('parquets/france_h3_schema.parquet')
print(f"Schema loaded: {len(df)} hexagons")

# Load nuclear plants
url = "https://opendata.edf.fr/api/explore/v2.1/catalog/datasets/centrales-de-production-nucleaire-edf/exports/geojson?lang=fr&timezone=Europe%2FParis"
response = requests.get(url, timeout=30)
data = response.json()

plants_dict = {}
for feature in data['features']:
    coords = feature['geometry']['coordinates']
    name = feature['properties']['centrale']
    if name not in plants_dict:
        plants_dict[name] = {'name': name, 'lon': coords[0], 'lat': coords[1]}

plants = list(plants_dict.values())
print(f"{len(plants)} unique plants loaded")

# Compute distances
print("Computing distances...")
distances = []
for i, row in df.iterrows():
    if i % 1000 == 0:
        print(f"  {i}/{len(df)}...")
    distances.append(nearest_plant_distance(row['lat'], row['lon'], plants))

df['nuclear_plant_distance_km'] = distances

# Save
df.to_parquet('parquets/france_h3_nuclear.parquet', index=False)
print(f"✅ Saved: parquets/france_h3_nuclear.parquet")