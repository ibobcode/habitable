import requests
import h3
import pandas as pd
from schema import SCHEMA
import os

def fetch_france_geometry():
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def generate_h3_grid(geojson_data, resolution=6):
    all_cells = set()
    features = geojson_data.get('features', [])

    for feature in features:
        code = feature.get('properties', {}).get('code', '99')
        if code < '97':
            geometry = feature.get('geometry')
            if geometry:
                cells = h3.geo_to_cells(geometry, resolution)
                all_cells.update(cells)

    return list(all_cells)

def main():
    output_dir = 'parquets/fr'
    output_file = os.path.join(output_dir, 'base_grid.parquet')
    os.makedirs(output_dir, exist_ok=True)

    resolution = 6

    geojson_data = fetch_france_geometry()
    h3_indices = generate_h3_grid(geojson_data, resolution)

    if not h3_indices:
        raise ValueError("Erreur : Aucune cellule H3 générée. Vérifiez la structure 'features' du JSON.")

    data = [
        {
            'h3_index': h3_index,
            'lat': round(h3.cell_to_latlng(h3_index)[0], 6),
            'lon': round(h3.cell_to_latlng(h3_index)[1], 6),
        }
        for h3_index in h3_indices
    ]

    df = pd.DataFrame(data)  # columns inferred from dict keys: h3_index, lat, lon

    df['h3_index'] = df['h3_index'].astype(SCHEMA['h3_index'])
    df['lat'] = df['lat'].astype(SCHEMA['lat'])
    df['lon'] = df['lon'].astype(SCHEMA['lon'])

    df.to_parquet(output_file, index=False)
    print(f"Succès : {len(df)} cellules H3 générées.")

if __name__ == "__main__":
    main()
