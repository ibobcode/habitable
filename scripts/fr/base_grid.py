import requests
import h3
import pandas as pd
from schema import SCHEMA
import os

def fetch_france_geometry():
    # Utilisation de l'URL des départements
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def generate_h3_grid(geojson_data, resolution=6):
    all_cells = set()
    
    # Parcours de chaque département (chaque feature)
    features = geojson_data.get('features', [])
    for feature in features:
        # Exclusion des DOM-TOM pour rester sur la métropole (codes > 97)
        code = feature.get('properties', {}).get('code', '00')
        if code >= '97':
            continue
            
        geometry = feature.get('geometry')
        if geometry:
            # h3.geo_to_cells extrait les mailles pour ce département précis
            cells = h3.geo_to_cells(geometry, resolution)
            all_cells.update(cells)
            
    return list(all_cells)

def main():
    output_file = 'parquets/fr/base_grid.parquet'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    resolution = 6

    geojson_data = fetch_france_geometry()
    h3_indices = generate_h3_grid(geojson_data, resolution)

    if not h3_indices:
        raise ValueError("Erreur : Aucune cellule H3 n'a été générée depuis le fichier des départements.")

    data = []
    for h3_index in h3_indices:
        lat, lon = h3.cell_to_latlng(h3_index)
        data.append({
            'h3_index': h3_index,
            'lat': round(lat, 6),
            'lon': round(lon, 6)
        })

    df = pd.DataFrame(data, columns=['h3_index', 'lat', 'lon'])

    # Typage via SCHEMA
    df['h3_index'] = df['h3_index'].astype(SCHEMA['h3_index'])
    df['lat'] = df['lat'].astype(SCHEMA['lat'])
    df['lon'] = df['lon'].astype(SCHEMA['lon'])

    df.to_parquet(output_file, index=False)
    print(f"Succès : {len(df)} cellules H3 générées pour la France métropolitaine.")

if __name__ == "__main__":
    main()
