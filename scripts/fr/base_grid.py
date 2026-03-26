import requests
import h3
import pandas as pd
from schema import SCHEMA

def fetch_france_geometry():
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/metropole-version-simplifiee.geojson"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def generate_h3_grid(geojson_data, resolution=6):
    # On extrait uniquement la partie 'geometry' pour h3.geo_to_cells
    geometry = geojson_data.get('geometry')
    if not geometry:
        return []
    
    # h3.geo_to_cells gère nativement les Polygon et MultiPolygon du GeoJSON
    cells = h3.geo_to_cells(geometry, resolution)
    return list(cells)

def main():
    output_file = 'parquets/fr/base_grid.parquet'
    resolution = 6

    geojson_data = fetch_france_geometry()
    h3_indices = generate_h3_grid(geojson_data, resolution)

    if not h3_indices:
        raise ValueError("Erreur critique : Aucune cellule H3 générée. Vérifiez la structure du GeoJSON.")

    # Création du dictionnaire de données
    data = []
    for h3_index in h3_indices:
        lat, lon = h3.cell_to_latlng(h3_index)
        data.append({
            'h3_index': h3_index,
            'lat': round(lat, 6),
            'lon': round(lon, 6)
        })

    # On force la création des colonnes pour éviter le KeyError même si data était vide
    df = pd.DataFrame(data, columns=['h3_index', 'lat', 'lon'])

    # Application du typage strict via ton fichier schema.py
    df['h3_index'] = df['h3_index'].astype(SCHEMA['h3_index'])
    df['lat'] = df['lat'].astype(SCHEMA['lat'])
    df['lon'] = df['lon'].astype(SCHEMA['lon'])

    # Export final
    df.to_parquet(output_file, index=False)
    print(f"Succès : {len(df)} cellules enregistrées dans {output_file}")

if __name__ == "__main__":
    main()
