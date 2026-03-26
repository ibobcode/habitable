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
    cells = set()
    geom = geojson_data.get('geometry', {})
    geom_type = geom.get('type')

    if geom_type == 'MultiPolygon':
        polygons = geom.get('coordinates', [])
    elif geom_type == 'Polygon':
        polygons = [geom.get('coordinates', [])]
    else:
        return list(cells)

    for polygon_coords in polygons:
        geo = {"type": "Polygon", "coordinates": polygon_coords}
        cells.update(h3.geo_to_cells(geo, resolution))
        
    return list(cells)

def main():
    output_file = 'parquets/fr/base_grid.parquet'
    resolution = 6

    geojson_data = fetch_france_geometry()
    h3_indices = generate_h3_grid(geojson_data, resolution)

    if not h3_indices:
        raise ValueError("Grid generation failed: 0 cells returned.")

    data = []
    for h3_index in h3_indices:
        lat, lon = h3.cell_to_latlng(h3_index)
        data.append({
            'h3_index': h3_index,
            'lat': round(lat, 6),
            'lon': round(lon, 6)
        })

    df = pd.DataFrame(data, columns=['h3_index', 'lat', 'lon'])

    df['h3_index'] = df['h3_index'].astype(SCHEMA['h3_index'])
    df['lat'] = df['lat'].astype(SCHEMA['lat'])
    df['lon'] = df['lon'].astype(SCHEMA['lon'])

    df.to_parquet(output_file, index=False)

if __name__ == "__main__":
    main()
