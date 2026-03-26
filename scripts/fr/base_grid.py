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
    
    if geojson_data.get('type') == 'FeatureCollection':
        features = geojson_data.get('features', [])
    elif geojson_data.get('type') == 'Feature':
        features = [geojson_data]
    else:
        features = []

    for feature in features:
        geom = feature.get('geometry', {})
        geom_type = geom.get('type')
        if geom_type == 'Polygon':
            polygons = [geom['coordinates']]
        elif geom_type == 'MultiPolygon':
            polygons = geom['coordinates']
        else:
            continue
        for poly_coords in polygons:
            exterior = [(lat, lon) for lon, lat in poly_coords[0]]
            holes = [[(lat, lon) for lon, lat in hole] for hole in poly_coords[1:]]
            h3_poly = h3.Polygon(exterior, *holes)
            cells.update(h3.polygon_to_cells(h3_poly, resolution))
    return list(cells)

def main():
    output_file = 'parquets/fr/base_grid.parquet'
    resolution = 6

    geojson_data = fetch_france_geometry()
    h3_indices = generate_h3_grid(geojson_data, resolution)

    data = []
    for h3_index in h3_indices:
        lat, lon = h3.cell_to_latlng(h3_index)
        data.append({
            'h3_index': h3_index,
            'lat': round(lat, 6),
            'lon': round(lon, 6)
        })

    df = pd.DataFrame(data)

    df['h3_index'] = df['h3_index'].astype(SCHEMA['h3_index'])
    df['lat'] = df['lat'].astype(SCHEMA['lat'])
    df['lon'] = df['lon'].astype(SCHEMA['lon'])

    df.to_parquet(output_file, index=False)

if __name__ == "__main__":
    main()
