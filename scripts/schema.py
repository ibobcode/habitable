# Single source of truth for all data fields
# Add new fields here before creating a new data script

SCHEMA = {
    # Identifiers — never modify
    'h3_index': str,
    'lat': float,
    'lon': float,

    # Nuclear
    'nuclear_plant_distance_km': float,

    # Seasonal weather
    'rainfall_winter': float,
    'rainfall_spring': float,
    'rainfall_summer': float,
    'rainfall_autumn': float,
    'temperature_winter': float,
    'temperature_spring': float,
    'temperature_summer': float,
    'temperature_autumn': float,

    # Annual weather
    'temp_min': float,
    'temp_max': float,
    'sunshine_hours': float,
    'rainy_days': float,
    'frost_days': float,
    'hot_days': float,
}