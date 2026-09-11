"""Stable domain constants shared by ingestion and transformation code."""

MISO_MARKET_TIMEZONE = "America/New_York"
EXPECTED_REGIONS = frozenset({"CENTRAL", "NORTH", "SOUTH"})
EXPECTED_ZONES = frozenset(f"Z{number}" for number in range(1, 11))

MISO_HISTORY_ENDPOINTS = {
    "actual_load": "/lgi/v1/real-time/{date}/demand/actual",
    "load_forecast": "/lgi/v1/forecast/{date}/load",
}

MISO_PUBLIC_ENDPOINTS = {
    "operations_snapshot": "/api/Snapshot",
    "realtime_load": "/api/RealTimeTotalLoad",
    "fuel_mix": "/api/FuelMix",
    "wind": "/api/WindSolar/getwind",
    "solar": "/api/WindSolar/getsolar",
    "generation_outage": "/api/GenerationOutages/GetGenerationOutagesPlusMinusFiveDays",
    "realtime_price": "/api/MarketPricing/GetRealTimeFiveMinExPost/Current",
    "actual_interchange": "/api/Interchange/GetNai",
    "scheduled_interchange": "/api/Interchange/GetNsi/MISO",
}

WEATHER_LOCATIONS = {
    "chicago": (41.8781, -87.6298),
    "des_moines": (41.5868, -93.6250),
    "detroit": (42.3314, -83.0458),
    "indianapolis": (39.7684, -86.1581),
    "jackson_ms": (32.2988, -90.1848),
    "little_rock": (34.7465, -92.2896),
    "milwaukee": (43.0389, -87.9065),
    "minneapolis": (44.9778, -93.2650),
    "new_orleans": (29.9511, -90.0715),
    "st_louis": (38.6270, -90.1994),
}

WEATHER_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "cloud_cover",
    "wind_speed_10m",
    "shortwave_radiation",
)
