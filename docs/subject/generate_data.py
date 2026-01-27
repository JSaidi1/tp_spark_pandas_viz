#!/usr/bin/env python3
"""
Script de generation des donnees pour le TP Spark/Pandas/Visualisation.
Genere des donnees realistes de qualite de l'air avec des defauts intentionnels.
"""

import random
import csv
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
OUTPUT_DIR = Path(__file__).parent / "data"
SEED = 42
random.seed(SEED)

# Villes et stations
CITIES = [
    ("Paris", 48.8566, 2.3522),
    ("Lyon", 45.7640, 4.8357),
    ("Marseille", 43.2965, 5.3698),
    ("Toulouse", 43.6047, 1.4442),
    ("Bordeaux", 44.8378, -0.5792),
    ("Lille", 50.6292, 3.0573),
    ("Nantes", 47.2184, -1.5536),
    ("Strasbourg", 48.5734, 7.7521),
    ("Grenoble", 45.1885, 5.7245),
    ("Rouen", 49.4432, 1.0999),
]

STATION_TYPES = ["urbaine", "periurbaine", "industrielle", "trafic", "rurale"]
POLLUTANTS = ["PM2.5", "PM10", "NO2", "O3", "SO2", "CO"]
WEATHER_CONDITIONS = ["ensoleille", "nuageux", "pluvieux", "orageux", "neigeux", "brumeux"]

# Formats de dates pour introduire des incoherences
DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",      # ISO
    "%d/%m/%Y %H:%M",         # FR
    "%m/%d/%Y %H:%M:%S",      # US
    "%Y-%m-%dT%H:%M:%S",      # ISO avec T
]


def generate_stations():
    """Genere le fichier stations.csv (propre)."""
    stations = []
    station_id = 1

    for city, lat, lon in CITIES:
        # 3 a 7 stations par ville
        num_stations = random.randint(3, 7)
        for i in range(num_stations):
            stations.append({
                "station_id": f"ST{station_id:04d}",
                "station_name": f"{city}-{STATION_TYPES[i % len(STATION_TYPES)]}-{i+1}",
                "city": city,
                "lat": round(lat + random.uniform(-0.05, 0.05), 6),
                "lon": round(lon + random.uniform(-0.05, 0.05), 6),
                "station_type": STATION_TYPES[i % len(STATION_TYPES)],
            })
            station_id += 1

    output_path = OUTPUT_DIR / "stations.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["station_id", "station_name", "city", "lat", "lon", "station_type"])
        writer.writeheader()
        writer.writerows(stations)

    print(f"Generated {len(stations)} stations -> {output_path}")
    return stations


def generate_air_quality(stations, start_date, end_date):
    """Genere le fichier air_quality_raw.csv avec des defauts intentionnels."""
    records = []

    current_date = start_date
    while current_date < end_date:
        for station in stations:
            for pollutant in POLLUTANTS:
                # Skip aleatoire (donnees manquantes)
                if random.random() < 0.02:
                    continue

                # Valeur de base selon le polluant et le type de station
                base_values = {
                    "PM2.5": 15, "PM10": 25, "NO2": 30,
                    "O3": 50, "SO2": 5, "CO": 0.5
                }
                base = base_values[pollutant]

                # Ajustement selon le type de station
                if station["station_type"] == "trafic":
                    base *= 1.5
                elif station["station_type"] == "industrielle":
                    base *= 1.3
                elif station["station_type"] == "rurale":
                    base *= 0.6

                # Variation saisonniere (plus de pollution en hiver)
                month = current_date.month
                if month in [11, 12, 1, 2]:
                    base *= 1.4
                elif month in [6, 7, 8]:
                    base *= 0.8

                # Variation horaire (pics matin et soir)
                hour = current_date.hour
                if hour in [7, 8, 9, 17, 18, 19]:
                    base *= 1.3
                elif hour in [2, 3, 4]:
                    base *= 0.7

                # Valeur finale avec bruit
                value = base * random.uniform(0.5, 2.0)

                # Introduction de defauts intentionnels
                defect_type = random.random()

                if defect_type < 0.01:
                    # Valeur negative (erreur)
                    value = -abs(value)
                elif defect_type < 0.02:
                    # Valeur aberrante (>1000)
                    value = random.uniform(1500, 5000)
                elif defect_type < 0.025:
                    # Valeur textuelle
                    value = random.choice(["N/A", "error", "---", "null"])

                # Format de la valeur (parfois avec virgule)
                if isinstance(value, float):
                    if random.random() < 0.15:
                        value_str = f"{value:.2f}".replace(".", ",")
                    else:
                        value_str = f"{value:.2f}"
                else:
                    value_str = str(value)

                # Format de date aleatoire
                date_format = random.choice(DATE_FORMATS)
                timestamp_str = current_date.strftime(date_format)

                records.append({
                    "station_id": station["station_id"],
                    "timestamp": timestamp_str,
                    "pollutant": pollutant,
                    "value": value_str,
                    "unit": "ug/m3" if pollutant != "CO" else "mg/m3",
                })

        # Avancer d'une heure
        current_date += timedelta(hours=1)

    # Ajouter des doublons intentionnels (2%)
    num_duplicates = int(len(records) * 0.02)
    duplicates = random.sample(records, num_duplicates)
    records.extend(duplicates)
    random.shuffle(records)

    output_path = OUTPUT_DIR / "air_quality_raw.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["station_id", "timestamp", "pollutant", "value", "unit"])
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {len(records)} air quality records -> {output_path}")


def generate_weather(cities, start_date, end_date):
    """Genere le fichier weather_raw.csv avec des defauts intentionnels."""
    records = []

    current_date = start_date
    while current_date < end_date:
        for city, _, _ in cities:
            # Skip aleatoire (donnees manquantes en bloc)
            if random.random() < 0.03:
                continue

            # Temperature de base selon la saison
            month = current_date.month
            if month in [12, 1, 2]:
                temp_base = 5
            elif month in [3, 4, 5]:
                temp_base = 15
            elif month in [6, 7, 8]:
                temp_base = 25
            else:
                temp_base = 12

            # Variation geographique
            if city in ["Marseille", "Toulouse", "Bordeaux"]:
                temp_base += 3
            elif city in ["Lille", "Strasbourg"]:
                temp_base -= 2

            temperature = temp_base + random.uniform(-8, 8)
            humidity = random.uniform(40, 95)
            wind_speed = random.uniform(0, 50)
            precipitation = random.uniform(0, 10) if random.random() < 0.3 else 0

            # Condition meteo coherente
            if precipitation > 5:
                condition = random.choice(["pluvieux", "orageux"])
            elif precipitation > 0:
                condition = "pluvieux"
            elif humidity > 80:
                condition = random.choice(["nuageux", "brumeux"])
            elif temperature > 20:
                condition = random.choice(["ensoleille", "nuageux"])
            else:
                condition = random.choice(WEATHER_CONDITIONS)

            # Introduction de defauts intentionnels
            defect_type = random.random()

            if defect_type < 0.01:
                # Temperature impossible
                temperature = random.choice([-80, 75, 100])
            elif defect_type < 0.02:
                # Humidite >100
                humidity = random.uniform(105, 150)
            elif defect_type < 0.03:
                # Valeur manquante
                temperature = random.choice(["", "NA", "null"])
            elif defect_type < 0.04:
                humidity = ""

            # Format de temperature (parfois avec virgule)
            if isinstance(temperature, (int, float)):
                if random.random() < 0.1:
                    temp_str = f"{temperature:.1f}".replace(".", ",")
                else:
                    temp_str = f"{temperature:.1f}"
            else:
                temp_str = str(temperature)

            # Format humidite
            if isinstance(humidity, (int, float)):
                humidity_str = f"{humidity:.1f}"
            else:
                humidity_str = str(humidity)

            # Format de date aleatoire
            date_format = random.choice(DATE_FORMATS)
            timestamp_str = current_date.strftime(date_format)

            records.append({
                "city": city,
                "timestamp": timestamp_str,
                "temperature_c": temp_str,
                "humidity_pct": humidity_str,
                "wind_speed_kmh": f"{wind_speed:.1f}",
                "precipitation_mm": f"{precipitation:.1f}",
                "weather_condition": condition if random.random() > 0.02 else "",
            })

        # Avancer d'une heure
        current_date += timedelta(hours=1)

    # Ajouter quelques blocs de donnees manquantes (simuler panne capteur)
    # En supprimant des plages pour certaines villes
    records_filtered = []
    skip_city = random.choice([c[0] for c in cities])
    skip_start = start_date + timedelta(days=random.randint(30, 60))
    skip_end = skip_start + timedelta(days=random.randint(3, 7))

    for record in records:
        # Parser approximatif de la date pour le filtrage
        if record["city"] == skip_city:
            # Simplification: on garde 95% des records de cette ville
            if random.random() < 0.05:
                continue
        records_filtered.append(record)

    random.shuffle(records_filtered)

    output_path = OUTPUT_DIR / "weather_raw.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "city", "timestamp", "temperature_c", "humidity_pct",
            "wind_speed_kmh", "precipitation_mm", "weather_condition"
        ])
        writer.writeheader()
        writer.writerows(records_filtered)

    print(f"Generated {len(records_filtered)} weather records -> {output_path}")


def main():
    """Point d'entree principal."""
    print("Generation des donnees pour le TP Spark/Pandas/Visualisation")
    print("=" * 60)

    # Creer le dossier de sortie
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Periode: 6 mois de donnees
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 7, 1, 0, 0, 0)

    print(f"Periode: {start_date.date()} -> {end_date.date()}")
    print()

    # Generer les stations
    stations = generate_stations()
    print()

    # Generer les donnees de qualite de l'air
    generate_air_quality(stations, start_date, end_date)
    print()

    # Generer les donnees meteo
    generate_weather(CITIES, start_date, end_date)
    print()

    print("=" * 60)
    print("Generation terminee!")
    print(f"Fichiers generes dans: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
