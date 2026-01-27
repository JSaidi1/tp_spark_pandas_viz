# TP – Analyse de la qualité de l'air : Spark, Pandas et Visualisation

---

## Objectif global

Vous travaillez pour une agence régionale de surveillance de la qualité de l'air. Votre mission est de construire un pipeline d'analyse complet permettant de :

1. Charger et agréger des données volumineuses de pollution avec **Apache Spark**.
2. Nettoyer et enrichir les données avec **Pandas**.
3. Produire des visualisations professionnelles avec **Matplotlib** et **Seaborn**.
4. Identifier les corrélations entre météo et pollution.
5. Communiquer les insights aux décideurs.

Les données sont volontairement bruitées (formats incohérents, valeurs manquantes, outliers, doublons) afin de travailler sur leur correction.

---

## Données fournies

1. **stations.csv** (propre)
   * `station_id`, `station_name`, `city`, `lat`, `lon`, `station_type`
   * Environ 50 stations de mesure

2. **air_quality_raw.csv** (bruité)
   * `station_id`, `timestamp`, `pollutant`, `value`, `unit`
   * Polluants : PM2.5, PM10, NO2, O3, SO2, CO
   * Défauts : dates multi-formats (FR/ISO/US), valeurs négatives, valeurs aberrantes (>1000), doublons, séparateurs décimaux mixtes (`.` et `,`), valeurs textuelles dans colonnes numériques.

3. **weather_raw.csv** (bruité)
   * `city`, `timestamp`, `temperature_c`, `humidity_pct`, `wind_speed_kmh`, `precipitation_mm`, `weather_condition`
   * Défauts : valeurs manquantes en bloc, températures impossibles (<-50 ou >60), humidité >100%, formats de dates incohérents.

---

## Seuils réglementaires

| Polluant | Seuil information (ug/m3) | Seuil alerte (ug/m3) |
|----------|---------------------------|----------------------|
| PM2.5    | 25                        | 50                   |
| PM10     | 50                        | 80                   |
| NO2      | 200                       | 400                  |
| O3       | 180                       | 240                  |
| SO2      | 300                       | 500                  |

---

# Etapes du TP

---

## Etape 1 – Exploration et chargement Spark

**Contexte** : Les fichiers de pollution représentent plusieurs mois de données. Spark permet de les traiter efficacement.

**Travail attendu** :

* Créer une session Spark locale.
* Charger `air_quality_raw.csv` en DataFrame Spark.
* Afficher le schéma inféré et identifier les problèmes de typage.
* Calculer des statistiques descriptives par polluant.
* Compter les valeurs nulles par colonne.
* Identifier les stations avec le plus d'enregistrements.

**Livrables** :

* Notebook `01_exploration_spark.ipynb`.
* Synthèse des problèmes de qualité identifiés.

---

## Etape 2 – Nettoyage Spark et agrégation

**Contexte** : Transformer les données brutes en données exploitables.

**Travail attendu** :

* Parser les timestamps multi-formats avec une UDF Spark.
* Convertir les valeurs avec virgule décimale en float.
* Supprimer les valeurs négatives et les outliers (>1000 ug/m3).
* Dédupliquer sur `(station_id, timestamp, pollutant)`.
* Calculer les moyennes horaires par station et polluant.
* Sauvegarder en Parquet partitionné par `date`.

**Livrables** :

* Script `02_nettoyage_spark.py`.
* Fichiers Parquet dans `output/air_quality_clean/`.
* Rapport : lignes en entrée, lignes supprimées, lignes en sortie.

---

## Etape 3 – Nettoyage avancé Pandas

**Contexte** : Préparer les données météo et fusionner avec les données de pollution.

**Travail attendu** :

* Charger `weather_raw.csv` avec Pandas.
* Identifier et traiter les valeurs manquantes :
  - Interpolation linéaire pour température et humidité.
  - Forward fill pour les conditions météo.
* Corriger les valeurs aberrantes :
  - Températures hors [-40, 50] : remplacer par NaN puis interpoler.
  - Humidité hors [0, 100] : clipper.
* Standardiser les formats de dates.
* Fusionner avec les données de pollution (jointure sur ville et heure arrondie).

**Livrables** :

* Notebook `03_nettoyage_pandas.ipynb`.
* Dataset fusionné `output/pollution_meteo_clean.csv`.
* Rapport de nettoyage (avant/après par colonne).

---

## Etape 4 – Analyse exploratoire et statistiques

**Contexte** : Comprendre les patterns de pollution.

**Travail attendu** :

* Calculer les statistiques descriptives par polluant et par ville.
* Identifier les jours de dépassement des seuils réglementaires.
* Calculer la matrice de corrélation entre polluants et variables météo.
* Analyser la saisonnalité (pollution par mois, jour de semaine, heure).
* Identifier les top 10 journées les plus polluées.

**Livrables** :

* Notebook `04_analyse_exploratoire.ipynb`.
* Tableau des dépassements de seuils.
* Matrice de corrélation.

---

## Etape 5 – Visualisations Matplotlib

**Contexte** : Créer des graphiques pour le rapport annuel.

**Travail attendu** :

* Evolution temporelle des PM2.5 et PM10 sur 6 mois (line plot).
* Distribution des concentrations de NO2 par ville (boxplot).
* Heatmap des concentrations moyennes par heure et jour de semaine.
* Scatter plot température vs ozone avec régression.

Chaque figure doit inclure : titre, labels des axes, unités, légende si nécessaire.

**Livrables** :

* Notebook `05_visualisations_matplotlib.ipynb`.
* Figures exportées en PNG (300 dpi) dans `output/figures/`.

---

## Etape 6 – Visualisations Seaborn avancées

**Contexte** : Produire des visualisations statistiques pour l'équipe data science.

**Travail attendu** :

* Pairplot des polluants principaux (PM2.5, PM10, NO2, O3).
* Violin plot des PM2.5 par saison.
* Heatmap de la matrice de corrélation avec annotations.
* FacetGrid : evolution temporelle par ville (une ligne par ville).
* Jointplot : relation humidité/PM2.5 avec distributions marginales.

**Livrables** :

* Notebook `06_visualisations_seaborn.ipynb`.
* Figures exportées dans `output/figures/`.

---

## Etape 7 – Dashboard final et rapport

**Contexte** : Présenter les résultats au conseil régional.

**Travail attendu** :

* Créer une figure multi-panneaux (2x3) synthétisant les insights :
  - Panel 1 : Evolution temporelle PM2.5 avec seuils
  - Panel 2 : Carte de chaleur pollution par ville
  - Panel 3 : Corrélation météo/pollution
  - Panel 4 : Distribution par saison
  - Panel 5 : Top 5 villes les plus polluées
  - Panel 6 : Nombre de jours d'alerte par mois

* Rédiger un résumé des conclusions (10-15 lignes).

**Livrables** :

* Notebook `07_dashboard_final.ipynb`.
* Figure `output/figures/dashboard_qualite_air.png`.
* Fichier `output/rapport_conclusions.md`.

---

## Structure attendue du projet

```
tp_spark_pandas_viz/
├── data/
│   ├── stations.csv
│   ├── air_quality_raw.csv
│   └── weather_raw.csv
├── notebooks/
│   ├── 01_exploration_spark.ipynb
│   ├── 02_nettoyage_spark.py
│   ├── 03_nettoyage_pandas.ipynb
│   ├── 04_analyse_exploratoire.ipynb
│   ├── 05_visualisations_matplotlib.ipynb
│   ├── 06_visualisations_seaborn.ipynb
│   └── 07_dashboard_final.ipynb
├── output/
│   ├── air_quality_clean/          # Parquet partitionné
│   ├── pollution_meteo_clean.csv
│   ├── figures/
│   └── rapport_conclusions.md
└── README.md
```