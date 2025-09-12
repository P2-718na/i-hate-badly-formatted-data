import sqlite3
import json

# File paths
geojson_path = '../data/raw/genova_full.geojson'
sqlite_db_path = '../data/genova.db'

# Load GeoJSON
with open(geojson_path) as f:
    gdf = json.load(f)

# Connect to SQLite
conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS geo_nodes;")

create_table_sql = """
CREATE TABLE geo_nodes (
    id INTEGER NOT NULL UNIQUE,
    lat REAL,
    lon REAL,
    PRIMARY KEY(id)
);
"""

cursor.execute(create_table_sql)

for row in gdf["features"]:
    idF = row["properties"]["poly_nF"]
    idT = row["properties"]["poly_nT"]

    [lonF, latF] = row["geometry"]["coordinates"][0]
    [lonT, latT] = row["geometry"]["coordinates"][-1]

    valuesF = (
        idF,
        latF,
        lonF
    )
    valuesT = (
        idT,
        latT,
        lonT
    )

    cursor.executemany('''
        INSERT OR IGNORE INTO geo_nodes (
            id, lat, lon
        ) VALUES (?, ?, ?);
    ''', [valuesT, valuesF])


# Finalize
conn.commit()
conn.close()

print("GeoJSON data inserted into 'geo_edges' table.")
