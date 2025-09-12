import sqlite3
import json
import geopandas as gpd

# File paths
geojson_path = '../data/raw/genova_full.geojson'
sqlite_db_path = '../data/genova.db'

# Load GeoJSON
gdf = gpd.read_file(geojson_path)

# Convert geometry and forbidden_turns to strings
gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
gdf['forbidden_turns'] = gdf['forbidden_turns'].apply(lambda x: json.dumps(x))

# nth bit of a
def nth_bit(a, n):
    return (a >> n) & 1

exclusive_encodings = {
    "car": 0,
    "pawn": 1,
    "bus": 2,
    "metro": 4,
    "train": 5,
    "bike": 6,
    # other two bits are unknown lol kys
}

for category, code in exclusive_encodings.items():
    gdf["F_" + category] = gdf["exclusive_FT"].apply(lambda a: nth_bit(a, code))
    gdf["T_" + category] = gdf["exclusive_TF"].apply(lambda a: nth_bit(a, code))


gdf = gdf.drop("exclusive_FT", axis=1)
gdf = gdf.drop("exclusive_TF", axis=1)
gdf = gdf.drop("poly_lid", axis=1)

# Connect to SQLite
conn = sqlite3.connect(sqlite_db_path)

conn.execute("PRAGMA foreign_keys = ON")
conn.execute("DROP TABLE IF EXISTS geo_edges;")

# Create table with explicit schema
# speed_cat INTEGER needs to be added, but formatted properly before
# poly_cid corresponds to traffic data edge ID

# TODO split Poly cd into T F separately kill yourself midcity
create_table_sql = """
CREATE TABLE geo_edges (
    name TEXT,
    poly_cid INTEGER,
    poly_length REAL,
    poly_nF INTEGER,
    poly_nT INTEGER,
    speed_FT REAL,
    speed_TF REAL,
    speed_cat INTEGER,
    width_FT REAL,
    width_TF REAL,
    F_car BOOLEAN,
    F_pawn BOOLEAN,
    F_bus BOOLEAN,
    F_metro BOOLEAN,
    F_train BOOLEAN,
    F_bike BOOLEAN,
    T_car BOOLEAN,
    T_pawn BOOLEAN,
    T_bus BOOLEAN,
    T_metro BOOLEAN,
    T_train BOOLEAN,
    T_bike BOOLEAN,
    forbidden_turns TEXT,
    geometry TEXT,
    FOREIGN KEY(poly_nF) REFERENCES geo_nodes(id),
    FOREIGN KEY(poly_nT) REFERENCES geo_nodes(id)
);
"""

conn.execute(create_table_sql)

# Insert data
# Do not use if_exists="replace" otherwise pandas WILL break everything (shitty ass library devs kys please)
gdf.to_sql('geo_edges', conn, if_exists='append', index=False)

# Finalize
conn.commit()
conn.close()

print("GeoJSON data inserted into 'geo_edges' table.")
