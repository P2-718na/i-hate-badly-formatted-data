import sqlite3
import json

# File paths
geojson_path = '../data/raw/genova_full.geojson'
sqlite_db_path = '../data/genova.db'

# Load GeoJSON
with open(geojson_path) as f:
    gdf = json.load(f)

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

# Connect to SQLite
conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()

cursor.execute("PRAGMA foreign_keys = ON")
cursor.execute("DROP TABLE IF EXISTS geo_edges;")

create_table_sql = """
CREATE TABLE geo_edges (
    edge_id INTEGER NOT NULL UNIQUE,
    is_valid BOOLEAN,
    edge_length REAL,
    node_from_id INTEGER,
    node_to_id INTEGER,
    speed REAL,
    speed_cat INTEGER,
    width REAL,
    car_allowed BOOLEAN,
    pawn_allowed BOOLEAN,
    bus_allowed BOOLEAN,
    metro_allowed BOOLEAN,
    train_allowed BOOLEAN,
    bike_allowed BOOLEAN,
    forbidden_turns TEXT,
    street_name TEXT,
    geometry TEXT,
    PRIMARY KEY(edge_id),
    FOREIGN KEY(node_from_id) REFERENCES geo_nodes(id),
    FOREIGN KEY(node_to_id) REFERENCES geo_nodes(id)
);
"""
cursor.execute(create_table_sql)


for row in gdf["features"]:
    props = row["properties"]
    coords = row["geometry"]["coordinates"]

    street_name = props["name"]
    edge_id = props["poly_cid"]
    edge_length = props["poly_length"]
    F = props["poly_nF"]
    T = props["poly_nT"]
    speed_cat = props["speed_cat"]

    exclusive_FT = props["exclusive_FT"]
    exclusive_TF = props["exclusive_TF"]

    valuesF = (
        edge_id,
        exclusive_FT != -1,
        edge_length,
        F,
        T,
        props["speed_FT"],
        speed_cat,
        props["width_FT"],
        nth_bit(exclusive_FT, exclusive_encodings["car"]),
        nth_bit(exclusive_FT, exclusive_encodings["pawn"]),
        nth_bit(exclusive_FT, exclusive_encodings["bus"]),
        nth_bit(exclusive_FT, exclusive_encodings["metro"]),
        nth_bit(exclusive_FT, exclusive_encodings["train"]),
        nth_bit(exclusive_FT, exclusive_encodings["bike"]),
        json.dumps(props["forbidden_turns"]),
        street_name,
        json.dumps(coords)
    )

    valuesT = (
        -edge_id, # T roads get negative index
        exclusive_TF != -1,
        edge_length,
        T,
        F,
        props["speed_TF"],
        speed_cat,
        props["width_TF"],
        nth_bit(exclusive_TF, exclusive_encodings["car"]),
        nth_bit(exclusive_TF, exclusive_encodings["pawn"]),
        nth_bit(exclusive_TF, exclusive_encodings["bus"]),
        nth_bit(exclusive_TF, exclusive_encodings["metro"]),
        nth_bit(exclusive_TF, exclusive_encodings["train"]),
        nth_bit(exclusive_TF, exclusive_encodings["bike"]),
        json.dumps(props["forbidden_turns"]),
        street_name,
        json.dumps(coords[::-1])
    )

    values = [valuesF, valuesT]
    '''if exclusive_FT != -1:
        values.append(valuesF)

    if exclusive_TF != -1:
        values.append(valuesT)'''
    # We need to add also invalid roads because data is retarded

    cursor.executemany('''
        INSERT INTO geo_edges (
            edge_id,
            is_valid,
            edge_length,
            node_from_id,
            node_to_id,
            speed,
            speed_cat,
            width,
            car_allowed,
            pawn_allowed,
            bus_allowed,
            metro_allowed,
            train_allowed,
            bike_allowed,
            forbidden_turns,
            street_name,
            geometry
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', values)


# Finalize
conn.commit()
conn.close()

print("GeoJSON data inserted into 'geo_edges' table.")
