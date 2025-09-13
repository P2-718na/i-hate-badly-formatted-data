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
# Need to drop forbinned turns before geo edges otherwise foreign key breaks
cursor.execute("DROP TABLE IF EXISTS forbidden_turns;")
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
    street_name TEXT,
    geometry TEXT,
    PRIMARY KEY(edge_id),
    FOREIGN KEY(node_from_id) REFERENCES geo_nodes(id),
    FOREIGN KEY(node_to_id) REFERENCES geo_nodes(id)
);
"""
cursor.execute(create_table_sql)
create_table_sql = """
CREATE TABLE forbidden_turns (
    edge_source INTEGER NOT NULL,
    edge_destination INTEGER NOT NULL,
    FOREIGN KEY(edge_source) REFERENCES geo_edges(edge_id),
    FOREIGN KEY(edge_destination) REFERENCES geo_edges(edge_id)
);
"""
cursor.execute(create_table_sql)

print("Created tables")

forbidden_turns = {}

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
        street_name,
        json.dumps(coords[::-1])
    )

    fturns = props["forbidden_turns"]
    if len(fturns) != 0:
        forbidden_turns[edge_id] = fturns
        forbidden_turns[-edge_id] = fturns

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
            street_name,
            geometry
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', values)

conn.commit()
print("Uploaded edges")

for edge, turns in forbidden_turns.items():
    values = []
    for turn in turns:
        values.append((edge, turn))
        values.append((edge, -turn))
    
    # Some forbidden turns are non existent IDs. We just ignore them
    # using this sketchy query.
    # Only edge_destination needs to be checked, as the source is alwasy
    # added into the db
    cursor.executemany('''
        WITH cte(edge_source, edge_destination)
        AS (VALUES (?, ?))
        INSERT INTO forbidden_turns (
            edge_source,
            edge_destination
        )
        SELECT c.edge_source, c.edge_destination
        FROM cte c
        WHERE EXISTS (
            SELECT 1 FROM geo_edges g WHERE g.edge_id = c.edge_destination
        )
    ''', values)

print("Uploaded forbidden turns")

# Finalize
conn.commit()
conn.close()

print("GeoJSON data inserted into 'geo_edges' table.")
