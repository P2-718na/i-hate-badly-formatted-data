import sqlite3
import csv

# File paths
csv_file_path = '../data/raw/edge_hour_stats_with_std.csv'  # Replace with your actual CSV file path
sqlite_db_path = '../data/genova.db'      # Desired SQLite DB file name

# Connect to SQLite database (creates if it doesn't exist)
conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()


cursor.execute('PRAGMA foreign_keys = ON;')
cursor.execute("DROP TABLE IF EXISTS traffic_data;")

# Create table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS traffic_data (
        edge_id TEXT,
        hour_slot INTEGER,
        speed_mean REAL,
        count_mean REAL,
        min_mean REAL,
        max_mean REAL,
        stddev_mean REAL,
        confidence_mean REAL,
        speed_stddev REAL,
        count_stddev REAL,
        min_stddev REAL,
        max_stddev REAL,
        stddev_stddev REAL,
        confidence_stddev REAL,
        edge_T BOOLEAN,
        edge_F BOOLEAN,
        FOREIGN_KEY 
    );
''')

# Read and insert data row by row, with progress feedback
row_count = 0
progress_interval = 10000  # Print progress every 10,000 rows

with open(csv_file_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:

        if  row['edge_id'][-1] not in ["T", "F"]:
            raise "DIOCANR"
        
        edge_T =  row['edge_id'][-1] == "T"

        values = (
            row['edge_id'],
            int(row['hour_slot']),
            float(row['speed_mean']),
            float(row['count_mean']),
            float(row['min_mean']),
            float(row['max_mean']),
            float(row['stddev_mean']),
            float(row['confidence_mean']),
            float(row['speed_stddev']),
            float(row['count_stddev']),
            float(row['min_stddev']),
            float(row['max_stddev']),
            float(row['stddev_stddev']),
            float(row['confidence_stddev']),
            edge_T,
            not edge_T
        )
        cursor.execute('''
            INSERT INTO traffic_data (
                edge_id, hour_slot, speed_mean, count_mean, min_mean, max_mean,
                stddev_mean, confidence_mean, speed_stddev, count_stddev,
                min_stddev, max_stddev, stddev_stddev, confidence_stddev, edge_T, edge_F
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ''', values)

        row_count += 1
        if row_count % progress_interval == 0:
            print(f"{row_count} rows inserted...")

# Final commit and close
conn.commit()
conn.close()

print(f"Import complete: {row_count} total rows written to '{sqlite_db_path}'")
