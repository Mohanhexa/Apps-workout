from pyhive import hive
import csv
import re

# Hive connection config
HIVE_HOST = 'your-hive-host'
HIVE_PORT = 10000
HIVE_USER = 'your-username'
HIVE_DB = 'your_database'

# List of specific tables to extract schema for
target_tables = ['users', 'orders', 'products']

# Regex patterns
type_pattern = re.compile(r"(varchar|char)\((\d+)\)", re.IGNORECASE)

# Connect to Hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, database=HIVE_DB)
cursor = conn.cursor()

# Prepare output data
columns_output = []
table_metadata_output = []

def parse_describe_formatted_output(table_name, rows):
    section = "columns"
    for row in rows:
        col_name = row[0].strip() if row[0] else ''
        data_type = row[1].strip() if len(row) > 1 and row[1] else ''
        comment = row[2].strip() if len(row) > 2 and row[2] else ''

        # Section switches
        if col_name.startswith("# Partition Information"):
            section = "partitions"
            continue
        elif col_name.startswith("# Detailed Table Information"):
            section = "table_info"
            continue

        # Skip headers and blanks
        if col_name.startswith("#") or not col_name:
            continue

        # Columns
        if section in ["columns", "partitions"]:
            length = ""
            match = type_pattern.search(data_type)
            if match:
                length = match.group(2)

            columns_output.append({
                "table_name": table_name,
                "column_name": col_name,
                "column_type": "Partition Column" if section == "partitions" else "Regular Column",
                "data_type": data_type,
                "length": length,
                "comment": comment
            })

        # Table-level metadata
        elif section == "table_info":
            table_metadata_output.append({
                "table_name": table_name,
                "property": col_name,
                "value": data_type
            })

# Loop through tables and extract metadata
for table in target_tables:
    print(f" Processing table: {table}")
    try:
        cursor.execute(f"DESCRIBE FORMATTED {table}")
        results = cursor.fetchall()
        parse_describe_formatted_output(table, results)
    except Exception as e:
        print(f" Error with table {table}: {e}")

#  Save column-level info to CSV
with open("columns.csv", mode="w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "table_name", "column_name", "column_type", "data_type", "length", "comment"
    ])
    writer.writeheader()
    writer.writerows(columns_output)

# Save table-level metadata to CSV
with open("table_metadata.csv", mode="w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["table_name", "property", "value"])
    writer.writeheader()
    writer.writerows(table_metadata_output)

print("\n Export complete.")
print(" columns.csv → Column-level schema")
print(" table_metadata.csv → Table-level metadata")
