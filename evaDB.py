# Import the EvaDB package
import evadb

cursor = evadb.connect().cursor()

# Connect to the existing humanDNA_data database
params = {
    "user": "postgres",
    "password": "Mm373619!",
    "host": "localhost",
    "port": "5432",
    "database": "postgres",  # Use the existing database name
}

# Drop the existing database if it exists
drop_database_query = "DROP DATABASE IF EXISTS humanDNA_data;"
cursor.query(drop_database_query).df()

# Create a new database named "humanDNA_data"
create_database_query = f"CREATE DATABASE humanDNA_data WITH ENGINE = 'postgres', PARAMETERS = {params};"
cursor.query(create_database_query).df()

# Specify the table name
table_name = 'humanDNA_table'

# Drop the existing table if it exists
drop_table_query = f"""
    USE humanDNA_data {{
        DROP TABLE IF EXISTS {table_name}
    }}
"""

cursor.query(drop_table_query).df()

# Create a new table in the "humanDNA_data" database
create_table_query = f"""
    USE humanDNA_data {{
        CREATE TABLE IF NOT EXISTS {table_name} (
            sequence_id SERIAL PRIMARY KEY,
            sequence_text TEXT,
            class_label TEXT
        )
    }}
"""
cursor.query(create_table_query).df()


file_path = '/Users/mokshith/Downloads/DNAClassificationData/human.txt'

# Use COPY command excluding the sequence_id column
cursor.query(f"""
  USE humanDNA_data {{
    COPY {table_name}(sequence_text, class_label)
    FROM '{file_path}' WITH (FORMAT CSV, DELIMITER E'\t', HEADER TRUE)
  }}
""").df()

select_query = f"SELECT * FROM {table_name}"
result = cursor.query(select_query).df()
