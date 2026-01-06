import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('news_db.sqlite')

# Read the SQL migration file
with open('migrations/add_source_type_field.sql', 'r') as file:
    sql_script = file.read()

# Execute the SQL script
try:
    conn.executescript(sql_script)
    print("Migration applied successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    conn.close()