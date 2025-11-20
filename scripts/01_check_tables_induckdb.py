import duckdb

con = duckdb.connect('/opt/airflow/db/lab.duckdb')

# Get list of tables
tables = con.execute('SHOW TABLES;').fetchall()
print('Tables:', tables)

# Fetch and print data for each table
for table in tables:
    table_name = table[0]
    print(f'\nData from table {table_name}:')
    rows = con.execute(f'SELECT * FROM {table_name} LIMIT 10;').fetchall()  # limit to 10 rows
    for row in rows:
        print(row)

con.close()



