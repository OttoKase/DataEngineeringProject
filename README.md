# DataEngineeringProject
This repository contains group 8's project in Data Engineering course held in 2025 autumn

## Project Structure
```

```

## How to Run

```
docker compose up -d --build

```




2. Create MinIO bucket:

* Login: [http://localhost:9001](http://localhost:9001)
* Bucket: `project-bucket`

3. Access DuckDB container via airflow-webserver:

```bash
docker exec -it airflow-webserver python3 -c "
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
"
#
# docker exec -it airflow-webserver bash
#
# python ./scripts/01_check_tables_induckdb.py
```

