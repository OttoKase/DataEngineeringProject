# DataEngineeringProject
This repository contains group 8's project in Data Engineering course held in 2025 autumn

## NB! All the authentication credentials for different services used are located in .env file.

## 1. Project Structure
```
├── compose.yml
├── config
│   └── clickhouse
├── dags
│   ├── 01_data_fetch_mkCHtbls.py
│   ├── 02_minio_to_duckdb.py
│   ├── 03_duckdb_to_iceberg.py
│ ├── dbt
│   ├── dbt_packages
│   ├── dbt_project.yml
│   ├── logs
│   ├── models
│   ├── profiles.yml
│   ├── seeds
│   └── target
├── docker
├── Dockerfile
├── Dockerfile.airflow
├── docker-volume
│   └── db-data
├── duckdb_lab
│   └── lab.duckdb
├── etc
│   └── requirements.txt
├── fact_peopletraffic_vs_weather.jpg
├── images
│   ├── b_name_pin_modeprcphr.png
│   ├── b_name_pin_modeprcphr_september.png
│   ├── dashboard.png
│   ├── iceberg_tbl.png
│   └── start_from_ddesktop.png
├── logs
├── minio_data
│   └── project-bucket
├── pgdata_airflow
├── pgdata_weather
├── README.md
├── sample_data
│   ├── bronze_infrared.csv
│   └── bronze_weather.csv
├── sql
│   ├── 01_create_DB_and_tables.sql
│   └── 02_load_data_from_csv.sql
└── superset-core
```

## Before the start the docker compose stack with containers should be built.


## How to Run

```
docker compose build --no-cache

docker compose up -d

```
## How to Up and Down a certain container

```
docker compose build --no-cache <service_name>
docker compose up -d <service_name>

docker compose stop <service_name>

```

* NB! NB! While building and composing the Superset container it may happen that some superset_* containers do not start from the terminal, howevere their building was Correct. Then one should start them from the Docker Desktop. The same goes for OpenMetaData (OMD), i.e. if building and composing of the stack were successful, but some services did not start, one can start them from Docker Desktop.


![Running the service(s) via Docker Desktop. Example with SuperSet_*](/images/start_from_ddesktop.png)



## 2 Data fetch, making dbt models: bronze, gold

These activities are implemented via Airflow DAG-1 (see Figure below). There is a need to prepare the data-sets for the other tasks.

* The raw-level data is used in the Task: "2. Apache Iceberg";

* The gold-level data, i.e. ./gold/fact_people_traffic is used in Tasks 3, 4, 5: ClickHouse, OpenMetaData, SuperSet.

* Documentation for the dbt project can be generated via:

```
docker exec dbt dbt docs generate
```
and/or be found in:

```
/dbt/target/catalog.json
```

![Airflow DAGs used in the current Project-3](/images/DAG_1,2,3_ppl_traffic.drawio.png)


* To DROP all tables created in Clickhouse in "default_gold" database:

```bash
docker exec -it clickhouse-server clickhouse-client --query "
SELECT concat('DROP TABLE IF EXISTS default_gold.', name, ';')
FROM system.tables
WHERE database = 'default_gold';" | docker exec -i clickhouse-server clickhouse-client
```

## 3. Create MinIO bucket:

* Login: [http://localhost:9501]
* Bucket: `project-bucket`

## 4. Access DuckDB container via airflow-webserver and check for the tables available:

```
Tables: [('bronze_infrared',), ('bronze_weather',)]
```

```bash
docker exec -it airflow-webserver python

>>> import duckdb
>>> con = duckdb.connect('/opt/airflow/db/lab.duckdb')

# Get list of tables
>>> tables = con.execute('SHOW TABLES;').fetchall()
>>> print('Tables:', tables)

# Fetch and print data for each table
>>> for table in tables:
    table_name = table[0]
    print(f'\nData from table {table_name}:')
    rows = con.execute(f'SELECT * FROM {table_name} LIMIT 10;').fetchall()  # limit to 10 rows
    for row in rows:
        print(row)
>>> con.close()
```

## 5. Fetch Iceberg table from ClickHouse

* Iceberg table is created via Airflow DAG:

![Iceberg table created via Airflow DAG](/images/iceberg_tbl.png)

* One can check the Iceberg table via duckdb bash:
```bash

docker compose exec duckdb_lab bash

python

from pyiceberg.catalog import load_catalog

# Load the REST catalog
catalog = load_catalog(
    "rest",
    uri="http://iceberg_rest:8181",
    warehouse="s3://project-bucket/",
    io_impl="org.apache.iceberg.aws.s3.S3FileIO",
    s3_endpoint="http://minio:9000",
    s3_access_key_id="minioadmin",
    s3_secret_access_key="minioadmin"
)

# List tables in the 'default' namespace
tables = catalog.list_tables(("default",))  # pass namespace as a tuple
print(tables)

[('default', 'bronze_infrared')]

```

## 6. Create Roles, Views in ClickHouse (OTTO !)
```sql
CREATE ROLE IF NOT EXISTS jun_analyst_role;

CREATE USER IF NOT EXISTS jun_analyst_user IDENTIFIED WITH plaintext_password BY 'password123';

GRANT SELECT(salary, department, location, hire_date) ON sec_demo.employees TO jun_analyst_role WITH GRANT OPTION;

GRANT SELECT(salary, department, location, hire_date) ON sec_demo.employees TO jun_analyst_user;
```


## 7. OpenMetaData (OMD) (TERJE !!)

Navigate to the OpenMetadata UI by opening your browser and going to `localhost:8585`
The default Username and Password are:


Create a Clickhouse user for OpenMetadata. Create the user `service_openmetadata` and assign it to a role `role_openmetadata`.
Add the role SELECT and SHOW access to `system` database.
Then, add the role SELECT rights on the `supermarket` database.


```sql
CREATE ROLE role_openmetadata;

CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';

GRANT role_openmetadata TO service_openmetadata;

GRANT SELECT, SHOW ON system.* to role_openmetadata;

GRANT SELECT ON supermarket.* TO role_openmetadata;

```



## 8. SuperSet
For making the Superset docker-init.sh file executable, one should change the following (applies for Unix system users)
```bash

chmod +x docker/docker-init.sh
chmod +x docker/docker-bootstrap.sh
```

Then open Superset in your browser:

- URL: http://localhost:8088

###  Create a Superset service account

Create a service account in ClickHouse for Superset application.
It should have SELECT rights on "default_gold" schema.

```sql
CREATE ROLE role_superset_full;

CREATE USER peopletraffic_user IDENTIFIED WITH sha256_password BY 'peopletraffic_pass';

GRANT role_superset_full TO peopletraffic_user;

GRANT SELECT ON default_gold.* TO role_superset_full;

```

###  Superset example Datasets can be created, such as:
Dataset from SQL

```sql
SELECT
    building_name,
    SUM(people_in) AS total_people_in,
    anyHeavy(toHour(join_timestamp)) AS mode_hour,
    anyHeavy(prcp) AS mode_prcp
FROM default_gold.fact_people_traffic
WHERE prcp != 0
  AND toMonth(join_timestamp) = 9  -- only September
GROUP BY building_name
ORDER BY building_name
LIMIT 1000;


SELECT
    building_name,
    toStartOfWeek(join_timestamp) AS week_start,  -- start of the week
    anyHeavy(prcp) AS mode_prcp,                  -- statistical mode of prcp within that week & building
    SUM(people_in) AS total_people_in
FROM default_gold.fact_people_traffic
WHERE prcp != 0
GROUP BY building_name, week_start
ORDER BY mode_prcp DESC, building_name DESC, week_start DESC
LIMIT 1000;

```

![SeperSet Dashboard answering the BQ-1 and BQ-2](/images/dashboard.png)


