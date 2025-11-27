# DataEngineeringProject
This repository contains group 8's project in Data Engineering course held in 2025 autumn




Before the start the docker compose stack with containers should be built.


## Project Structure
```

```

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

* NB! NB! While building and composing the Superset container it may happen that .... containers do not start from the terminal, howevere their building was Correct, then one should start them from the Docker Desktop. The same goes for Open Meta Data, i.e. if building and composing of the stack was successful ,but some services did not start, one can start them from Docker Desktop.


2. Data fetch, making models: staging, marts
This section is needed to prepare the data-sets for the other tasks.
The staging data is used in the Task: " Apche Iceberg", ./marts/fact_people_traffic is used in Tasks: ClickHouse, OpenMetaData, SuperSet.


* To DROP all tables created in Clickhouse in "default" database:

```bash
docker exec -i clickhouse-server clickhouse-client --query "
SELECT 'DROP TABLE IF EXISTS ' || name || ';'
FROM system.tables
WHERE database='default';
" | docker exec -i clickhouse-server clickhouse-client
```


2. Create MinIO bucket:

* Login: <http://localhost:9501>
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
```
Tables: [('bronze_infrared',), ('bronze_mobility',), ('bronze_weather',)]
```

#
# docker exec -it airflow-webserver bash
#
# python ./scripts/01_check_tables_induckdb.py
```

4. Fetch data from ClickHOUSE

Check Iceberg tables via duckdb bash:

```
docker-compose exec duckdb_lab bash

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

-- DuckDB container can reach Iceberg REST (iceberg_rest:8181).

-- Iceberg REST can access the S3 warehouse in MinIO (s3://project-bucket/).

-- Your Iceberg table exists: bronze_infrared in the default namespace.

###############################

docker exec -it clickhouse-server clickhouse-client

:) SHOW DATABASES;

SHOW DATABASES

Query id: 042aa5c1-9330-4518-aa0b-aae9d7282dd4

   ┌─name───────────────┐
1. │ INFORMATION_SCHEMA │
2. │ default            │
3. │ information_schema │
4. │ peopletraffic      │
5. │ system             │
   └────────────────────┘

5 rows in set. Elapsed: 0.003 sec.

Not seen:
iceberg_catalog_default



5. CREATE USERS, ROLES IN CH


CREATE ROLE IF NOT EXISTS jun_analyst_role;

CREATE USER IF NOT EXISTS jun_analyst_user IDENTIFIED WITH plaintext_password BY 'password123';

--GRANT SELECT(salary, department, location, hire_date) ON sec_demo.employees TO jun_analyst_role WITH GRANT OPTION;

GRANT SELECT(salary, department, location, hire_date) ON sec_demo.employees TO jun_analyst_user;





## Task 0: Check OpenMetadata UI

Use `docker compose up -d` to start the OpenMetadata services.
Note: this can take several minutes.

Then, navigate to the OpenMetadata UI by opening your browser and going to `localhost:8585`

The default Username and Password are:
```
Username - admin@open-metadata.org
Password - admin
```

Connect to Clickhouse database: peopletraffic.

Use the CH database: peopletraffic with tables.






<details>
<summary>Example scripts</summary>

`docker exec -it clickhouse-server-omd bash`
`clickhouse-client --multiquery --queries-file=/sql/01_create_db_and_tables.sql`
`clickhouse-client --multiquery --queries-file=/sql/02_load_queries.sql`

</details>

Next, you need to create a Clickhouse user for OpenMetadata. Create the user `service_openmetadata` and assign it to a role `role_openmetadata`.
Add the role SELECT and SHOW access to `system` database.
Then, add the role SELECT rights on the `supermarket` database.

<details>
<summary>Example solution</summary>

```
CREATE ROLE role_openmetadata;

CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';

GRANT role_openmetadata TO service_openmetadata;

GRANT SELECT, SHOW ON system.* to role_openmetadata;

GRANT SELECT ON supermarket.* TO role_openmetadata;
```
</details>



<summary>Example solution</summary>

In the OpenMetadata UI:
* Go to **Settings → Services → Databases**
* Click **+ Add New Service**
* Choose **ClickHouse** as the service type
* Fill in the connection details (adapt as needed):
  * **Service Name:**
  e.g. `clickhouse_warehouse`, can be whatever you would like
  * **Host and Port:**
  Use the Docker service name and HTTP port, for example:
  `clickhouse-server-omd:8123`
  * **Username:** `service_openmetadata`
  * **Password:** `omd_very_secret_password`
  * **Database / Schema:**
  you can leave empty
  * **Https / Secure:**
  leave them off, we have not configured Clickhouse for HTTPS or SSL/TLS.
  * Click **Test Connection**
  * If successful, click **Next** and **Save** the service.

</details>



<summary>Superset docker-init.sh file execution enabling</summary>
chmod +x docker/docker-init.sh
chmod +x docker/docker-bootstrap.sh


docker compose up --build

Then open Superset in your browser:

- URL: http://localhost:8088
- Login: for now, use the default credentials ( `admin` / `admin`)

### 0.3 Create a Superset service account

Create a service account in ClickHouse for Superset application. It should have SELECT rights on supermarket schema.

<details>
<summary>Example solution</summary>

```
CREATE ROLE role_superset_full;

CREATE USER service_superset_full IDENTIFIED WITH sha256_password BY 'superset_very_secret_password';

GRANT role_superset_full TO service_superset_full;

GRANT SELECT ON supermarket.* TO role_superset_full;
```
</details>




