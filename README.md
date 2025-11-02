# DataEngineeringProject
This repository contains group 8's project in Data Engineering course held in 2025 autumn

## How to Run

```
docker-compose up -d
```


If the Airflow doesn't initialize, try before docker compose:
```
docker compose run --rm airflow-init
```

Access Airflow at:  
[http://localhost:8080](http://localhost:8080)
Username: airflow
Password: airflow

If the Dag isn't automatically on, pleas turn it active.
