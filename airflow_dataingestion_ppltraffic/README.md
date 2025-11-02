## Login credentials

Username: airflow
Password: airflow

---

## Credentials

| Component | Username | Password | Port |
|------------|-----------|-----------|------|
| **airflow-db** | `airflow` | `airflow` | 5432 |
| **peopletraffic-db** | `prices_user` | `prices_pass` | 5433 |
| **pgAdmin** | `admin@example.com` | `admin` | 5050 |

Access pgAdmin at:  
[http://localhost:5050](http://localhost:5050)

Access Airflow at:  
[http://localhost:8080](http://localhost:8080)

Connecting `peopletraffic-db` through PgAdmin

| Field                    | Value                                                |
| ------------------------ | ---------------------------------------------------- |
| **Host name / address**  | `peopletraffic-db` *(use service name from docker-compose)* |
| **Port**                 | `5432`                                               |
| **Maintenance database** | `postgres`                                          |
| **Username**             | `prices_user`                                        |
| **Password**             | `prices_pass`                                        |

---
