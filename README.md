# Canada Weather ETL Pipeline

An Apache Airflow pipeline that collects current weather for 25 Canadian cities from the Open-Meteo API once a day, reshapes it, and loads it into PostgreSQL. It runs locally with the Astro CLI and Docker.

## Overview

```mermaid
flowchart LR
    A[Open-Meteo API] -->|extract| B[extract_weather_data]
    B -->|transform| C[transform_weather_data]
    C -->|load| D[(PostgreSQL: weather_canada)]
    D --> E[pgAdmin]
```

| Step | Task | What it does |
|---|---|---|
| Extract | `extract_weather_data` | Calls the Open-Meteo forecast endpoint for each city through an Airflow `HttpHook` |
| Transform | `transform_weather_data` | Keeps the fields of interest and attaches the city name and province |
| Load | `load_weather_data` | Creates the table if needed and inserts all rows in a single transaction through a `PostgresHook` |

The pipeline covers the largest Canadian cities plus every provincial and territorial capital.

## Tech stack

- **Apache Airflow 3** on Astro Runtime, using the TaskFlow API (`@task`)
- **PostgreSQL 16** as the data store
- **pgAdmin 4** for browsing the database
- **Docker** and the **Astro CLI** for local development
- **Open-Meteo API** as the data source (free, no API key required)

## Design choices

- **Separate tasks for extract, transform and load.** Each step is visible in the Airflow UI and can be retried on its own.
- **Hooks and connections instead of hardcoded credentials.** The DAG only refers to connection IDs; hostnames and passwords live in `.env`.
- **A dedicated data database.** Weather data is stored in its own Postgres container (`weather-db`), separate from the database Airflow uses for its own metadata.
- **Fail loudly.** A failed API call or insert fails the task, and the load rolls back so the table is never left half-written.
- **Retries.** Each task retries twice, five minutes apart.

## Project structure

```
.
├── dags/
│   └── weather_etl.py              # The DAG (dag_id: weather_etl_pipeline)
├── docker-compose.override.yml     # Adds weather-db and pgAdmin to the Astro stack
├── requirements.txt                # Airflow provider packages
├── .env                            # Credentials and Airflow connections (not committed)
├── Dockerfile                      # Astro Runtime image
└── README.md
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

## Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

### 2. Create the `.env` file

Create a file named `.env` in the project root with your own values:

```dotenv
# pgAdmin login
PGADMIN_DEFAULT_EMAIL=you@example.com
PGADMIN_DEFAULT_PASSWORD=change_me

# Weather database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=weatherdb

# Airflow connections
AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://your_user:your_password@weather-db:5432/weatherdb
AIRFLOW_CONN_OPEN_METEO_API={"conn_type":"http","host":"https://api.open-meteo.com"}
```

The user, password and database name in `AIRFLOW_CONN_POSTGRES_DEFAULT` must match the three `POSTGRES_` values.

### 3. Check `requirements.txt`

```
apache-airflow-providers-postgres
apache-airflow-providers-http
```

### 4. Start the stack

```bash
astro dev start
```

| Service | Address | Notes |
|---|---|---|
| Airflow UI | http://localhost:8080 | |
| pgAdmin | http://localhost:5050 | Log in with the `PGADMIN_` values from `.env` |
| Weather database | `localhost:5433` | From your own machine |

## Running the pipeline

1. Open the Airflow UI at http://localhost:8080.
2. Switch on the toggle next to `weather_etl_pipeline` to unpause it.
3. Click **Trigger** to run it immediately.

Once unpaused, the DAG runs on its own every day at 00:00 UTC (`@daily`). Scheduled runs are not backfilled (`catchup=False`), so a run is skipped if Airflow is stopped at that time.

## Viewing the data

In pgAdmin, register a server with these settings:

| Field | Value |
|---|---|
| Host | `weather-db` |
| Port | `5432` |
| Username / Password | The `POSTGRES_` values from `.env` |
| Database | The `POSTGRES_DB` value from `.env` |

The table appears under Schemas → public → Tables after the first successful run.

### Table: `weather_canada`

| Column | Type | Description |
|---|---|---|
| `city` | TEXT | City name |
| `province` | TEXT | Province or territory code |
| `latitude` | FLOAT | Latitude used for the request |
| `longitude` | FLOAT | Longitude used for the request |
| `temperature` | FLOAT | Current temperature (°C) |
| `windspeed` | FLOAT | Wind speed (km/h) |
| `winddirection` | FLOAT | Wind direction (degrees) |
| `weathercode` | INT | WMO weather condition code |
| `timestamp` | TIMESTAMP | When the row was loaded |

