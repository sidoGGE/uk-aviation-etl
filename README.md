# UK Aviation ETL Project

This project collects UK flight data from the OpenSky Network, enriches it with weather data from OpenWeatherMap, and loads it into a Postgres database using an Apache Airflow DAG.

## What the project does

1. Fetch flight state data from OpenSky public API.
2. Filter flight data to UK bounding box coordinates.
3. Enrich each flight with weather data from OpenWeatherMap.
4. Save enriched data into two Postgres tables:
   - `uk_flights_weather`
   - `raw_uk_flights`
5. Run the pipeline hourly using Airflow.

## Data sources

- **OpenSky Network**
  - Endpoint: `https://opensky-network.org/api/states/all`
  - This public endpoint provides live flight state data and does not require an API key for the UK bounding box used in this project.

- **OpenWeatherMap**
  - Endpoint: `https://api.openweathermap.org/data/2.5/weather`
  - Requires an API key stored in `.env` as `WEATHER_API_KEY` 

## Project files

- `dags/aviation_dag.py` - Main Airflow DAG and ETL logic
- `docker-compose.yaml` - Docker configuration for Airflow, Postgres, and pgAdmin
- `.env` - Local environment file containing secrets and configuration
- `.env.example` - Example file showing required variables

## Required external services

- Docker and Docker Compose installed locally
- A valid OpenWeatherMap API key

## Setup steps

1. Copy `.env.example` to `.env`
2. Fill in the `.env` values:
   - `WEATHER_API_KEY`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_NAME`
   - `DB_HOST`
   - `DB_PORT`
3. Do not commit `.env` to GitHub

## Running the project

From the project root directory:

```bash
docker-compose up -d
```

This starts:
- `airflow-init` to initialize Airflow metadata
- `db` with Postgres
- `pgadmin` for database UI
- `airflow-webserver`
- `airflow-scheduler`

## Accessing the app

- Airflow Web UI: `http://localhost:8080`
- pgAdmin: `http://localhost:8081`

## How the DAG works

- The DAG is named `uk_aviation_pipeline`.
- It runs every hour using `schedule='@hourly'`.
- The Python task `fetch_and_store_aviation_data` executes `run_aviation_etl()`.
- Secrets are loaded from environment variables, not hardcoded.

## Notes for GitHub users

- Keep `.env` local and private.
- Only push `.env.example` as a template.
- The GitHub reader does not need the actual keys to understand the project structure.

## Optional improvements

- Add error handling for OpenWeatherMap rate limits.
- Store historical data instead of replacing tables.
- Add DAG task dependencies or separate extract/transform/load steps.
