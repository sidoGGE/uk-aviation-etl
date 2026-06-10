import os  # import operating system utilities for reading environment variables
from airflow import DAG  # import Airflow DAG class to define the workflow
from airflow.operators.python import PythonOperator  # import PythonOperator for running Python functions
from airflow.operators.bash import BashOperator  # import BashOperator if needed later
from datetime import datetime, timedelta  # import date utilities for DAG scheduling
import requests  # import requests to call web APIs
import pandas as pd  # import pandas for DataFrame operations
from sqlalchemy import create_engine  # import SQLAlchemy engine builder for database writes
import logging  # import logging to log warnings or errors
import time  # import time to use sleep delays

# Put your logic here as a function
# This function will be executed by the Airflow PythonOperator
def run_aviation_etl():
    # Read API keys and database secrets from environment variables.
    # This keeps sensitive values out of the source code.
    weather_api_key = os.getenv("WEATHER_API_KEY")  # OpenWeatherMap API key
    if not weather_api_key:  # if the weather key is missing, stop immediately
        raise ValueError("Missing WEATHER_API_KEY environment variable")

    db_user = os.getenv("DB_USER")  # database user name
    db_password = os.getenv("DB_PASSWORD")  # database password
    db_name = os.getenv("DB_NAME", "uk_aviation_db")  # database name with default
    db_host = os.getenv("DB_HOST", "db")  # database host name inside Docker
    db_port = os.getenv("DB_PORT", "5432")  # database port with default
    if not db_user or not db_password:  # ensure database credentials are provided
        raise ValueError("Missing DB_USER or DB_PASSWORD environment variable")

    # OpenSky does not require API credentials for the public states endpoint.
    uk_params = {"lamin": 49.9, "lamax": 59.4, "lomin": -8.6, "lomax": 1.7}  # UK bounds for the OpenSky request

    # Extract data from OpenSky
    url_opensky = "https://opensky-network.org/api/states/all"  # OpenSky endpoint URL
    response = requests.get(url_opensky, params=uk_params, timeout=10)  # call OpenSky API with bounding box
    response.raise_for_status()  # raise an error if API response is not successful
    raw_data = response.json().get('states', [])  # parse the states list from the JSON response
    
    if not raw_data:  # if no flights were returned, stop the function
        return "No flights found"

    columns = [  # define the expected columns in OpenSky state data
        'icao24', 'callsign', 'origin_country', 'time_position', 
        'last_contact', 'longitude', 'latitude', 'baro_altitude', 
        'on_ground', 'velocity', 'true_track', 'vertical_rate', 
        'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
    ]
    df = pd.DataFrame(raw_data, columns=columns)  # build a DataFrame from raw data
    df = df[['callsign', 'origin_country', 'longitude', 'latitude']]  # select only needed columns
    df['callsign'] = df['callsign'].str.strip()  # remove extra spaces from callsign values

    # Fetch weather data for each flight and add temperature and description.
    df['temperature'] = None  # initialize temperature column
    df['weather_desc'] = None  # initialize weather description column
    
    with requests.Session() as session:  # reuse HTTP session for all weather requests
        for index, row in df.iterrows():  # iterate over each flight row
            try:
                w_url = f"https://api.openweathermap.org/data/2.5/weather?lat={row['latitude']}&lon={row['longitude']}&appid={weather_api_key}&units=metric"  # build weather API URL
                w_res = session.get(w_url, timeout=5)  # call OpenWeatherMap API
                if w_res.status_code == 200:  # if the request succeeded
                    w_data = w_res.json()  # parse weather JSON
                    df.at[index, 'temperature'] = w_data['main']['temp']  # save temperature
                    df.at[index, 'weather_desc'] = w_data['weather'][0]['description']  # save weather description
                time.sleep(0.1)  # small delay to avoid API rate limit
            except Exception as e:  # catch any request or parsing errors
                logging.warning(f"Error fetching weather for callsign {row['callsign']}: {e}")  # log the warning
                continue  # skip this row and continue looping

    # Clean the data and keep only rows with fetched temperature values.
    final_df = df.dropna(subset=['temperature']).copy()  # drop rows without temperature data
    
    # Add ingest timestamp so we know when each row was stored.
    final_df['ingest_time'] = pd.Timestamp.now()  # add current timestamp column
    
    # Load data to the Postgres database.
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"  # build DB connection string
    engine = create_engine(db_url)  # create SQLAlchemy engine
    final_df.to_sql('uk_flights_weather', engine, if_exists='replace', index=False)  # write to uk_flights_weather table
    final_df.to_sql('raw_uk_flights', engine, if_exists='replace', index=False)  # write to raw_uk_flights table

# DAG definition
# Define default task arguments for the Airflow DAG
default_args = {
    'owner': 'aviation_admin',  # task owner name
    'depends_on_past': False,  # do not wait for previous DAG runs
    'start_date': datetime(2024, 1, 1),  # DAG start date
    'retries': 1,  # retry once on failure
    'retry_delay': timedelta(minutes=5),  # wait 5 minutes before retry
}

with DAG('uk_aviation_pipeline',  # create a DAG named uk_aviation_pipeline
          default_args=default_args,
          schedule='@hourly',  # run every hour
          catchup=False  # do not backfill missed runs
        ) as dag:
    
    task_etl = PythonOperator(  # define a Python task
        task_id='fetch_and_store_aviation_data',  # task identifier
        python_callable=run_aviation_etl  # function to execute
    )


