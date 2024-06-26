from fastapi import FastAPI
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL is None:
    raise ValueError("The DATABASE_URL environment variable MUST be set!")
if not DATABASE_URL.startswith("postgresql"):
   DATABASE_URL = f"postgresql://{DATABASE_URL}"

app = FastAPI()
eng = create_engine(DATABASE_URL)

def create_simple_endpoint(endpoint, query):
   """Function to manufacture simple endpoints for those without much
   Python experience.
   """
   @app.get(endpoint)
   def auto_simple_endpoint():
      f"""Automatic endpoint function for {endpoint}"""
      with eng.connect() as con:
         res = con.execute(query)
         return [r._asdict() for r in res]
            
with open("endpoints.yaml") as f:
   endpoints = yaml.safe_load(f)
   for endpoint, query in endpoints.items():
      create_simple_endpoint(endpoint, query)





#------------------------------------------------
# Custom Endpoints
#------------------------------------------------



@app.get("/traffic/{page}")
def traffic_by_page(page, city:str=None):
     with eng.connect() as con:
        query = """
                SELECT 
                  traffic_id,
                  traffic.road_id,
                  city,
                  current_speed,
                  free_flow_speed,
                  round(current_speed::NUMERIC/free_flow_speed*100,2) AS speed_pct_of_capacity,
                  current_travel_time,
                  free_flow_travel_time,
                  current_travel_time-free_flow_travel_time AS additional_travel_time_due_to_traffic,
                  time_added_pst AS time_pst
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                JOIN roads
                ON traffic.road_id = roads.road_id
                ORDER BY traffic_id
                LIMIT 50
                OFFSET :off
                """
        if city is not None:
            query = """
                SELECT 
                  traffic_id,
                  traffic.road_id,
                  city,
                  current_speed,
                  free_flow_speed,
                  round(current_speed::NUMERIC/free_flow_speed*100,2) AS speed_pct_of_capacity,
                  current_travel_time,
                  free_flow_travel_time,
                  current_travel_time-free_flow_travel_time AS additional_travel_time_due_to_traffic,
                  time_added_pst AS time_pst
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                JOIN roads
                ON traffic.road_id = roads.road_id
                WHERE city = :ct
                ORDER BY traffic_id
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'ct': city})
        return [r._asdict() for r in res]



@app.get("/avghourtraffic/{page}")
def avg_hour_traffic_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT 
                  hour, 
                  round(avg(current_speed),3) AS average_speed,
                  round(avg(free_flow_speed),3) AS average_free_flow_speed,
                  round(avg(current_speed/free_flow_speed)*100,3) AS pct_free_flow_capacity
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                GROUP BY hour
                ORDER BY hour
                LIMIT 50
                OFFSET :off
                """
        if hour is not None:
            query = """
                SELECT 
                  hour, 
                  round(avg(current_speed),3) AS average_speed,
                  round(avg(free_flow_speed),3) AS average_free_flow_speed,
                  round(avg(current_speed/free_flow_speed)*100,3) AS pct_free_flow_capacity
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                WHERE hour = :hr
                GROUP BY hour
                ORDER BY hour
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'hr': hour})
        return [r._asdict() for r in res]


@app.get("/avgdaytraffic/{page}")
def avg_day_traffic_by_page(page, day:str=None):
     with eng.connect() as con:
        query = """
                SELECT 
                  day_name, 
                  round(avg(current_speed),3) AS average_speed,
                  round(avg(free_flow_speed),3) AS average_free_flow_speed,
                  round(avg(current_speed/free_flow_speed)*100,3) AS pct_free_flow_capacity
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                GROUP BY day_name
                ORDER BY pct_free_flow_capacity
                LIMIT 50
                OFFSET :off
                """
        if day is not None:
            query = """
                SELECT 
                  day_name, 
                  round(avg(current_speed),3) AS average_speed,
                  round(avg(free_flow_speed),3) AS average_free_flow_speed,
                  round(avg(current_speed/free_flow_speed)*100,3) AS pct_free_flow_capacity
                FROM traffic
                JOIN datetimes
                ON traffic.time_id = datetimes.time_id
                WHERE day_name = :dy
                GROUP BY day_name
                ORDER BY pct_free_flow_capacity
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'dy': day})
        return [r._asdict() for r in res]


@app.get("/trafficbyweather/{page}")
def traffic_weather_by_page(page, weather:str=None):
     with eng.connect() as con:
        query = """
                SELECT 
                    conditions,
                    date_weather,
                    road_id1 AS road_id,
                    current_speed,
                    free_flow_speed,
                    current_speed::NUMERIC/free_flow_speed*100 AS pct_speed_of_capacity,
                    cloud_cover,
                    temperature
                FROM traffic_weather
                WHERE date_weather IS NOT NULL
                  AND city='Portland'
                LIMIT 50
                OFFSET :off
                """
        if weather is not None:
            query = """
                SELECT 
                    conditions,
                    date_weather,
                    road_id1 AS road_id,
                    current_speed,
                    free_flow_speed,
                    current_speed::NUMERIC/free_flow_speed*100 AS pct_speed_of_capacity,
                    cloud_cover,
                    temperature
                FROM traffic_weather
                WHERE date_weather IS NOT NULL
                  AND city='Portland'
                  AND conditions ~ :wh
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'wh': weather})
        return [r._asdict() for r in res]

@app.get("/weatherstuff/{page}")
def weather_stuff_by_page(page):
     with eng.connect() as con:
        query = """
                SELECT *
                FROM remote2.weather_backup_data
                ORDER BY id
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page)})
        return [r._asdict() for r in res]

@app.get("/mtbh/{page}")
def mtbh_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT MAX(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM remote2.weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        if hour is not None:
            query = """
                SELECT MAX(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM remote2.weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                AND DATE_PART = :hr
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'hr': hour})
        return [r._asdict() for r in res]


@app.get("/mintbh/{page}")
def mintbh_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT MIN(temperature) AS min_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM remote2.weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        if hour is not None:
            query = """
                SELECT MIN(temperature) AS min_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM remote2.weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                AND DATE_PART = :hr
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'hr': hour})
        return [r._asdict() for r in res]



