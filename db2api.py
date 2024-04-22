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
def traffic_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT 
                  traffic_id,
                  road_id,
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
                ORDER BY traffic_id
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

