from http import client
from sodapy import Socrata
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/elija/repos/.gitignore/.env")  # loads .env from current working directory

class Data_Ingestion:
     def __init__(self):
          self.APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
          self.SOCRATA_USER = os.getenv("SOCRATA_USER") 
          self.SOCRATA_PASSWORD = os.getenv("SOCRATA_PASSWORD")
          

     def get_data_from_socrata(self, offset=0) -> pd.DataFrame:
        try:
            client = Socrata("data.cityofnewyork.us",
                        self.APP_TOKEN,
                        username=self.SOCRATA_USER,
                        password=self.SOCRATA_PASSWORD)
            client.timeout = 30000  # 30 seconds
            results = client.get("erm2-nwe9", limit=5, offset=offset)
            df = pd.DataFrame.from_records(results)
            print(f"Fetched {len(df)} records from Socrata with offset {offset}.")
        except client.HTTPError as e:
            print(f"HTTPError: {e}. Retrying...")
        except client.RemoteDisconnected as e:
            print(f"RemoteDisconnected error: {e}. Retrying...")

        return df


