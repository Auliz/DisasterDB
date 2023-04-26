from fastapi import FastAPI
import pandas as pd

# ---BEGIN CSV's ---
# df = pd.read_csv('data/us_disaster_declarations.csv')
df = pd.read_csv('data/us_disasters_m5.csv')

# ---BEGIN FAST API---
app = FastAPI()

@app.get("/")
def root():
  dummy_data = {'word': 'Hello World'}
  return dummy_data

@app.get("/data")
def data():
  return df.T.to_json()