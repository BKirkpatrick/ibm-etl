import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime

def extract(url, table_attributes):
    """Extract data from url"""
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    # initialise empty dataframe
    df = pd.DataFrame(columns=table_attributes_raw)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if (col[0].find('a') is not None) and ('—' not in col[2]):
                name = col[0].a.contents[0]
                gdp = col[2].contents[0]
                gdp = float("".join(gdp.split(",")))
                data_dict = {
                    "Country": name,
                    "GDP_USD_million": gdp
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform_mil2bil(df):
    """Take df with GDP in millions, transform to billions"""
    df["GDP_USD_billion"] = round(df.GDP_USD_million / 1000, 2)
    df = df[table_attributes_processed]
    return df

def load_to_csv(df, csv_path):
    """Save processed data to csv"""
    df.to_csv(csv_path)
    print(f"dataframe saved to csv @ {csv_path}")

def load_to_db(df, conn, table_name):
    """Load processed data to <table_name> in database"""
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Table {table_name} is ready")

def run_query(query, conn):
    """Query the database"""
    query_output = pd.read_sql(query, conn)
    return query_output

def log_progress(msg):
    """Log progress with <msg>"""
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {msg}\n")

url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
log_file = "etl_project_log.txt"
target_file = "Countries_by_GDP.csv"
database_name = "World_Economies.db"
table_name = "Countries_by_GDP"
table_attributes_raw = ["Country", "GDP_USD_million"]
table_attributes_processed = ["Country", "GDP_USD_billion"]

log_progress("ETL job started")
conn = sqlite3.connect(database_name)
log_progress("connected to database")

# Extract data from url
log_progress("Extract phase started")
raw_data = extract(url=url, table_attributes=table_attributes_raw)
#print(raw_data)
log_progress("Extract phase completed")

# transform million USD to billion USD
log_progress("Transform phase started")
processed_data = transform_mil2bil(raw_data)
log_progress("Transform phase completed")

# load into csv
log_progress("Load phase started")
load_to_csv(df=processed_data, csv_path=target_file)
# load into database
load_to_db(
    df=processed_data,
    conn=conn,
    table_name=table_name
)
log_progress("Load phase completed")
# run query on db
# display only entries with more than 100 B USD GDP
query = f"SELECT * FROM {table_name} WHERE GDP_USD_billion >= 100"
query_output = run_query(query, conn)
print(f"QUERY:\n{query}\n")
print(f"RESPONSE:\n{query_output}\n")

log_progress("ETL job completed")