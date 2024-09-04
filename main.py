import pandas as pd
import pyarrow.parquet as pq
import requests
from io import BytesIO
import logging
import pyarrow as pa
from bs4 import BeautifulSoup
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_parquet_urls():
    """Fetch all .parquet file URLs from the webpage."""
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        parquet_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.parquet') and 'yellow' in a['href'].lower()]
        logger.info(f"Found {len(parquet_links)} .parquet links.")
        return parquet_links
    else:
        logger.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return []
    
def read_parquet_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    
    with BytesIO(response.content) as buffer:
        # Read the parquet file into an Arrow Table
        table = pq.read_table(buffer)
        # print(table.schema.names)
        datetime_columns = [c for c in table.schema.names if 'time' in c]
        # Iterate over the columns and cast if they exist
        for col in datetime_columns:
            table = table.set_column(
                table.schema.get_field_index(col),  # column index
                col,  # column name
                table.column(col).cast(pa.string())
            )
            
        # Convert the Arrow Table to a Pandas DataFrame
        df = table.to_pandas()
    
    return df

def fetch_and_process_parquet(url, percentile=0.9):
    logger.info(f"Processing {url}")
    df = read_parquet_from_url(url)
    logger.info(f"Raw data has {len(df)} rows")

    if 'trip_distance' in df.columns:
        distance_column = 'trip_distance'
    elif 'Trip_Distance' in df.columns:
        distance_column = 'Trip_Distance'
    else:
        raise ValueError("Neither 'trip_distance' nor 'Trip_Distance' columns are found in the dataframe.")
    
    thresh = df[distance_column].quantile(0.9)
    logger.info(f"90th percentile above: {thresh}")
    df_90 = df[df[distance_column] > thresh]
    logger.info(f"Final data has {len(df_90)} rows")
    # {url:df_90.index.tolist()}
    return df_90

def process_all_parquet():
    filepath = 'data/indexes.json'
    if os.path.exists(filepath):
       with open(filepath, 'r') as f:
            results = json.load(f)
    else:
        results = {}
    
    urls = get_parquet_urls()
    
    for url in [u for u in urls if u not in results]:
        d = fetch_and_process_parquet(url)
        results |= d
        with open(filepath, 'w') as file:
            json.dump(results, file, indent=4)


def main():
    url = input("Please enter the URL for yellow taxi parquet: ")
    print(f"The URL you entered is: {url}")
    urls = get_parquet_urls()
    if url in urls:
        df = fetch_and_process_parquet(url)
    else:
        year = -1
        while int(year) not in range(2009,2025):
            year = input(f"The URL you entered is invalid, please select a year between 2009-2024: ")
        urls = [(u.split('_')[-1].split('.')[0].split('-')[-1],u) for u in urls if year in u]
        available_months = [u[0] for u in urls]
        month = -1
        while month not in available_months:
            month = input(f"Select the month, including the starting 0 if applicable: {available_months}: ")
        url = [u[1] for u in urls if u[0]==month][0]
        df = fetch_and_process_parquet(url)

    b = input(f"Would you like the results saved locally? y/n: ")
    if b.lower() == 'y':
        filepath = f"data/{year}-{month}-yellow-90.csv"
        df.to_csv(filepath)
        print(f"Saved to: {filepath}")
        
    

if __name__ == "__main__":
    main()