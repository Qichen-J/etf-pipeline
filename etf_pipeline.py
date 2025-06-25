#!/usr/bin/env python3
"""
get_etf_list.py

1. Fetch all US ETF tickers and names from Financial Modeling Prep API
2. Save the ticker list to data/etf_list.csv
3. Download 1-year daily OHLCV price data for each ETF via yfinance
4. Save each ETF's price data to data/prices/{ticker}.csv
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
import logging

from sqlalchemy import create_engine, Column, String, Date, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = 'data'
PRICES_DIR = os.path.join(DATA_DIR, 'prices')
DB_PATH = os.path.join(DATA_DIR, 'prices.db')

Base = declarative_base()

class Price(Base):
    __tablename__ = 'prices'
    symbol = Column(String, primary_key=True)
    date   = Column(Date, primary_key=True)
    open   = Column(Float)
    high   = Column(Float)
    low    = Column(Float)
    close  = Column(Float)
    volume = Column(Integer)

os.makedirs(DATA_DIR, exist_ok=True)
engine = create_engine(f'sqlite:///{DB_PATH}')
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

# Replace 'YOUR_API_KEY' with your actual Financial Modeling Prep API key.
API_KEY = 'YOUR_API_KEY'
# Endpoint for listing ETFs; limit ensures full universe
FMP_URL = (
    f'https://financialmodelingprep.com/api/v3/etf/list'
    f'?apikey={API_KEY}&limit=5000'
)
# US exchange short names to include
US_EXCHANGES = {'NASDAQ', 'NYSE', 'AMEX'}

# Download settings
MAX_RETRIES   = 3
SLEEP_BETWEEN = 0.5  # seconds between each ticker request
PERIOD        = '1y'
INTERVAL      = '1d'

# Directories for output
DATA_DIR   = os.path.join(os.getcwd(), 'data')
PRICES_DIR = os.path.join(DATA_DIR, 'prices')
LIST_PATH  = os.path.join(DATA_DIR, 'etf_list.csv')


def fetch_etf_list():
    """
    Fetch ETF list from FMP, keep only US-listed ETFs.
    Returns DataFrame of columns ['ticker','name'].
    """
    resp = requests.get(FMP_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json() or []

    records = []
    for item in data:
        sym = item.get('symbol')
        name = item.get('name')
        exch = (item.get('exchangeShortName') or '').upper()
        if sym and name and exch in US_EXCHANGES:
            records.append({'ticker': sym.strip(), 'name': name.strip()})

    df = pd.DataFrame(records)
    return df.drop_duplicates(subset='ticker').reset_index(drop=True)


def download_prices(df):
    """
    Download daily OHLCV for each ticker in df and save to CSV.
    save to CSV and insert into SQLite via SQLAlchemy.
    """
    # ensure prices output directory exists
    os.makedirs(PRICES_DIR, exist_ok=True)
    session= Session()
    tickers = df['ticker'].tolist()
    for ticker in tickers:
        logger.info(f"Downloading {ticker}...")
        for attempt in range(1, MAX_RETRIES+1):
            try:
                hist = yf.Ticker(ticker).history(period=PERIOD, interval=INTERVAL, auto_adjust=False)
                if hist.empty:
                    raise ValueError("No data returned")
                out_file = os.path.join(PRICES_DIR, f"{ticker}.csv")
                hist.to_csv(out_file, index_label='Date')
                session = Session()
                for row in hist.reset_index().itertuples():
                    session.merge(Price(
                        symbol = ticker,
                        date   = row.Date.date(),
                        open   = row.Open,
                        high   = row.High,
                        low    = row.Low,
                        close  = row.Close,
                        volume = int(row.Volume)
                    ))
                session.commit()
                session.close()
                break
            except Exception as err:
                print(f"  Attempt {attempt} failed for {ticker}: {err}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  Skipping {ticker} after {MAX_RETRIES} attempts.")
        time.sleep(SLEEP_BETWEEN)


def main():
    # create data directories
    os.makedirs(DATA_DIR, exist_ok=True)

    # Step 1: fetch and save ETFs
    etf_df = fetch_etf_list()
    etf_df.to_csv(LIST_PATH, index=False)
    print(f"ETF list saved to {LIST_PATH} ({len(etf_df)} tickers)")

    # Step 2: download price data
    download_prices(etf_df)
    print("All price downloads complete.")


if __name__ == '__main__':
    main()
