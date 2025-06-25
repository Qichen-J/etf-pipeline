# analysis.py

from sqlalchemy import create_engine
import pandas as pd

# Path to the SQLite database created by etf_pipeline.py
DB_PATH = 'data/prices.db'

def top10_gainers():
    """
    Query top-10 ETFs by yearly gain percentage.
    Gain = (max(close) - min(close)) / min(close) * 100
    """
    engine = create_engine(f"sqlite:///{DB_PATH}")
    query = """
    SELECT
      symbol,
      ROUND((MAX(close) - MIN(close)) * 100.0 / MIN(close), 2) AS gain_pct
    FROM prices
    GROUP BY symbol
    ORDER BY gain_pct DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    print("Top 10 ETF Gainers (%) in Past Year:")
    print(df.to_string(index=False))

if __name__ == '__main__':
    top10_gainers()
