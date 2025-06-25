import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
import etf_pipeline

def test_fetch_etf_list(monkeypatch):
    fake_data = [{'symbol':'TEST','name':'Test ETF','exchangeShortName':'NASDAQ'}]
    class FakeResp:
        def raise_for_status(self): pass
        def json(self): return fake_data
    monkeypatch.setattr(etf_pipeline.requests, 'get', lambda url, timeout: FakeResp())

    df = etf_pipeline.fetch_etf_list()
    assert list(df['ticker']) == ['TEST']
    assert list(df['name'])   == ['Test ETF']

def test_download_prices(monkeypatch, tmp_path):
    # Redirect default output directory
    monkeypatch.setattr(etf_pipeline, 'PRICES_DIR', str(tmp_path))

    # Mock yfinance behaviour
    class Dummy:
        def history(self, period, interval, **kwargs):
            dates = pd.date_range('2020-01-01', periods=3, freq='D')
            return pd.DataFrame({
                'Open': [1,2,3],
                'High': [1,2,3],
                'Low':  [1,2,3],
                'Close':[1,2,3],
                'Volume':[100,200,300]
            }, index=dates)
    monkeypatch.setattr(etf_pipeline.yf, 'Ticker', lambda sym: Dummy())

    # Run download
    etf_pipeline.download_prices(pd.DataFrame({'ticker': ['TEST']}))

    # Verify file and columns
    from pathlib import Path
    files = list(Path(tmp_path).glob('TEST.csv'))
    assert len(files) == 1, "CSV not created"
    df2 = pd.read_csv(files[0])
    expected = ['Date','Open','High','Low','Close','Volume']
    assert list(df2.columns) == expected
