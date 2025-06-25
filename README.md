# ETF Data Pipeline

![CI](https://github.com/Qichen-J/etf-pipeline/actions/workflows/ci.yml/badge.svg)

**A Python ETL pipeline for fetching ETF lists & historical prices, with tests & CI**

## Features

* **Data Ingestion**

  * Fetch ETF list via Financial Modeling Prep API
  * Download 1-year OHLCV data using `yfinance`
* **Automated Tests**

  * `pytest` mocks for API & price downloads
  * 100% local, no real HTTP calls
* **CI Pipeline**

  * GitHub Actions runs `pytest` on every push

## Usage

```powershell
# Clone repository
git clone https://github.com/Qichen-J/etf-pipeline.git
cd etf-pipeline

# Set up virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Run the pipeline
python etf_pipeline.py

# Run tests
pytest -q
```

## Project Structure

```text
.
├── etf_pipeline.py       # Main ETL script
├── requirements.txt      # Python dependencies
├── tests/                # Unit tests
│   └── test_etf_pipeline.py
└── .github/
    └── workflows/ci.yml  # GitHub Actions CI workflow
```
