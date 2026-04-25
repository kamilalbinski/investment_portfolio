# 📊 Investment Portfolio Manager

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![SQLite](https://img.shields.io/badge/database-SQLite-lightgrey.svg)](https://sqlite.org/index.html)

##  Overview

**Investment Portfolio Manager** is a desktop application and backend pipeline for tracking and analyzing personal investments. It supports automated data import from brokers/banks, return calculation, time series aggregation, and export-ready reporting — all managed through a GUI or programmable CLI.

---

##  Features

- ✅ GUI built with `Tkinter`
- ✅ Parses files from **mBank**, **PKO Treasury Bonds**, and others
- ✅ Loads into a robust **SQLite** database
- ✅ ETL pipeline for parsing, transforming, and loading holdings and transactions
- ✅ Calculates portfolio value over time and per-asset return rates
- ✅ Updates market data from:
  - YFinance
  - Biznesradar (Polish bond prices data)
  - Polish Retail inflation-rating bonds (EDO)
- ✅ Visualizations
- ✅ Exports reports in CSV and PDF formats

---

##  Project Structure

```text
investment_portfolio/
|-- .gitignore
|-- README.md
|-- main.py                          # GUI application entry point
|-- manage_calculations.py           # Portfolio calculation helpers / CLI entry
|-- manage_database_functions.py     # Database refresh functions
|-- manage_pipeline_functions.py     # ETL orchestration helpers
|-- requirements.txt
|-- calculations/
|   |-- calculations_edo.py          # Bond-specific return logic
|   `-- calculations_main.py         # General return and valuation logic
|-- ddl/
|   |-- tables/                      # SQL table definitions
|   `-- view/                        # SQL view definitions
|-- etl_pipeline/
|   |-- etl_utils.py                 # Shared ETL utilities
|   |-- loaders.py                   # Database loading logic
|   |-- parsers_files.py             # Broker and file parsers
|   |-- parsers_webpages.py          # Web scraping parsers
|   |-- parsers_yfinance.py          # YFinance market data access
|   `-- transformers.py              # Data cleaning and transformation
|-- gui/
|   `-- portfolio_management_gui.py  # Main CustomTkinter GUI
|-- utils/
|   |-- config.py                    # Paths, runtime flags, local config
|   |-- database_setup.py            # DB setup and query utilities
|   `-- misc_func.py                 # Miscellaneous helper functions
|-- views/
|   |-- custom_views.py              # DataFrame-based table and view builders
|   `-- pivot.csv                    # Example / generated pivot output
`-- visualization/
    |-- dashboards.py                # Dashboard-oriented visual helpers
    `-- dynamic_plots.py             # Matplotlib plots used by the GUI
```

---

##  Installation

```bash
git clone https://github.com/kamilalbinski/investment_portfolio.git
cd investment_portfolio
pip install -r requirements.txt
```
After installing dependencies, initialize the database:
```python
from utils.database_setup import setup_database
setup_database("portfolio.db")
```
---

##  Configuration

Edit `utils/config.py` to point to correct folders and DB:

```python
MBANK_FOLDER = "data/mbank"
PKOTB_FOLDER = "data/pkotb"
DATABASE_FILE = "portfolio.db"
```

---

## Quick Start

### GUI App

```bash
python main.py
```

### Portfolio Analysis (CLI)

```bash
python manage_calculations.py
```

---

## Screenshots

> *TBD*

---

## Core Calculations

- Current vs purchase value
- Average purchase price
- Return rate per asset and per portfolio
- Time series of portfolio value
- Currency-adjusted asset returns

---

## Reports

Export includes:

- Portfolio breakdown (table)
- Allocation pie chart
- Value-over-time chart
- Return summary

Formats: **CSV**, **PDF**

---

## Extensibility

- Easily add support for new brokers via custom parsers
- ETL structure is modular (Parser → Transformer → Loader)
- SQLite-backed, schema-controlled
- Automatically archives processed files

---

## Dependencies

- `pandas`,`numpy`,`sqlite3`
- `yfinance`,`openpyxl`, `requests`
- `customtkinter`, `matplotlib`,`python-dotenv`
- `python-dateutil`, `pdfplumber`, `beautifulsoup4`

Install with:

```bash
pip install -r requirements.txt
```

---

## 📄 License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
