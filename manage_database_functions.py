# functions to refresh price data, fx data etc
from etl_pipeline.transformers import *
from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table
from utils.database_setup import get_latest_prices_from_database

import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")

def refresh_market():
    # Get prices from latest table Join with yfinance mapping
    table = 'PRICES'
    transformed_prices = transform_prices_for_refresh(table_type=table)

   #  output: prices table (ASSET_ID, DATE, PRICE) -> to append to new database
    if not transformed_prices.empty:
        upload_to_table(transformed_prices, table, action='append')

    print(f'Prices Refresh completed')

def refresh_fx():
    # Get prices from latest table Join with yfinance mapping
    table = 'CURRENCIES'
    transformed_prices = transform_prices_for_refresh(table_type=table)
    upload_to_table(transformed_prices, table, action='append')
    # update_latest_prices(table_type=table)
    print(f'FX Refresh completed')


#TODO refresh portfolio value data