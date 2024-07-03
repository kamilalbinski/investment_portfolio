# functions to refresh price data, fx data etc
from etl_pipeline.transformers import *
from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table
from utils.database_setup import get_latest_prices_from_database

import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")


def update_latest_prices(table_type='PRICES'):
    # get latest data from PRICES database
    df = get_latest_prices_from_database(table=table_type)
    # load data to a table containing latest records only
    upload_to_table(df, f'LATEST_{table_type}', action='replace')

def refresh_market():
    # Get prices from latest table Join with yfinance mapping
    table = 'PRICES'
    transformed_prices = transform_prices_for_refresh(table_type=table)

    # output: prices table (ASSET_ID, DATE, PRICE) -> to append to new database
    upload_to_table(transformed_prices, table, action='append')
    update_latest_prices(table_type=table)
    print(f'Refresh completed')


def refresh_fx():
    # Get prices from latest table Join with yfinance mapping
    table = 'CURRENCIES'
    transformed_prices = transform_prices_for_refresh(table_type=table)
    upload_to_table(transformed_prices, table, action='append')
    update_latest_prices(table_type=table)
    print(f'Refresh completed')
