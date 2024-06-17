# functions to refresh price data, fx data etc
from etl_pipeline.transformers import *
from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table
from utils.database_setup import get_latest_prices_from_database

import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")


def update_latest_prices(table_type='Prices'):
    # get latest data from Prices database
    df = get_latest_prices_from_database(table=table_type)
    # load data to a table containing latest records only
    upload_to_table(df, f'Latest_{table_type}', action='replace')

# def refresh_market():
#     print('### Refreshing market prices ###')
#     # get data to refresh from database
#     transformed_assets = transform_assets_for_refresh()
#
#     # (with joins) -> return list
#     tickers_list = get_tickers_from_assets_df(transformed_assets, tickers_only=True).to_list()
#
#     # download from yfinance
#     yfinance_data = get_prices_from_yfinance(tickers_list, rounding=4)
#
#     # compare results with table
#     merged_df = merge_prices_data(transformed_assets, yfinance_data)
#     # upload to database
#
#     upload_to_table(merged_df, 'Assets')
#
#     print(f'Refresh completed')


def refresh_market():
    # Get prices from latest table Join with yfinance mapping
    table = 'Prices'
    transformed_prices = transform_prices_for_refresh(table_type=table)

    # output: prices table (ASSET_ID, DATE, PRICE) -> to append to new database
    upload_to_table(transformed_prices, table, action='append')
    update_latest_prices(table_type=table)
    print(f'Refresh completed')


def refresh_fx():
    # Get prices from latest table Join with yfinance mapping
    table = 'Currencies'
    transformed_prices = transform_prices_for_refresh(table_type=table)
    upload_to_table(transformed_prices, table, action='append')
    update_latest_prices(table_type=table)
    print(f'Refresh completed')


####refresh CPI()

# refresh_market()
# refresh_FX()
