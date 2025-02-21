# functions to refresh price data, fx data etc
from etl_pipeline.transformers import *
from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table
from utils.database_setup import get_temporary_owners_list, backup_database
from manage_calculations import calculate_all_portfolios_over_time
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

def refresh_calculated_tables():
    # TODO - refresh for all existing portfolios
    owners = get_temporary_owners_list()[:-1]#temporary solution
    #owners.append(None)
    portfolio_df = calculate_all_portfolios_over_time(owners)
    upload_to_table(portfolio_df, 'AGGREGATED_VALUES', action='replace')
    print(f'Calculation Tables Refresh completed')

def refresh_all(backup=True):

    print(f'Starting full refresh')
    if backup:
        backup_database()
    refresh_market()
    refresh_fx()
    refresh_calculated_tables()
    print(f'Refresh completed')