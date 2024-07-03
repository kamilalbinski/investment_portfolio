# Example TRANSACTIONS DataFrame
# Replace this with your actual DataFrame loading method
import pandas as pd

from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table

from etl_pipeline.transformers import *
from etl_pipeline.parsers_yfinance import *
from etl_pipeline.loaders import upload_to_table

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")

transformed_assets = transform_assets_for_refresh()

tickers_list = get_tickers_from_assets_df(transformed_assets)

DATE_FROM = '2020-01-01'
DATE_TO = '2024-03-15'

stock_prices = []

for ticker in tickers_list:
    if not ticker == 'SMEA.L':
        stock_data = yf.download(ticker, start=DATE_FROM, end=DATE_TO)
#        print(stock_data.head(1))
    else:
        stock_data = pd.read_csv('SMEA.L.csv', parse_dates=True, index_col='Date')
        stock_data.sort_index(ascending=True, inplace=True)
#        print(stock_data.head(1))
#index_col='Date',
    stock_data = stock_data[['Adj Close']].rename(columns={'Adj Close': 'PRICE'}).round(4)
    stock_data['DATE'] = stock_data.index
    stock_data['DATE'] = stock_data['DATE'].astype('str')
    stock_data['DATE'] = stock_data['DATE'].apply(lambda x: f'{x} 00:00:00')

    asset_id = transformed_assets.loc[transformed_assets['YFINANCE_ID'] == ticker, 'ASSET_ID'].iloc[0].copy()
    stock_data['ASSET_ID'] = asset_id

    multiplier = transformed_assets.loc[transformed_assets['YFINANCE_ID'] == ticker, 'PRICE_MULTIPLIER'].iloc[0].copy()
    stock_data['PRICE'] /= multiplier

    stock_data.reset_index(drop=True, inplace=True)
    stock_prices.append(stock_data[['ASSET_ID', 'DATE', 'PRICE']])

data = pd.concat(stock_prices, axis=0)
data['ASSET_ID'] = data['ASSET_ID'].astype('int')
data['DATE'] = data['DATE'].astype('str')
data['PRICE'] = data['PRICE'].astype('float')

upload_to_table(data, 'PRICES')

# # download from yfinance
# yfinance_data = get_prices_from_yfinance(tickers_list, rounding=4)
#
# # compare results with table
# merged_df = merge_prices_data(transformed_assets, yfinance_data)
# # upload to database
#


print(f'Refresh completed')



#upload_to_table(merged_df, 'ASSETS')

