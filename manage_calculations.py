from datetime import datetime

from calculations.calculations_edo import *
from calculations.calculations_edo import calculate_bulk_edo_values
from calculations.calculations_main import calculate_average_purchase_price, preprocess_transactions, adjust_prices, \
    calculate_asset_daily_values

import warnings

from utils.database_setup import get_price_data, get_all_currency_asset_ids, query_all_transactions, query_all_holdings

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")


def add_calc_fields_for_returns(df):
    df['CURRENT_ASSET_VALUE'] = (df['VOLUME'] * df['CURRENT_PRICE'] * df['FX_RATE']).round(4)

    df['PURCHASE_ASSET_VALUE_BASE'] = (df['VOLUME'] * df['AVERAGE_PURCHASE_PRICE_BASE'] * df['FX_RATE']).round(4)

    df['PURCHASE_ASSET_VALUE'] = (df['VOLUME'] * df['AVERAGE_PURCHASE_PRICE']).round(4)

    df['CURRENT_RETURN_VALUE_BASE'] = (df['CURRENT_ASSET_VALUE'] - df['PURCHASE_ASSET_VALUE_BASE']).round(4)

    df['CURRENT_RETURN_VALUE'] = (df['CURRENT_ASSET_VALUE'] - df['PURCHASE_ASSET_VALUE']).round(4)

    df['RETURN_RATE_BASE'] = (df['CURRENT_ASSET_VALUE'] / df['PURCHASE_ASSET_VALUE_BASE'] - 1).round(4)

    df['RETURN_RATE'] = (df['CURRENT_ASSET_VALUE'] / df['PURCHASE_ASSET_VALUE'] - 1).round(4)

    return df


def calculate_current_values(owner=None, return_totals=False):
    transactions_df = query_all_transactions(owner)
    market_df = query_all_holdings(owner, listed=True)
    edo_df = query_all_holdings(owner, listed=False)

    average_prices_df = calculate_average_purchase_price(transactions_df)

    # Merge market holdings with their average purchase prices
    merged_df = pd.merge(market_df, average_prices_df, on=['ACCOUNT_ID', 'ASSET_ID'], how='left')

    # Calculate current values for EDO assets and merge them into the main DataFrame
    if not edo_df.empty:
        # Use the modified function to handle EDO values
        edo_values_df = calculate_bulk_edo_values(edo_df, mode='current')
        edo_values_df = edo_values_df.tail(1).drop(columns=['TIMESTAMP']).melt(var_name='ASSET_ID',
                                                                               value_name='CURRENT_PRICE')

        # Merge EDO current values into edo_df based on 'NAME'
        edo_df = pd.merge(edo_df, edo_values_df, on='ASSET_ID', how='left')

        # Assuming calculate_bulk_edo_values function adjusts EDO DataFrame appropriately
        # and 'CURRENT_PRICE' field is already calculated and filled in edo_values_df
        edo_df['CURRENT_PRICE_x'] = edo_df['CURRENT_PRICE_y']
        edo_df.drop(columns='CURRENT_PRICE_y',
                    inplace=True)  # .rename(columns={'CURRENT_PRICE_x': 'CURRENT_PRICE'},inplace=True)
        edo_df.rename(columns={'CURRENT_PRICE_x': 'CURRENT_PRICE'}, inplace=True)
        edo_df['AVERAGE_PURCHASE_PRICE_BASE'] = 100  # Placeholder, adjust as necessary
        edo_df['AVERAGE_PURCHASE_PRICE'] = 100  # Placeholder, adjust as necessary

        # Concatenate EDO data with market data
        merged_df = pd.concat([merged_df, edo_df])

    # Add calculated fields for returns

    merged_df['CURRENT_PRICE'] = merged_df['CURRENT_PRICE'].astype('float64')

    final_df = add_calc_fields_for_returns(merged_df)

    if return_totals:

        # Summarize values for reporting
        return_value_base = final_df['CURRENT_RETURN_VALUE_BASE'].sum().round(2)
        return_value = final_df['CURRENT_RETURN_VALUE'].sum().round(2)
        asset_value = final_df['CURRENT_ASSET_VALUE'].sum().round(2)

        # Calculate return rates
        return_rate_base = ((asset_value / (asset_value - return_value_base) - 1) * 100).round(2)
        return_rate = ((asset_value / (asset_value - return_value) - 1) * 100).round(2)
        # print(f'Total asset value for {owner}: {asset_value:,} PLN')
        # print(f'Total investment return for {owner}: {return_value_base:,} PLN ({return_rate_base:,} %)')
        # print(f'Total investment return (including FX) for {owner}: {return_value:,} PLN ({return_rate:,} %)')
        return final_df, asset_value, return_value_base, return_rate_base, return_value, return_rate
    else:
        return final_df


def calculate_portfolio_over_time(owner=None):
    transactions_df = query_all_transactions(owner)
    transactions_df = preprocess_transactions(transactions_df)

    asset_ids = transactions_df['ASSET_ID'].unique()
    prices_df = get_price_data('PRICES', asset_ids, transactions_df['TIMESTAMP'].min(),
                               datetime.now().strftime('%Y-%m-%d'))
    currency_ids = get_all_currency_asset_ids()['ASSET_ID'].to_list()
    currency_rates_df = get_price_data('CURRENCIES', currency_ids, transactions_df['TIMESTAMP'].min(),
                                       datetime.now().strftime('%Y-%m-%d'))

    adjusted_prices_df = adjust_prices(prices_df, currency_rates_df)

    portfolio_df = pd.DataFrame()
    for asset_id in asset_ids:
        daily_values_df = calculate_asset_daily_values(transactions_df, adjusted_prices_df, asset_id)
        portfolio_df = pd.merge(portfolio_df, daily_values_df, on='TIMESTAMP',
                                how='outer') if not portfolio_df.empty else daily_values_df

    # Handling EDO data
    edo_data = query_all_holdings(owner, listed=False)
    if not edo_data.empty:
        edos_df = calculate_bulk_edo_values(edo_data, mode='daily')
        edos_df['TIMESTAMP'] = pd.to_datetime(edos_df['TIMESTAMP'])
        portfolio_df = pd.merge(portfolio_df, edos_df, on='TIMESTAMP', how='outer').fillna(0)

    portfolio_df.fillna(0, inplace=True)
    portfolio_df['Total Portfolio Value'] = portfolio_df.drop('TIMESTAMP', axis=1).sum(axis=1)
    portfolio_df['TIMESTAMP'] = pd.to_datetime(portfolio_df['TIMESTAMP']).dt.date

    return portfolio_df, transactions_df
