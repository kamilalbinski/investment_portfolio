from datetime import datetime

from calculations.calculations_edo import *
from calculations.calculations_main import calculate_average_purchase_price, preprocess_transactions, adjust_prices, \
    calculate_asset_daily_values

import warnings

from utils.database_setup import get_price_data, get_all_currency_asset_ids, get_current_portfolio_data

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

    # Get data from holdings and transactions view

    transactions_df = get_current_portfolio_data(table_name='TRANSACTIONS_ALL', account_owner=owner)
    market_df = get_current_portfolio_data(table_name='CURRENT_HOLDINGS_ALL', account_owner=owner)

    # Calculate average purchase price for all transactions
    average_prices_df = calculate_average_purchase_price(transactions_df)

    # Merge market holdings with their average purchase prices
    merged_df = pd.merge(market_df, average_prices_df, on=['ACCOUNT_ID', 'ASSET_ID'], how='left')

    # Ensure CURRENT_PRICE is numeric
    merged_df['CURRENT_PRICE'] = merged_df['CURRENT_PRICE'].astype(float)

    # Add calculated fields for returns
    final_df = add_calc_fields_for_returns(merged_df)

    if return_totals:
        # Summarize values for reporting
        asset_value = final_df['CURRENT_ASSET_VALUE'].sum().round(2)
        return_value_base = final_df['CURRENT_RETURN_VALUE_BASE'].sum().round(2)
        return_value = final_df['CURRENT_RETURN_VALUE'].sum().round(2)

        # Calculate return rates
        return_rate_base = ((asset_value / (asset_value - return_value_base) - 1) * 100).round(2)
        return_rate = ((asset_value / (asset_value - return_value) - 1) * 100).round(2)

        return final_df, asset_value, return_value_base, return_rate_base, return_value, return_rate
    else:
        return final_df



def calculate_portfolio_over_time(owner=None):
    # transactions_df = query_all_transactions(owner)
    transactions_df = get_current_portfolio_data(table_name='TRANSACTIONS_ALL', account_owner=owner)
    transactions_df = preprocess_transactions(transactions_df)

    asset_ids = transactions_df['ASSET_ID'].unique()
    prices_df = get_price_data('PRICES', asset_ids, transactions_df['TIMESTAMP'].min(),
                               datetime.now().strftime('%Y-%m-%d'))
    currency_ids = get_all_currency_asset_ids()['ASSET_ID'].to_list()
    currency_rates_df = get_price_data('CURRENCIES', currency_ids, transactions_df['TIMESTAMP'].min(),
                                       datetime.now().strftime('%Y-%m-%d'))

    adjusted_prices_df = adjust_prices(prices_df, currency_rates_df)

    all_daily_values = []

    for asset_id in asset_ids:
        daily_values_df = calculate_asset_daily_values(transactions_df, adjusted_prices_df, asset_id)
        all_daily_values.append(daily_values_df)

    portfolio_df = pd.concat(all_daily_values, axis=0)

    portfolio_df['AGGREGATED_VALUE'] = portfolio_df['AGGREGATED_VALUE'].round(2)

    return portfolio_df

def calculate_all_portfolios_over_time(owners):

    all_porfolios = []

    for owner in owners:
        portfolio_df = calculate_portfolio_over_time(owner=owner)
        all_porfolios.append(portfolio_df)

    all_porfolios_df = pd.concat(all_porfolios, keys=owners).reset_index(drop=True)

    return all_porfolios_df


def calculate_return_rate_per_asset(owner=None, aggregation_column='NAME'):

    df = calculate_current_values(owner=owner)

    df = df.groupby([aggregation_column])[
        ['PURCHASE_ASSET_VALUE_BASE', 'PURCHASE_ASSET_VALUE', 'CURRENT_ASSET_VALUE', 'CURRENT_RETURN_VALUE_BASE',
         'CURRENT_RETURN_VALUE', 'RETURN_RATE_BASE', 'RETURN_RATE']].sum()

    df['RETURN_RATE_BASE'] = (df['CURRENT_ASSET_VALUE'] / (df['CURRENT_ASSET_VALUE'] - df['CURRENT_RETURN_VALUE_BASE']) - 1).multiply(100).round(2)
    df['RETURN_RATE'] = (df['CURRENT_ASSET_VALUE'] / (df['CURRENT_ASSET_VALUE'] - df['CURRENT_RETURN_VALUE']) - 1).multiply(100).round(2)

    return df[['CURRENT_RETURN_VALUE', 'RETURN_RATE']]

