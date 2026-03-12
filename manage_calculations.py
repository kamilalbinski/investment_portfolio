from datetime import datetime

from calculations.calculations_edo import *
from calculations.calculations_main import calculate_average_purchase_price, preprocess_transactions, adjust_prices, \
    calculate_asset_daily_values

import warnings
import pandas as pd

from utils.database_setup import (
    get_price_data,
    get_all_currency_asset_ids,
    get_portfolio_holdings_values,
    get_portfolio_transactions
)

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


def calculate_current_values(portfolio_id=None, return_totals=False):
    transactions_df = get_portfolio_transactions(portfolio_id=portfolio_id)
    market_df = get_portfolio_holdings_values(portfolio_id=portfolio_id)

    if transactions_df.empty or market_df.empty:
        final_df = market_df.copy()
        for col in [
            'AVERAGE_PURCHASE_PRICE_BASE',
            'AVERAGE_PURCHASE_PRICE',
            'CURRENT_ASSET_VALUE',
            'PURCHASE_ASSET_VALUE_BASE',
            'PURCHASE_ASSET_VALUE',
            'CURRENT_RETURN_VALUE_BASE',
            'CURRENT_RETURN_VALUE',
            'RETURN_RATE_BASE',
            'RETURN_RATE',
        ]:
            if col not in final_df.columns:
                final_df[col] = 0.0
    else:
        average_prices_df = calculate_average_purchase_price(transactions_df)

        merged_df = pd.merge(market_df, average_prices_df, on=['ACCOUNT_ID', 'ASSET_ID'], how='left')
        merged_df['CURRENT_PRICE'] = merged_df['CURRENT_PRICE'].astype(float)

        final_df = add_calc_fields_for_returns(merged_df)

    if return_totals:
        asset_value = round(float(final_df['CURRENT_ASSET_VALUE'].sum()), 2)
        return_value_base = round(float(final_df['CURRENT_RETURN_VALUE_BASE'].sum()), 2)
        return_value = round(float(final_df['CURRENT_RETURN_VALUE'].sum()), 2)

        base_denominator = asset_value - return_value_base
        value_denominator = asset_value - return_value

        return_rate_base = round((asset_value / base_denominator - 1) * 100, 2) if base_denominator else 0.0
        return_rate = round((asset_value / value_denominator - 1) * 100, 2) if value_denominator else 0.0

        return final_df, asset_value, return_value_base, return_rate_base, return_value, return_rate
    else:
        return final_df


def calculate_portfolio_over_time(portfolio_id=None):
    transactions_df = get_portfolio_transactions(portfolio_id=portfolio_id)
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


def calculate_all_portfolios_over_time(portfolio_ids):
    all_portfolios = []

    for portfolio_id in portfolio_ids:
        portfolio_df = calculate_portfolio_over_time(portfolio_id=portfolio_id)
        all_portfolios.append(portfolio_df)

    all_portfolios_df = pd.concat(all_portfolios, keys=portfolio_ids).reset_index(drop=True)

    return all_portfolios_df


def calculate_return_rate_per_asset(portfolio_id=None, aggregation_column='NAME'):
    df = calculate_current_values(portfolio_id=portfolio_id)

    df = df.groupby([aggregation_column])[
        ['PURCHASE_ASSET_VALUE_BASE', 'PURCHASE_ASSET_VALUE', 'CURRENT_ASSET_VALUE', 'CURRENT_RETURN_VALUE_BASE',
         'CURRENT_RETURN_VALUE', 'RETURN_RATE_BASE', 'RETURN_RATE']].sum()

    df['RETURN_RATE_BASE'] = (
            df['CURRENT_ASSET_VALUE'] / (df['CURRENT_ASSET_VALUE'] - df['CURRENT_RETURN_VALUE_BASE']) - 1
    ).multiply(100).round(2)
    df['RETURN_RATE'] = (
            df['CURRENT_ASSET_VALUE'] / (df['CURRENT_ASSET_VALUE'] - df['CURRENT_RETURN_VALUE']) - 1
    ).multiply(100).round(2)

    return df[['CURRENT_RETURN_VALUE', 'RETURN_RATE']]
