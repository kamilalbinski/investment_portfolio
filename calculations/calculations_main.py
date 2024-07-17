from collections import deque
from datetime import datetime

import pandas as pd
import numpy as np


def calculate_average_purchase_price(df):
    # Reset the index to ensure 'ACCOUNT_ID' is treated as a 1-dimensional column
    df = df.reset_index(drop=True)

    # Check and remove any duplicate columns named 'ACCOUNT_ID'
    df = df.loc[:, ~df.columns.duplicated()]

    # Initialize the column for tracking volume after FIFO
    df['PURCHASE_VOLUME'] = df['VOLUME']  # Start with the original volume

    # Group the DataFrame by 'ACCOUNT_ID' and 'ASSET_ID' and apply FIFO within each group
    for _, group in df.groupby(['ACCOUNT_ID', 'ASSET_ID'], as_index=False):
        purchases = deque()

        for index, row in group.iterrows():
            if row['BUY_SELL'] == 'B':
                # Add the purchase to the queue (volume and total price)
                purchases.append((index, row['VOLUME']))
            elif row['BUY_SELL'] == 'S':
                # Initialize the sell volume
                sell_volume = row['VOLUME']

                # FIFO calculation for sell
                while sell_volume > 0 and purchases:
                    buy_index, buy_volume = purchases[0]
                    if buy_volume > sell_volume:
                        # Reduce the buy volume and update the queue
                        new_buy_volume = buy_volume - sell_volume
                        purchases[0] = (buy_index, new_buy_volume)
                        df.at[buy_index, 'PURCHASE_VOLUME'] = new_buy_volume
                        sell_volume = 0
                    else:
                        # Remove the purchase from the queue and reduce the sell volume
                        sell_volume -= buy_volume
                        df.at[buy_index, 'PURCHASE_VOLUME'] = 0
                        purchases.popleft()

    # Ensure that VOLUME_FOR_FIFO for S transactions is set to 0
    df.loc[df['BUY_SELL'] == 'S', 'PURCHASE_VOLUME'] = 0
    df['PURCHASE_VALUE_BASE'] = (df['PRICE'] + df['TRANSACTION_FEE'] / df['VOLUME']) * df['PURCHASE_VOLUME']
    df['PURCHASE_VALUE'] = (df['PURCHASE_VALUE_BASE'] * df['FX_RATE']).round(4)

    # grouped_df = df.groupby(['ACCOUNT_ID', 'ASSET_ID'])[['PURCHASE_VALUE', 'PURCHASE_VOLUME']].sum()
    #
    # grouped_df['AVERAGE_PRICE'] = grouped_df['PURCHASE_VALUE'] / ['PURCHASE_VOLUME']
    grouped = df.groupby(['ACCOUNT_ID', 'ASSET_ID'])
    grouped_df = grouped.apply(
        lambda g: pd.Series({
            'TOTAL_PURCHASE_VALUE_BASE': g['PURCHASE_VALUE_BASE'].sum(),
            'TOTAL_PURCHASE_VALUE': g['PURCHASE_VALUE'].sum(),
            'TOTAL_PURCHASE_VOLUME': g['PURCHASE_VOLUME'].sum()
        })
    ).reset_index()

    # Calculate the average purchase price for each group
    grouped_df['AVERAGE_PURCHASE_PRICE_BASE'] = (
            grouped_df['TOTAL_PURCHASE_VALUE_BASE'] / grouped_df['TOTAL_PURCHASE_VOLUME']).round(4)
    grouped_df['AVERAGE_PURCHASE_PRICE'] = (
            grouped_df['TOTAL_PURCHASE_VALUE'] / grouped_df['TOTAL_PURCHASE_VOLUME']).round(4)
    grouped_df = grouped_df[['ACCOUNT_ID', 'ASSET_ID', 'AVERAGE_PURCHASE_PRICE_BASE',
                             'AVERAGE_PURCHASE_PRICE']].dropna(axis=0, how='any')

    return grouped_df


def fix_price_no_fx_case(merged_price_fx_df):
    """handles scenario where a certain asset is priced for a given date, but its relevant fx not by backward fill"""

    df = merged_price_fx_df

    columns_to_backfill = ['PRICE_FX', 'ASSET_ID_FX', 'NAME_FX']

    # Apply the transformation to return previous FX rate details
    condition = (pd.notnull(df['PRICE'])) & (pd.isnull(df['PRICE_FX'])) & (df['ASSET_ID'] == df['ASSET_ID'].shift(1))

    for column in columns_to_backfill:
        df[column] = np.where(condition, df[column].shift(1), df[column])

    return df


def adjust_prices_for_currency(prices_df, currency_rates_df):
    # Merge prices with currency rates on ASSET_ID and DATE
    merged_price_fx_df = prices_df.merge(currency_rates_df, on=['CURRENCY', 'DATE'], how='left', suffixes=('', '_FX'))

    adjusted_prices_df = fix_price_no_fx_case(merged_price_fx_df)

    # Adjust prices for currency exchange rates
    adjusted_prices_df['CONVERTED_PRICE'] = adjusted_prices_df.apply(
        lambda x: (x['PRICE'] * x['PRICE_FX']) if pd.notnull(x['PRICE_FX']) else x['PRICE'],
        axis=1
    )
    adjusted_prices_df.rename(columns={'DATE': 'TIMESTAMP'}, inplace=True)

    return adjusted_prices_df


def preprocess_transactions(transactions_df):
    """
    Preprocess transactions DataFrame to include necessary columns and formats.
    """
    transactions_df['TIMESTAMP'] = pd.to_datetime(transactions_df['TIMESTAMP']).dt.date
    transactions_df['EFFECTIVE_VOLUME'] = transactions_df['VOLUME'].where(transactions_df['BUY_SELL'] == 'B',
                                                                          -transactions_df['VOLUME'])
    transactions_df.sort_values(by=['ASSET_ID', 'TIMESTAMP'], inplace=True)
    return transactions_df


def adjust_prices(prices_df, currency_rates_df):
    """
    Adjust prices for currency and multiplier. Placeholder for actual implementation.
    """
    adjusted_prices_df = adjust_prices_for_currency(prices_df, currency_rates_df)
    adjusted_prices_df['TIMESTAMP'] = pd.to_datetime(adjusted_prices_df['TIMESTAMP'])
    return adjusted_prices_df


def calculate_asset_daily_values(transactions_df, adjusted_prices_df, asset_id):
    """
    Calculate daily values for a given asset.
    """
    daily_data = pd.DataFrame(
        {'TIMESTAMP': pd.date_range(start=transactions_df['TIMESTAMP'].min(), end=datetime.now().strftime('%Y-%m-%d'))})
    transactions_subset = transactions_df[transactions_df['ASSET_ID'] == asset_id].copy()
    transactions_subset['CUMULATIVE_VOLUME'] = transactions_subset.groupby('ASSET_ID')['EFFECTIVE_VOLUME'].cumsum()
    transactions_subset['TIMESTAMP'] = pd.to_datetime(transactions_subset['TIMESTAMP'])

    daily_data = pd.merge(daily_data, adjusted_prices_df[adjusted_prices_df['ASSET_ID'] == asset_id],
                          on='TIMESTAMP', how='left')
    daily_data = pd.merge(daily_data, transactions_subset, on='TIMESTAMP', how='left')

    asset_name = daily_data['NAME'].dropna().unique()[0]

    daily_data = daily_data[['TIMESTAMP', 'CONVERTED_PRICE', 'CUMULATIVE_VOLUME']].ffill().fillna(0)

    daily_data[asset_name] = daily_data['CONVERTED_PRICE'] * daily_data['CUMULATIVE_VOLUME']

    return daily_data[['TIMESTAMP', asset_name]]
