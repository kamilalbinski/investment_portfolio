import pandas
import pandas as pd
import numpy as np

from utils.database_setup import fetch_data_from_database
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import DateOffset
from pandas import Timestamp


def calculate_from_date(date):
    # Subtract 2 months
    date_backwards = date - relativedelta(months=2)
    # Return the first day of that month
    return date_backwards.replace(day=1)


def days_to_new_year(current_date):
    """
    Returns the number of days from the current_date to the end of the year.

    Args:
    - current_date (str or datetime-like): The current date.

    Returns:
    - int: Number of days to the end of the year.
    """
    current_date = pd.to_datetime(current_date)
    year_end = Timestamp(year=current_date.year, month=12, day=31)
    return (year_end - current_date).days + 1


def get_edo_details(asset_id):  # get_edo_general_details(asset_id, volume?):

    asset_id = asset_id
    table = 'ASSETS'
    query = f'SELECT * FROM {table} WHERE ASSET_ID = {asset_id}'
    edo_df = fetch_data_from_database(table, query)

    edo_name = edo_df['NAME'].max()
    table = 'TB_PL'
    query = f'SELECT * FROM {table} WHERE "NAME" = "{edo_name}"'

    edo_df = fetch_data_from_database(table, query).merge(edo_df[['NAME','CURRENT_PRICE', 'INITIAL_DATE']], on='NAME', how='inner')
    # edo_df = fetch_data_from_database(table, query).merge(edo_df[['NAME', 'CURRENT_PRICE',
    #                                                               'PRICE_DATE']], on='NAME', how='inner')


    edo_df['INITIAL_DATE'] = pd.to_datetime(edo_df['INITIAL_DATE']).dt.tz_localize(None)

    return edo_df

    # from name get interest + margin


def get_cpi_for_period(first_date, to_date):
    # Load data from the database
    cpi_df = fetch_data_from_database('CPI_PL')

    # Convert date columns to datetime and ensure they are timezone-naive for consistent comparison
    cpi_df['DATE'] = pd.to_datetime(cpi_df['DATE']).dt.tz_localize(None)

    # Offset the first date: 1 year forward, 2 months back, and set to the 1st day of the month
    from_date = (first_date + DateOffset(years=1, months=-2)).replace(day=1)

    # Get today's date, normalized to midnight, and ensure it is timezone-naive

    # Filter the DataFrame for dates within the specified range
    filtered_df = cpi_df[(cpi_df['DATE'] >= from_date) & (cpi_df['DATE'] <= to_date)]

    # Further filter to include only rows where the month matches the FROM date's month
    final_df = filtered_df[filtered_df['DATE'].dt.month == from_date.month].copy()

    # Replace CPI dates accordingly to new capitalisation year
    final_df['DATE'] = final_df['DATE'].apply(lambda x: x.replace(month=first_date.month, day=first_date.day))

    return final_df


def calculate_edo_aggregated_value(from_date, to_date, edo_df, cpi_df):
    """
    Calculates the current value of a treasury bond over a specified period using CPI adjustments.

    :param from_date: The start date for the calculation period.
    :param to_date: The end date for the calculation period.
    :param edo_df: A DataFrame containing the treasury bond data, including initial rate, margin, and current price.
    :param cpi_df: A DataFrame containing CPI data with dates and CPI values.
    :param return_table: A flag to determine if the function should return a detailed table of calculations or just the final value.
    :return: The current value of the treasury bond at the end of the period, or a detailed DataFrame if return_table is True.
    """

    # Adjust from_date to start from the next day
    from_date += DateOffset(days=1)

    # Generate a date range for the specified period
    date_range = pd.date_range(start=from_date, end=to_date)

    # Create a DataFrame from the date range
    date_df = pd.DataFrame(date_range, columns=['DATE'])

    # Ensure CPI DataFrame dates are in datetime format and adjusted to start from the next day
    cpi_df['DATE'] = pd.to_datetime(cpi_df['DATE']) + DateOffset(days=1)

    # Merge the generated date DataFrame with the CPI DataFrame
    merged_df = pd.merge(date_df, cpi_df, how='left', on='DATE')

    # Calculate the date one year from the from_date
    first_year_date = from_date + DateOffset(years=1)

    # Retrieve bond data from edo_df
    initial_rate = edo_df['INITIAL_RATE'].iloc[0]
    margin = edo_df['MARGIN'].iloc[0]
    current_value = edo_df['CURRENT_PRICE'].iloc[0]

    # Adjust CPI values in the merged DataFrame
    merged_df['CPI'] = merged_df['CPI'].ffill().sub(100).fillna(0)

    # Determine the fixed annual rate based on the date
    merged_df['FIXED_ANNUAL_RATE'] = np.where(merged_df['DATE'] < first_year_date, initial_rate, margin)

    # Calculate the total annual rate by adding CPI adjustments
    merged_df['TOTAL_ANNUAL_RATE'] = merged_df['CPI'] + merged_df['FIXED_ANNUAL_RATE']

    # Add a dummy date column to help calculate the daily rate
    merged_df['DUMMY_DATE'] = merged_df['DATE'] + DateOffset(days=days_to_new_year(from_date))

    # Set the DataFrame index to the date column
    merged_df = merged_df.set_index('DATE', drop=True)

    # Determine if the year is a leap year for the daily rate calculation
    merged_df['DAILY_RATE'] = merged_df['DUMMY_DATE'].dt.is_leap_year

    # Calculate the daily rate based on whether it's a leap year or not
    merged_df['DAILY_RATE'] = merged_df.apply(
        lambda row: row['TOTAL_ANNUAL_RATE'] / 366 if row['DAILY_RATE'] else row['TOTAL_ANNUAL_RATE'] / 365,
        axis=1) / 100

    # Initialize the base value and daily interest columns as floats
    merged_df['BASE_VALUE'] = current_value.astype(float)
    merged_df['DAILY_INTEREST'] = 0.0
    merged_df['AGGREGATED_VALUE'] = current_value.astype(float)

    # Iterate over the DataFrame to calculate daily interest and aggregate values
    for i in range(0, len(merged_df)):
        # Update base value at the start of each year to the previous day's aggregated value

        merged_df.at[merged_df.index[i], 'BASE_VALUE'] = merged_df.at[merged_df.index[i - 1], 'BASE_VALUE']

        if merged_df.index[i].year > from_date.year and merged_df.index[i].month == from_date.month \
                and merged_df.index[i].day == from_date.day:
            merged_df.at[merged_df.index[i], 'BASE_VALUE'] = merged_df.at[merged_df.index[i - 1], 'AGGREGATED_VALUE']


        # Calculate daily interest
        daily_interest = merged_df.at[merged_df.index[i], 'BASE_VALUE'] * merged_df.at[merged_df.index[i], 'DAILY_RATE']

        # Update daily interest and aggregated value columns
        merged_df.at[merged_df.index[i], 'DAILY_INTEREST'] = daily_interest
        merged_df.at[merged_df.index[i], 'AGGREGATED_VALUE'] = merged_df.at[merged_df.index[
                                                                                i - 1], 'AGGREGATED_VALUE'] + daily_interest

    # Drop the dummy date column as it's no longer needed
    merged_df = merged_df.drop(columns=['DUMMY_DATE'])

    return merged_df

    # Return the final aggregated value or the detailed DataFrame based on the return_table flag
    # if not return_table:
    #     return round(merged_df.iloc[-1]['AGGREGATED_VALUE'], 2)
    # else:
    #     return merged_df


def calculate_edo_values(edo_id, latest_price_date):
    # Get EDO details from ASSETS table in database
    edo_df = get_edo_details(edo_id)  # currently lookup via ASSET_ID supported

    # Get dates required for calculation table
    purchase_date = edo_df['INITIAL_DATE'].iloc[0]
    today_date = pd.Timestamp.today().normalize().tz_localize(None)
    latest_price_date = pd.to_datetime(latest_price_date)
    # Get CPI yearly updates from CPI table
    cpi_table = get_cpi_for_period(purchase_date, today_date)

    # Create a calculations table, toggle return_table based on mode
    # return_table = True# if mode == 'daily' else False

    #TODO - continue calculations from last price, instead of calculating from initial date

    final_df = calculate_edo_aggregated_value(purchase_date, today_date, edo_df, cpi_table)#, return_table=return_table)

    final_df.reset_index(inplace=True)
    final_df['ASSET_ID'] = edo_id
    final_df.rename(columns={'AGGREGATED_VALUE':'PRICE'}, inplace=True)
    final_df['PRICE'] = final_df['PRICE'].round(2)

    # final_df['DATE'] = final_df['index']

    final_df = final_df[['ASSET_ID','DATE','PRICE']].copy()

    final_df = final_df[pd.to_datetime(final_df['DATE'])>latest_price_date]



    # if mode == 'daily':
    #     # Adjust by volume and reformat for daily data
    #     final_df['AGGREGATED_VALUE'] *= volume
    #     final_df = final_df.rename(columns={'AGGREGATED_VALUE': edo_df['NAME'].iloc[0]}
    #                                ).drop(columns=['CPI', 'FIXED_ANNUAL_RATE', 'TOTAL_ANNUAL_RATE',
    #                                                'DAILY_RATE', 'BASE_VALUE', 'DAILY_INTEREST'])
    #
    # else:
    #     final_df = final_df[['AGGREGATED_VALUE']].tail(1).rename(columns={'AGGREGATED_VALUE': edo_id})

    return final_df

def calculate_bulk_edo_values(edo_data, mode='daily'):
    edos_df_list = []
    for index, row in edo_data.iterrows():
        asset_id = row['ASSET_ID']
        latest_price_date = row['DATE']
        # volume = row['VOLUME']
        edo_df = calculate_edo_values(asset_id, latest_price_date)
        edos_df_list.append(edo_df)

    edos_df = pd.concat(edos_df_list, axis=0).fillna(0)
    # edos_df.reset_index(inplace=True)
    # edos_df['TIMESTAMP'] = pd.to_datetime(edos_df['TIMESTAMP'])

    return edos_df