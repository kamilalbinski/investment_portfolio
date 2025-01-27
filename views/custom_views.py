from manage_calculations import calculate_current_values
from utils.database_setup import get_asset_ids_from_database, get_current_portfolio_data
import pandas as pd


def default_pivot(data=None, owner=None, save_results=False):
    #TODO Add more columns to the view
    df = data
    if df is None or df.empty:
        df = calculate_current_values(owner, return_totals=False)

    # Calculate detailed sum for PROFILE level
    detailed_pivot = pd.pivot_table(df, values=['CURRENT_ASSET_VALUE'], index=['SUB_CATEGORY', 'PROFILE'],
                                    aggfunc='sum').reset_index()
    detailed_pivot['SortKey'] = detailed_pivot['PROFILE']  # Normal rows will be sorted by PROFILE
    detailed_pivot['IsSubtotal'] = False  # Marking detailed rows

    # Calculate Subtotals for each SUB_CATEGORY
    subtotals = pd.pivot_table(df, values='CURRENT_ASSET_VALUE', index=['SUB_CATEGORY'], aggfunc='sum').reset_index()
    subtotals['PROFILE'] = '[Subtotal]'  # Marking subtotal rows
    subtotals['SortKey'] = ''  # Ensuring subtotals sort to the top
    subtotals['IsSubtotal'] = True

    # Combine detailed and subtotal data
    combined_pivot = pd.concat([detailed_pivot, subtotals], ignore_index=True)

    # Sorting combined data to ensure subtotals are correctly positioned
    combined_pivot = combined_pivot.sort_values(by=['SUB_CATEGORY', 'IsSubtotal', 'SortKey'])

    # Calculate grand total of CURRENT_ASSET_VALUE and portion for each row
    grand_total = df['CURRENT_ASSET_VALUE'].sum()

    # Dropping the sorting helper columns for the final output
    combined_pivot.drop(columns=['SortKey', 'IsSubtotal'], inplace=True)

    combined_pivot['PERCENTAGE'] = (combined_pivot['CURRENT_ASSET_VALUE'] / grand_total).round(4).apply(
        lambda x: f"{x * 100: >7.2f}%")  # .map('{:.2%}'.format)
    combined_pivot['CURRENT_ASSET_VALUE'] = combined_pivot['CURRENT_ASSET_VALUE'].round(2).apply(lambda x: f"{x:,.2f}")

    if save_results:
        combined_pivot.to_csv('pivot.csv', index=False)

    # print(combined_pivot)

    return combined_pivot


def default_table(data=None, owner=None):
    df = data
    if df is None or df.empty:
        df = calculate_current_values(owner, return_totals=False)

    if not owner:
        columns = [
            'ACCOUNT_OWNER',
            'ACCOUNT_NAME',
            'NAME',
            'CATEGORY',
            'SUB_CATEGORY',
            'PROFILE',
            'CURRENT_PRICE',
            'CURRENT_ASSET_VALUE',
            'RETURN_RATE',
            'RETURN_RATE_BASE'
        ]
    else:
        columns = [
            'ACCOUNT_NAME',
            'NAME',
            'CATEGORY',
            'SUB_CATEGORY',
            'PROFILE',
            'CURRENT_ASSET_VALUE',
            'RETURN_RATE',
            'RETURN_RATE_BASE'
        ]

    transformed_df = df[columns]

    return transformed_df

def aggregated_values_pivoted(data=None, owner=None):

    df = data

    if df is None or df.empty:
        df = get_current_portfolio_data('AGGREGATED_VALUES', account_owner=owner)

    if owner:
        df = df[df['ACCOUNT_OWNER']==owner]

    assets_df = get_asset_ids_from_database()
    merged_df = df.merge(assets_df[['ASSET_ID','NAME']], how='left', on='ASSET_ID')

    agg_df = merged_df.groupby(['TIMESTAMP', 'NAME'])['AGGREGATED_VALUE'].sum().reset_index()

    transformed_df = agg_df.pivot(index='TIMESTAMP', columns='NAME',values='AGGREGATED_VALUE')

    return transformed_df.reset_index()
