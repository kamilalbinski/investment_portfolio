from manage_calculations import calculate_current_values
from utils.database_setup import get_asset_ids_from_database, get_current_portfolio_data
import pandas as pd


def default_pivot(data=None, portfolio_id=None, save_results=False):
    df = data
    if df is None or df.empty:
        df = calculate_current_values(portfolio_id, return_totals=False)

    detailed_pivot = pd.pivot_table(df, values=['CURRENT_ASSET_VALUE'], index=['SUB_CATEGORY', 'PROFILE'],
                                    aggfunc='sum').reset_index()
    detailed_pivot['SortKey'] = detailed_pivot['PROFILE']
    detailed_pivot['IsSubtotal'] = False

    subtotals = pd.pivot_table(df, values='CURRENT_ASSET_VALUE', index=['SUB_CATEGORY'], aggfunc='sum').reset_index()
    subtotals['PROFILE'] = '[Subtotal]'
    subtotals['SortKey'] = ''
    subtotals['IsSubtotal'] = True

    combined_pivot = pd.concat([detailed_pivot, subtotals], ignore_index=True)
    combined_pivot = combined_pivot.sort_values(by=['SUB_CATEGORY', 'IsSubtotal', 'SortKey'])
    grand_total = df['CURRENT_ASSET_VALUE'].sum()

    combined_pivot.drop(columns=['SortKey', 'IsSubtotal'], inplace=True)
    combined_pivot['PERCENTAGE'] = (combined_pivot['CURRENT_ASSET_VALUE'] / grand_total).round(4).apply(
        lambda x: f"{x * 100: >7.2f}%")
    combined_pivot['CURRENT_ASSET_VALUE'] = combined_pivot['CURRENT_ASSET_VALUE'].round(2).apply(lambda x: f"{x:,.2f}")

    if save_results:
        combined_pivot.to_csv('pivot.csv', index=False)

    return combined_pivot


def default_table(data=None, portfolio_id=None):
    df = data
    if df is None or df.empty:
        df = calculate_current_values(portfolio_id, return_totals=False)

    columns = [
        'PORTFOLIO_NAME',
        'ACCOUNT_OWNER_ID',
        'ACCOUNT_NAME',
        'NAME',
        'CATEGORY',
        'SUB_CATEGORY',
        'PROFILE',
        'TARGET_ALLOCATION',
        'CURRENT_ASSET_VALUE',
        'RETURN_RATE',
        'RETURN_RATE_BASE'
    ]

    return df[columns]


def aggregated_values_pivoted(data=None, portfolio_id=None):
    df = data

    if df is None or df.empty:
        df = get_current_portfolio_data('AGGREGATED_VALUES', portfolio_id=portfolio_id)

    assets_df = get_asset_ids_from_database()
    merged_df = df.merge(assets_df[['ASSET_ID', 'NAME']], how='left', on='ASSET_ID')
    agg_df = merged_df.groupby(['TIMESTAMP', 'NAME'])['AGGREGATED_VALUE'].sum().reset_index()
    transformed_df = agg_df.pivot(index='TIMESTAMP', columns='NAME', values='AGGREGATED_VALUE')

    return transformed_df.reset_index()