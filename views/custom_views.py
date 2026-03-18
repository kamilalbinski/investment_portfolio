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


def _is_all_portfolio(portfolio_id):
    portfolios = get_current_portfolio_data('PORTFOLIOS', portfolio_id=portfolio_id)
    if portfolios.empty:
        return False

    row = portfolios.iloc[0]
    if 'IS_ALL_HOLDINGS' in portfolios.columns:
        try:
            return int(row['IS_ALL_HOLDINGS']) == 1
        except (TypeError, ValueError):
            return False

    name = str(row.get('NAME', '')).strip().upper()
    return name in {'ALL', 'ALL PORTFOLIO', 'ALL HOLDINGS'}


def _build_weighted_targets_for_all_portfolio(current_holdings_df):
    portfolios = get_current_portfolio_data('PORTFOLIOS')
    if portfolios.empty:
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])

    required_columns = {'PORTFOLIO_ID', 'OWNER_ID'}
    if not required_columns.issubset(portfolios.columns):
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])

    if 'IS_ALL_HOLDINGS' in portfolios.columns:
        portfolios = portfolios[portfolios['IS_ALL_HOLDINGS'] != 1]

    owner_weights = (
        current_holdings_df.groupby('ACCOUNT_OWNER_ID', dropna=False)['CURRENT_ASSET_VALUE']
        .sum()
        .reset_index()
        .rename(columns={'CURRENT_ASSET_VALUE': 'OWNER_CURRENT_VALUE'})
    )
    total_owner_value = owner_weights['OWNER_CURRENT_VALUE'].sum()
    if total_owner_value <= 0:
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])
    owner_weights['OWNER_WEIGHT'] = owner_weights['OWNER_CURRENT_VALUE'] / total_owner_value

    portfolio_weights = portfolios.merge(
        owner_weights,
        left_on='OWNER_ID',
        right_on='ACCOUNT_OWNER_ID',
        how='inner'
    )
    if portfolio_weights.empty:
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])

    targets = get_current_portfolio_data('PORTFOLIO_PROFILE_TARGETS')
    if targets.empty:
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])

    weighted_targets = targets.merge(
        portfolio_weights[['PORTFOLIO_ID', 'OWNER_WEIGHT']],
        on='PORTFOLIO_ID',
        how='inner'
    )
    if weighted_targets.empty:
        return pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])

    weighted_targets['TARGET_PERCENTAGE'] = (
        weighted_targets['TARGET_PERCENTAGE'] * weighted_targets['OWNER_WEIGHT']
    )
    return weighted_targets.groupby('PROFILE', as_index=False)['TARGET_PERCENTAGE'].sum()


def current_vs_target_profile_table(data=None, portfolio_id=None, include_gap=False):
    df = data
    if df is None or df.empty:
        df = calculate_current_values(portfolio_id, return_totals=False)

    current_allocations = (
        df.groupby('PROFILE', dropna=False)['CURRENT_ASSET_VALUE']
        .sum()
        .reset_index()
    )
    current_allocations['PROFILE'] = current_allocations['PROFILE'].fillna('UNASSIGNED')

    total_value = current_allocations['CURRENT_ASSET_VALUE'].sum()
    if total_value > 0:
        current_allocations['CURRENT_ALLOCATION_PERCENT'] = (
            current_allocations['CURRENT_ASSET_VALUE'] / total_value * 100
        )
    else:
        current_allocations['CURRENT_ALLOCATION_PERCENT'] = 0

    if _is_all_portfolio(portfolio_id):
        targets = _build_weighted_targets_for_all_portfolio(df)
    else:
        targets = get_current_portfolio_data('PORTFOLIO_PROFILE_TARGETS', portfolio_id=portfolio_id)
        if targets.empty:
            targets = pd.DataFrame(columns=['PROFILE', 'TARGET_PERCENTAGE'])
        else:
            targets = (
                targets[['PROFILE', 'TARGET_PERCENTAGE']]
                .groupby('PROFILE', as_index=False)['TARGET_PERCENTAGE']
                .sum()
            )

    merged = current_allocations.merge(targets, on='PROFILE', how='outer')
    merged['CURRENT_ASSET_VALUE'] = merged['CURRENT_ASSET_VALUE'].fillna(0.0)
    merged['CURRENT_ALLOCATION_PERCENT'] = merged['CURRENT_ALLOCATION_PERCENT'].fillna(0.0)
    merged['TARGET_PERCENTAGE'] = merged['TARGET_PERCENTAGE'].fillna(0.0)
    merged['GAP_PCT'] = merged['CURRENT_ALLOCATION_PERCENT'] - merged['TARGET_PERCENTAGE']
    merged = merged.sort_values('CURRENT_ALLOCATION_PERCENT', ascending=False)

    merged['CURRENT_ASSET_VALUE'] = merged['CURRENT_ASSET_VALUE'].round(2)
    merged['CURRENT_PCT'] = merged['CURRENT_ALLOCATION_PERCENT'].round(2)
    merged['TARGET_PCT'] = merged['TARGET_PERCENTAGE'].round(2)
    merged['GAP_PCT'] = merged['GAP_PCT'].round(2)

    output_columns = ['PROFILE', 'CURRENT_ASSET_VALUE', 'CURRENT_PCT', 'TARGET_PCT']
    if include_gap:
        output_columns.append('GAP_PCT')

    return merged[output_columns].reset_index(drop=True)
