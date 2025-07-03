import pandas as pd
import numpy as np
from matplotlib import pyplot as plt


def plot_portfolio_percentage(data):
    df = pd.DataFrame(data)

    grouped = df.groupby(['SUB_CATEGORY', 'PROFILE'])['CURRENT_ASSET_VALUE'].sum().unstack(fill_value=0)
    subtotals = grouped.sum(axis=1)
    grand_total = df['CURRENT_ASSET_VALUE'].sum()
    sorted_profiles = grouped.sum(axis=0).sort_values().index.tolist()
    grouped = grouped[sorted_profiles]

    cmap = plt.get_cmap('Blues')

    fig, ax = plt.subplots(figsize=(10, 6))
    grouped.plot(kind='bar', stacked=True, ax=ax, colormap=cmap, edgecolor='black', linewidth=1, width=0.8)

    plt.title('Current Asset Value by Sub-Category and Profile')
    plt.xlabel('')
    plt.ylabel('Current Asset Value')
    plt.xticks(rotation=0)
    plt.legend(title='Profile')

    plt.subplots_adjust(top=0.95)
    # plt.subplots_adjust(top=0.85)
    plt.tight_layout()

    bottoms = np.zeros(len(grouped))

    for idx, (name, row) in enumerate(grouped.iterrows()):
        row_total = 0
        for col, value in row.items():
            proportion_of_grand = value / grand_total * 100
            profile_midpoint = bottoms[idx] + value / 2
            if value > 0:
                ax.text(idx, profile_midpoint, f'{col}: {proportion_of_grand:.1f}%', ha='center', fontsize=9)
            bottoms[idx] += value
            row_total += value

        adjusted_position = bottoms[idx] * 1
        ax.text(idx, adjusted_position, f'Subtotal: {row_total:,.2f}({row_total / grand_total * 100:.1f}%)',
                ha='center',
                va='bottom', color='black', fontsize=10, weight='bold')

    # ax.text(len(grouped) / 4, max(bottoms) * 1, f'Grand Total: {grand_total:,.2f}', ha='center', va='bottom',
    #         fontsize=12, weight='bold', color='black')

    return fig


# def plot_portfolio_over_time(portfolio_data, transactions_data):
#     # Convert 'TIMESTAMP' to datetime if it's not already
#     portfolio_data['TIMESTAMP'] = pd.to_datetime(portfolio_data['TIMESTAMP'])
#     transactions_data['TIMESTAMP'] = pd.to_datetime(transactions_data['TIMESTAMP'])
#
#     # Create figure and axes for the plot
#     fig, ax = plt.subplots(figsize=(10, 6))
#
#     # Plot the 'AGGREGATED_VALUE'
#
#     ax.fill_between(portfolio_data['TIMESTAMP'], portfolio_data['AGGREGATED_VALUE'], color="skyblue", alpha=0.4)
#     ax.plot(portfolio_data['TIMESTAMP'], portfolio_data['AGGREGATED_VALUE'], label='AGGREGATED_VALUE',
#             color="Slateblue", alpha=0.6)
#
#     # Include buy & sell information on the plot. Use 1-day delay for better visibility.
#     for _, transaction in transactions_data.iterrows():
#         closest_date = portfolio_data.iloc[
#             (portfolio_data['TIMESTAMP'] - (transaction['TIMESTAMP'] - pd.Timedelta(days=1))).abs().argsort()[:1]]
#         transaction_value = closest_date['AGGREGATED_VALUE'].values[0]
#
#         if transaction['BUY_SELL'] == 'B':
#             ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='green', marker='^', alpha=0.7,
#                        label='Buy' if 'Buy' not in ax.get_legend_handles_labels()[1] else "")
#         elif transaction['BUY_SELL'] == 'S':
#             ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='red', marker='v', alpha=0.7,
#                        label='Sell' if 'Sell' not in ax.get_legend_handles_labels()[1] else "")
#
#     ax.grid(True, axis='y', zorder=1, linestyle='--', )
#
#     ax.set_title('Portfolio Value Over Time', zorder=2)
#     ax.set_xlabel('Date', zorder=2)
#     ax.set_ylabel('Total Value', zorder=2)
#     ax.legend()
#
#     return fig

def plot_portfolio_over_time(portfolio_data, transactions_data):
    import matplotlib.pyplot as plt
    import pandas as pd

    # Convert timestamps
    portfolio_data['TIMESTAMP'] = pd.to_datetime(portfolio_data['TIMESTAMP'])
    transactions_data['TIMESTAMP'] = pd.to_datetime(transactions_data['TIMESTAMP'])

    # Pivot to wide format: TIMESTAMP as index, SUB_CATEGORY as columns
    pivot_df = portfolio_data.pivot_table(
        index='TIMESTAMP',
        columns='SUB_CATEGORY',
        values='AGGREGATED_VALUE',
        aggfunc='sum'
    ).fillna(0).sort_index()

    fig, ax = plt.subplots(figsize=(10, 6))

    # Use a nicer color palette with subtle edge lines
    cmap = plt.get_cmap('tab10')
    pivot_df.plot.area(ax=ax, alpha=0.85, colormap=cmap, linewidth=0.5)

    # Re-plot buy/sell markers (unchanged)
    total_value = pivot_df.sum(axis=1)

    for _, transaction in transactions_data.iterrows():
        closest_idx = total_value.index.get_indexer(
            [transaction['TIMESTAMP'] - pd.Timedelta(days=1)],
            method='nearest'
        )[0]
        transaction_date = total_value.index[closest_idx]
        transaction_value = total_value.iloc[closest_idx]

        if transaction['BUY_SELL'] == 'B':
            ax.scatter(transaction_date, transaction_value, color='green', marker='^', alpha=0.7,
                       label='Buy' if 'Buy' not in ax.get_legend_handles_labels()[1] else "")
        elif transaction['BUY_SELL'] == 'S':
            ax.scatter(transaction_date, transaction_value, color='red', marker='v', alpha=0.7,
                       label='Sell' if 'Sell' not in ax.get_legend_handles_labels()[1] else "")

    ax.set_title('Portfolio Value Over Time by Sub-Category')
    ax.set_xlabel('Date')
    ax.set_ylabel('Total Value')
    ax.grid(True, axis='y', linestyle='--')
    ax.legend()

    return fig



def plot_asset_value_by_account(data, drill_down_profile=True):
    """
    Plots the sum of CURRENT_ASSET_VALUE grouped by ACCOUNT_NAME,
    optionally drilling down by PROFILE if drill_down_profile=True.
    """

    df = pd.DataFrame(data)

    # Decide which columns to group by depending on the drill_down_profile flag
    group_cols = ['ACCOUNT_NAME']
    if drill_down_profile:
        group_cols.append('PROFILE')  # Include 'PROFILE' only if we want a deeper breakdown

    # Group the data
    aggregated = (
        df.groupby(group_cols, dropna=False)
        .agg(CURRENT_ASSET_VALUE=('CURRENT_ASSET_VALUE', 'sum'))
        .reset_index()
    )

    # Total asset value (for percentage calculations)
    total_asset_value = aggregated['CURRENT_ASSET_VALUE'].sum()

    # Sort the data so bars are displayed in an intuitive order
    # We sort by *all* grouping columns ascending, except the asset value desc
    aggregated = aggregated.sort_values(
        by=group_cols + ['CURRENT_ASSET_VALUE'],
        ascending=[True] * len(group_cols) + [False]
    )

    # Create the figure
    fig, ax = plt.subplots(figsize=(10, 6))

    # We'll iterate over the distinct ACCOUNT_NAME groups in the aggregated data
    # and plot sub-bars for each PROFILE if applicable
    positions = []
    labels = []
    current_pos = 0  # Running "y-position" for bars

    for account_name, group in aggregated.groupby('ACCOUNT_NAME'):
        # If we have profiles, each account_name group may contain multiple rows (one per profile).
        # Otherwise, there's just one row (the sum for that account).

        # If we *did* include PROFILE in grouping, we’ll use them as sub-labels
        # Otherwise, just label by the account_name itself
        sublabels = None
        if drill_down_profile and 'PROFILE' in group.columns:
            sublabels = group['PROFILE'].tolist()
        else:
            sublabels = [account_name]  # Single bar for the entire account

        bar_positions = range(current_pos, current_pos + len(group))
        positions.extend(bar_positions)
        labels.extend(sublabels)

        # Plot the bars for this (ACCOUNT_NAME) group
        ax.barh(
            bar_positions,
            group['CURRENT_ASSET_VALUE'],
            label=account_name,
            alpha=0.7
        )

        # Add text annotations for each bar
        for i, row_ in enumerate(group.itertuples()):
            current_asset_value = row_.CURRENT_ASSET_VALUE
            percentage_of_total = (current_asset_value / total_asset_value) * 100
            bar_ypos = list(bar_positions)[i]

            # Decide if annotation should be inside or outside the bar
            if current_asset_value > total_asset_value * 0.05:
                # Place text inside the bar
                annotation_position = current_asset_value - (total_asset_value * 0.01)
                ha_align = 'right'
                color = 'white'
            else:
                # Place text just outside the bar
                annotation_position = current_asset_value + (total_asset_value * 0.01)
                ha_align = 'left'
                color = 'black'

            ax.text(
                annotation_position,
                bar_ypos,
                f'{current_asset_value:,.0f} ({percentage_of_total:.2f}%)',
                va='center',
                ha=ha_align,
                fontsize=10,
                color=color
            )

        # After plotting all sub-bars (profiles) for this account, advance our position counter
        current_pos += len(group)

    # Configure axis labels and plot title
    ax.set_yticks(positions)
    ax.set_yticklabels(labels)

    ax.set_xlabel('Current Asset Value', fontsize=12)
    if drill_down_profile:
        ax.set_title('Asset Value by Account and Profile', fontsize=14)
    else:
        ax.set_title('Asset Value by Account', fontsize=14)

    ax.legend(title='Account Name', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    return fig

# Creating the new function
def plot_return_values(df, sort_by='CURRENT_RETURN_VALUE'):
    """
    Plots a horizontal bar plot for current return values and return rates.
    Supports both positive and negative values using a two-sided bar layout.

    """

    # Sort by the chosen column for better visualization
    # Sort and calculate bar sizes based on the chosen column
    df = df.sort_values(by=sort_by, ascending=True)
    bar_sizes = df[sort_by]

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot horizontal bars using the chosen sort column for size
    bars = ax.barh(df.index, bar_sizes,
                   color=np.where(bar_sizes >= 0, 'green', 'red'), alpha=0.7)

    # Adding text labels (value and rate) on the bars
    for bar, (index, row) in zip(bars, df.iterrows()):
        value = row['CURRENT_RETURN_VALUE']
        rate = row['RETURN_RATE']
        text_pos = bar.get_width()
        alignment = 'left' if text_pos < 0 else 'right'
        ax.text(
            text_pos - (text_pos * 0.01) if text_pos > 0 else text_pos + (text_pos * 0.01),
            bar.get_y() + bar.get_height() / 2,
            f'{value:,.2f} ({rate:.2f}%)',
            va='center',
            ha=alignment,
            fontsize=10,
            color='black' #if text_pos > 0 else 'black'
        )

    # Axis labels and title
    ax.set_title(f'Return Values by Asset {df.index.name.capitalize() if df.index.name else "Index"}', fontsize=14)
    ax.set_xlabel('Return Value' if sort_by == 'CURRENT_RETURN_VALUE' else 'Return Rate', fontsize=12)
    ax.set_ylabel(f'Asset {df.index.name.capitalize() if df.index.name else "Index"}', fontsize=12)

    # Grid for better visibility of values
    ax.grid(True, axis='x', linestyle='--', alpha=0.7)

    plt.tight_layout()
    return fig