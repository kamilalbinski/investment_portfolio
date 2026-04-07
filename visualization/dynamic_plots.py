import pandas as pd
import numpy as np
from matplotlib import pyplot as plt


def _data_offset_for_pixels(ax, pixels):
    x0 = ax.transData.inverted().transform((0, 0))[0]
    x1 = ax.transData.inverted().transform((pixels, 0))[0]
    return x1 - x0


def _bar_length_in_pixels(ax, bar):
    left_edge = min(bar.get_x(), bar.get_x() + bar.get_width())
    right_edge = max(bar.get_x(), bar.get_x() + bar.get_width())
    left_px = ax.transData.transform((left_edge, 0))[0]
    right_px = ax.transData.transform((right_edge, 0))[0]
    return abs(right_px - left_px)


def _label_width_in_pixels(ax, label, fontsize=10, fontweight=None):
    renderer = ax.figure.canvas.get_renderer()
    temp_text = ax.text(0, 0, label, fontsize=fontsize, fontweight=fontweight, alpha=0)
    label_width = temp_text.get_window_extent(renderer=renderer).width
    temp_text.remove()
    return label_width


def _annotate_horizontal_bars(ax, bars, labels, fontsize=10, fontweight=None):
    ax.figure.canvas.draw()

    inner_padding = _data_offset_for_pixels(ax, 8)
    outer_padding = _data_offset_for_pixels(ax, 10)

    for bar, label in zip(bars, labels):
        right_edge = max(bar.get_x(), bar.get_x() + bar.get_width())
        label_width = _label_width_in_pixels(ax, label, fontsize=fontsize, fontweight=fontweight)
        fits_inside = _bar_length_in_pixels(ax, bar) >= label_width + 16

        if fits_inside:
            x_position = right_edge - inner_padding
            horizontal_alignment = 'right'
            color = 'white'
        else:
            x_position = right_edge + outer_padding
            horizontal_alignment = 'left'
            color = 'black'

        ax.text(
            x_position,
            bar.get_y() + bar.get_height() / 2,
            label,
            va='center',
            ha=horizontal_alignment,
            fontsize=fontsize,
            fontweight=fontweight,
            color=color
        )


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

def _filter_data_for_timeframe(portfolio_data, transactions_data, timeframe='All'):
    """
    Filter portfolio and transaction data to selected timeframe.
    Timeframe uses latest portfolio timestamp as an anchor.
    """
    if timeframe == 'All':
        return portfolio_data, transactions_data

    if portfolio_data.empty:
        return portfolio_data, transactions_data.iloc[0:0]

    latest_timestamp = portfolio_data['TIMESTAMP'].max()

    if timeframe == 'YTD':
        start_date = pd.Timestamp(year=latest_timestamp.year, month=1, day=1)
    else:
        offsets = {
            '1M': pd.DateOffset(months=1),
            '3M': pd.DateOffset(months=3),
            '6M': pd.DateOffset(months=6),
            '1Y': pd.DateOffset(years=1),
            '3Y': pd.DateOffset(years=3),
            '5Y': pd.DateOffset(years=5),
        }
        start_date = latest_timestamp - offsets.get(timeframe, pd.DateOffset(years=100))

    filtered_portfolio_data = portfolio_data[portfolio_data['TIMESTAMP'] >= start_date].copy()
    filtered_transactions_data = transactions_data[transactions_data['TIMESTAMP'] >= start_date].copy()

    return filtered_portfolio_data, filtered_transactions_data


def plot_portfolio_over_time(portfolio_data, transactions_data, timeframe='All'):
    import matplotlib.pyplot as plt
    import pandas as pd

    # Convert timestamps
    portfolio_data['TIMESTAMP'] = pd.to_datetime(portfolio_data['TIMESTAMP'])
    transactions_data['TIMESTAMP'] = pd.to_datetime(transactions_data['TIMESTAMP'])

    portfolio_data, transactions_data = _filter_data_for_timeframe(
        portfolio_data,
        transactions_data,
        timeframe=timeframe
    )

    if portfolio_data.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        title_suffix = '' if timeframe == 'All' else f' ({timeframe})'
        ax.set_title(f'Portfolio Value Over Time by Sub-Category{title_suffix}')
        ax.text(0.5, 0.5, 'No data for selected timeframe', ha='center', va='center', transform=ax.transAxes)
        ax.set_axis_off()
        return fig

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

    title_suffix = '' if timeframe == 'All' else f' ({timeframe})'
    ax.set_title(f'Portfolio Value Over Time by Sub-Category{title_suffix}')
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

    if aggregated.empty:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_title('Asset Value by Account and Profile' if drill_down_profile else 'Asset Value by Account')
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', transform=ax.transAxes)
        ax.set_axis_off()
        return fig

    # Total asset value (for percentage calculations)
    total_asset_value = aggregated['CURRENT_ASSET_VALUE'].sum()

    # Sort the data so bars are displayed in an intuitive order
    # We sort by *all* grouping columns ascending, except the asset value desc
    aggregated = aggregated.sort_values(
        by=group_cols + ['CURRENT_ASSET_VALUE'],
        ascending=[True] * len(group_cols) + [False]
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    # We'll iterate over the distinct ACCOUNT_NAME groups in the aggregated data
    # and plot sub-bars for each PROFILE if applicable
    positions = []
    labels = []
    current_pos = 0  # Running "y-position" for bars

    bar_patches = []
    bar_labels = []

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
        bars = ax.barh(
            bar_positions,
            group['CURRENT_ASSET_VALUE'],
            label=account_name,
            alpha=0.7
        )
        bar_patches.extend(bars.patches)

        for row_ in group.itertuples():
            current_asset_value = row_.CURRENT_ASSET_VALUE
            percentage_of_total = (current_asset_value / total_asset_value) * 100 if total_asset_value else 0
            bar_labels.append(f'{current_asset_value:,.0f} ({percentage_of_total:.2f}%)')

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

    max_asset_value = aggregated['CURRENT_ASSET_VALUE'].max()
    ax.set_xlim(0, max_asset_value * 1.3 if max_asset_value > 0 else 1)
    ax.margins(y=0.06)
    ax.grid(axis='x', linestyle='--', alpha=0.4)

    _annotate_horizontal_bars(ax, bar_patches, bar_labels, fontsize=10)

    if drill_down_profile and aggregated['ACCOUNT_NAME'].nunique() > 1:
        ax.legend(
            title='Account Name',
            loc='upper right',
            framealpha=0.9
        )

    plt.tight_layout()

    return fig


def plot_current_vs_target_profile(data, portfolio_id=None):
    from views.custom_views import current_vs_target_profile_table

    comparison_df = current_vs_target_profile_table(data=data, portfolio_id=portfolio_id, include_gap=True)

    fig, ax = plt.subplots(figsize=(10, 6))

    if comparison_df.empty:
        ax.set_title('Current Portfolio Versus Target')
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', transform=ax.transAxes)
        ax.set_axis_off()
        return fig

    y_pos = np.arange(len(comparison_df))
    bar_height = 0.45

    ax.barh(
        y_pos,
        comparison_df['TARGET_PCT'],
        height=bar_height,
        color='tab:blue',
        alpha=0.35,
        label='Target allocation (%)'
    )
    ax.barh(
        y_pos,
        comparison_df['CURRENT_PCT'],
        height=bar_height * 0.65,
        color='tab:blue',
        alpha=0.9,
        label='Current allocation (%)'
    )

    max_allocation = comparison_df[['TARGET_PCT', 'CURRENT_PCT']].to_numpy().max()
    right_padding = max(3, max_allocation * 0.15)
    ax.set_xlim(0, max_allocation + right_padding)

    for idx, row in comparison_df.iterrows():
        label_x = max(row['TARGET_PCT'], row['CURRENT_PCT']) + (right_padding * 0.15)
        ax.text(
            label_x,
            idx,
            f"{row['GAP_PCT']:+.1f}%",
            va='center',
            fontsize=9
        )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(comparison_df['PROFILE'])
    ax.invert_yaxis()
    ax.set_xlabel('Allocation (%)')
    ax.set_title('Current Portfolio Versus Target')
    ax.grid(axis='x', linestyle='--', alpha=0.4)
    ax.legend(loc='lower right')
    plt.tight_layout()

    return fig

# Creating the new function
def plot_return_values(df, sort_by='CURRENT_RETURN_VALUE'):
    """
    Plots a horizontal bar plot for current return values and return rates.
    Supports both positive and negative values using a two-sided bar layout.

    """
    df = pd.DataFrame(df).copy()
    if df.empty:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_title('Return Values by Asset')
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', transform=ax.transAxes)
        ax.set_axis_off()
        return fig

    # Sort by the chosen column for better visualization
    # Sort and calculate bar sizes based on the chosen column
    df = df.sort_values(by=sort_by, ascending=True)
    bar_sizes = df[sort_by]

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot horizontal bars using the chosen sort column for size
    bars = ax.barh(df.index, bar_sizes,
                   color=np.where(bar_sizes >= 0, 'green', 'red'), alpha=0.7)

    value_min = min(0, bar_sizes.min())
    value_max = max(0, bar_sizes.max())
    value_span = value_max - value_min if value_max != value_min else max(abs(value_max), 1)
    ax.set_xlim(value_min - (value_span * 0.05), value_max + (value_span * 0.3))
    ax.axvline(0, color='black', linewidth=0.8, alpha=0.5)

    label_texts = [
        f"{row['CURRENT_RETURN_VALUE']:,.2f} ({row['RETURN_RATE']:.2f}%)"
        for _, row in df.iterrows()
    ]
    _annotate_horizontal_bars(ax, bars.patches, label_texts, fontsize=10)

    # Axis labels and title
    ax.set_title(f'Return Values by Asset {df.index.name.capitalize() if df.index.name else "Index"}', fontsize=14)
    ax.set_xlabel('Return Value' if sort_by == 'CURRENT_RETURN_VALUE' else 'Return Rate', fontsize=12)
    ax.set_ylabel(f'Asset {df.index.name.capitalize() if df.index.name else "Index"}', fontsize=12)

    # Grid for better visibility of values
    ax.grid(True, axis='x', linestyle='--', alpha=0.7)

    plt.tight_layout()
    return fig
