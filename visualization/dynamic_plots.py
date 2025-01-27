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


def plot_portfolio_over_time(portfolio_data, transactions_data):
    # Convert 'TIMESTAMP' to datetime if it's not already
    portfolio_data['TIMESTAMP'] = pd.to_datetime(portfolio_data['TIMESTAMP'])
    transactions_data['TIMESTAMP'] = pd.to_datetime(transactions_data['TIMESTAMP'])

    # Create figure and axes for the plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the 'AGGREGATED_VALUE'
    ax.fill_between(portfolio_data['TIMESTAMP'], portfolio_data['AGGREGATED_VALUE'], color="skyblue", alpha=0.4)
    ax.plot(portfolio_data['TIMESTAMP'], portfolio_data['AGGREGATED_VALUE'], label='AGGREGATED_VALUE',
            color="Slateblue", alpha=0.6)

    # Include buy & sell information on the plot. Use 1-day delay for better visibility.
    for _, transaction in transactions_data.iterrows():
        closest_date = portfolio_data.iloc[
            (portfolio_data['TIMESTAMP'] - (transaction['TIMESTAMP'] - pd.Timedelta(days=1))).abs().argsort()[:1]]
        transaction_value = closest_date['AGGREGATED_VALUE'].values[0]

        if transaction['BUY_SELL'] == 'B':
            ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='green', marker='^', alpha=0.7,
                       label='Buy' if 'Buy' not in ax.get_legend_handles_labels()[1] else "")
        elif transaction['BUY_SELL'] == 'S':
            ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='red', marker='v', alpha=0.7,
                       label='Sell' if 'Sell' not in ax.get_legend_handles_labels()[1] else "")

    ax.grid(True, axis='y', zorder=1, linestyle='--', )

    ax.set_title('Portfolio Value Over Time', zorder=2)
    ax.set_xlabel('Date', zorder=2)
    ax.set_ylabel('Total Value', zorder=2)
    ax.legend()

    return fig


def plot_asset_value_by_account(data):
    df = pd.DataFrame(data)

    # Aggregate CURRENT_ASSET_VALUE by ACCOUNT_NAME and PROFILE
    aggregated = df.groupby(['ACCOUNT_NAME', 'PROFILE']).agg(
        CURRENT_ASSET_VALUE=('CURRENT_ASSET_VALUE', 'sum')
    ).reset_index()

    # Calculate the total asset value for percentage calculation
    total_asset_value = aggregated['CURRENT_ASSET_VALUE'].sum()

    # Sort by ACCOUNT_NAME and CURRENT_ASSET_VALUE in descending order
    aggregated = aggregated.sort_values(by=['ACCOUNT_NAME', 'CURRENT_ASSET_VALUE'], ascending=[True, False])

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    # Initialize y-position for bars
    positions = []
    labels = []
    for account_name, group in aggregated.groupby('ACCOUNT_NAME'):
        bar_positions = range(len(labels), len(labels) + len(group))
        positions.extend(bar_positions)
        labels.extend(group['PROFILE'])

        # Plot bars
        ax.barh(
            bar_positions,
            group['CURRENT_ASSET_VALUE'],
            label=account_name,
            alpha=0.7
        )

        # Add annotations for CURRENT_ASSET_VALUE and percentage of total
        for i, row in enumerate(group.itertuples()):
            current_asset_value = row.CURRENT_ASSET_VALUE
            percentage_of_total = (current_asset_value / total_asset_value) * 100

            # Determine if the bar is long enough for inside text
            if current_asset_value > total_asset_value * 0.05:  # If the bar length is > 5% of total
                annotation_position = current_asset_value - (total_asset_value * 0.01)  # Slight padding
                ha_align = 'right'  # Align text to the right
                color = 'white'  # Use contrasting color
            else:
                annotation_position = current_asset_value + 0.1  # Outside text
                ha_align = 'left'
                color = 'black'

            ax.text(
                annotation_position, bar_positions[i],
                f'{current_asset_value:,.0f} ({percentage_of_total:.2f}%)',
                va='center', ha=ha_align, fontsize=10, color=color
            )

    # Customizing the plot
    ax.set_yticks(positions)
    ax.set_yticklabels(labels)
    ax.set_xlabel('Current Asset Value', fontsize=12)
    ax.set_title('Asset Value by Account and Profile', fontsize=14)
    ax.legend(title='Account Name', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Corrected tight layout usage
    plt.tight_layout()

    return fig
