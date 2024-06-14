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
    #plt.subplots_adjust(top=0.85)
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
        ax.text(idx, adjusted_position, f'Subtotal: {row_total:,.2f}({row_total / grand_total * 100:.1f}%)', ha='center',
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

    # Plot the 'Total Portfolio Value'
    ax.fill_between(portfolio_data['TIMESTAMP'], portfolio_data['Total Portfolio Value'], color="skyblue", alpha=0.4)
    ax.plot(portfolio_data['TIMESTAMP'], portfolio_data['Total Portfolio Value'], label='Total Portfolio Value',
            color="Slateblue", alpha=0.6)

    for _, transaction in transactions_data.iterrows():
        closest_date = portfolio_data.iloc[(portfolio_data['TIMESTAMP'] - transaction['TIMESTAMP']).abs().argsort()[:1]]
        transaction_value = closest_date['Total Portfolio Value'].values[0]

        if transaction['BUY_SELL'] == 'B':
            ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='green', marker='^', alpha=0.7,
                       label='Buy' if 'Buy' not in ax.get_legend_handles_labels()[1] else "")
        elif transaction['BUY_SELL'] == 'S':
            ax.scatter(closest_date['TIMESTAMP'], transaction_value, color='red', marker='v', alpha=0.7,
                       label='Sell' if 'Sell' not in ax.get_legend_handles_labels()[1] else "")

    ax.set_title('Portfolio Value Over Time')
    ax.set_xlabel('Date')
    ax.set_ylabel('Total Value')
    ax.legend()
    # Avoid calling plt.show() to enable plot embedding in Tkinter

    return fig
