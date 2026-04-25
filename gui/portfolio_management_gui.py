# PortfolioManager.py
import customtkinter as ctk
import pandas as pd
import tkinter as tk
from tkinter import ttk, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import os
import datetime

from manage_calculations import calculate_current_values, calculate_return_rate_per_asset
from views.custom_views import default_pivot, default_table, aggregated_values_pivoted, current_vs_target_profile_table
from visualization.dynamic_plots import (
    plot_portfolio_percentage,
    plot_portfolio_over_time,
    plot_asset_value_by_account,
    plot_return_values,
    plot_current_vs_target_profile,
    _filter_data_for_timeframe,
)
from manage_database_functions import refresh_all
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_portfolio_selector_items, get_portfolio_over_time


class PortfolioManager:
    def __init__(self, master):
        self.master = master
        self.master.title("Portfolio Management")
        self.setup_frames()
        self.setup_display_widgets()
        self.setup_control_widgets()
        self.on_selection_change()
        self.log_area.delete("1.0","end")

    def setup_frames(self):
        self.left_frame_width = 300
        self.details_frame_width = 200
        self.table_sub_frame_width = 670
        self.table_frame_width = self.details_frame_width + self.table_sub_frame_width
        self.plot_frame_width = 900
        self.table_frame_height = 280
        self.plot_frame_height = 520
        self.top_panel_color = ("gray86", "gray20")

        self.left_frame = ctk.CTkFrame(master=self.master, width=self.left_frame_width)
        self.left_frame.grid(row=0, column=0, sticky="nswe", padx=20, pady=20)
        self.left_frame.grid_propagate(False)

        self.right_frame = ctk.CTkFrame(master=self.master)
        self.right_frame.grid(row=0, column=1, sticky="nswe", padx=20, pady=20)

        self.table_frame = ctk.CTkFrame(
            master=self.right_frame,
            width=self.table_frame_width,
            height=self.table_frame_height,
            fg_color=self.top_panel_color
        )
        self.table_frame.pack(side=tk.TOP, fill=tk.X, expand=False)
        self.table_frame.pack_propagate(False)

        self.details_frame = ctk.CTkFrame(
            master=self.table_frame,
            width=self.details_frame_width,
            height=self.table_frame_height,
            fg_color=self.top_panel_color
        )
        self.details_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        self.details_frame.pack_propagate(False)

        self.table_sub_frame = ctk.CTkFrame(
            master=self.table_frame,
            width=self.table_sub_frame_width,
            height=self.table_frame_height
        )
        self.table_sub_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=False)
        self.table_sub_frame.pack_propagate(False)

        self.plot_frame = ctk.CTkFrame(
            master=self.right_frame,
            width=self.plot_frame_width,
            height=self.plot_frame_height
        )
        self.plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.plot_frame.pack_propagate(False)

        self.master.update_idletasks()
        self.master.minsize(self.left_frame_width + self.table_frame_width + 80, 800)

    def setup_control_widgets(self):

        self.portfolio_names, self.portfolio_name_to_id = get_portfolio_selector_items()

        # Buttons
        self.refresh_etl_button = ctk.CTkButton(self.left_frame, text="Run ETL", command=self.gui_run_etl)
        self.refresh_etl_button.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.refresh_db_button = ctk.CTkButton(self.left_frame, text="Refresh Database", command=self.gui_refresh_db)
        self.refresh_db_button.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        # Container frame for the label and Combobox
        self.portfolio_selection_frame = ctk.CTkFrame(self.left_frame)
        self.portfolio_selection_frame.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        # Label for "Portfolio Owner:"
        self.portfolio_label = ctk.CTkLabel(self.portfolio_selection_frame, text="Portfolio name:")
        self.portfolio_label.pack(side=tk.LEFT, padx=(0, 10), pady=(10, 10))  # Adjust padding as needed

        # Combobox for selecting the portfolio_id's name
        self.portfolio_combobox = ttk.Combobox(self.portfolio_selection_frame, values=self.portfolio_names, state="readonly")
        self.portfolio_combobox.pack(side=tk.LEFT, fill=tk.X)
        self.portfolio_combobox.set(self.portfolio_names[0] if self.portfolio_names else "")  # Default selection

        self.timeframe_selection_frame = ctk.CTkFrame(self.left_frame)
        self.timeframe_selection_frame.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.timeframe_label = ctk.CTkLabel(self.timeframe_selection_frame, text="Time frame:")
        self.timeframe_label.pack(side=tk.LEFT, padx=(0, 10), pady=(10, 10))

        self.timeframe_values = ["All", "1M", "3M", "6M", "1Y", "3Y", "5Y", "YTD"]
        self.timeframe_combobox = ttk.Combobox(
            self.timeframe_selection_frame,
            values=self.timeframe_values,
            state="readonly",
            width=8
        )
        self.timeframe_combobox.pack(side=tk.LEFT, fill=tk.X)
        self.timeframe_combobox.set("All")

        self.execute_button = ctk.CTkButton(self.left_frame, text="Save results to file (csv)",
                                            command=self.save_table_to_csv)
        self.execute_button.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.portfolio_combobox.bind("<<ComboboxSelected>>", lambda event: self.on_selection_change())
        self.timeframe_combobox.bind("<<ComboboxSelected>>", lambda event: self.on_selection_change())

        # Radio buttons for selecting the plot function
        self.plot_choice = tk.IntVar()
        self.plot_choice.set(1)  # Default to the first plot option

        self.plot_option_a = ctk.CTkRadioButton(self.left_frame, text="Portfolio Value by Category",
                                                variable=self.plot_choice, value=1)
        self.plot_option_a.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.plot_option_b = ctk.CTkRadioButton(self.left_frame, text="Portfolio Value Over Time",
                                                variable=self.plot_choice,
                                                value=2)
        self.plot_option_b.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.plot_option_c = ctk.CTkRadioButton(self.left_frame, text="Current Asset Values Per Account",
                                                variable=self.plot_choice,
                                                value=3)
        self.plot_option_c.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.plot_option_d = ctk.CTkRadioButton(self.left_frame, text="Current Asset Values Per Profile",
                                                variable=self.plot_choice,
                                                value=4)
        self.plot_option_d.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))


        self.plot_option_e = ctk.CTkRadioButton(self.left_frame, text="Portfolio Return Per Asset",
                                                variable=self.plot_choice,
                                                value=5)
        self.plot_option_e.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.plot_option_f = ctk.CTkRadioButton(self.left_frame, text="Current Portfolio Versus Target",
                                                variable=self.plot_choice,
                                                value=6)
        self.plot_option_f.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.plot_option_a.configure(command=self.on_selection_change)
        self.plot_option_b.configure(command=self.on_selection_change)
        self.plot_option_c.configure(command=self.on_selection_change)
        self.plot_option_d.configure(command=self.on_selection_change)
        self.plot_option_e.configure(command=self.on_selection_change)
        self.plot_option_f.configure(command=self.on_selection_change)

        # Dark Mode Switch
        self.dark_mode_switch = ctk.CTkSwitch(self.left_frame, text="Dark Mode", command=self.toggle_dark_mode)
        self.dark_mode_switch.pack(side=tk.BOTTOM, fill=tk.X, padx=(0, 10), pady=(10, 10))

    def setup_display_widgets(self):

        # Portfolio metrics
        self.custom_font = ('Helvetica', 24)  # Font name and size

        # Log Area
        self.log_area = scrolledtext.ScrolledText(self.left_frame, width=30, height=10)
        self.log_area.pack(side=tk.BOTTOM, pady=10)

        self.total_asset_value_label = ctk.CTkLabel(self.details_frame, text="Current portfolio value")
        self.total_asset_value_label.pack(pady=(10, 2), padx=10, anchor='c')

        self.total_asset_value = ctk.CTkLabel(self.details_frame, text="0", font=self.custom_font)
        self.total_asset_value.pack(pady=(2, 10), padx=10, anchor='center')

        self.total_return_label = ctk.CTkLabel(self.details_frame, text="Total return value")
        self.total_return_label.pack(pady=(10, 2), padx=10, anchor='c')

        self.total_return_value = ctk.CTkLabel(self.details_frame, text="0", font=self.custom_font)
        self.total_return_value.pack(pady=(2, 10), padx=10, anchor='center')

        self.total_return_base_label = ctk.CTkLabel(self.details_frame, text="Total return rate")
        self.total_return_base_label.pack(pady=(10, 2), padx=10, anchor='c')

        self.total_return_base_value = ctk.CTkLabel(self.details_frame, text="0%", font=self.custom_font)
        self.total_return_base_value.pack(pady=(2, 10), padx=10, anchor='center')

    def append_log(self, message):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_area.insert("end", f"[{now}] {message}\n")
        self.log_area.see("end")

    def gui_run_etl(self):
        self.append_log("Starting ETL process...")
        run_etl_processes()
        self.append_log("ETL process ran successfully!")

    def gui_refresh_db(self):
        self.append_log("Starting database refresh...")
        try:
            refresh_all()
            self.append_log("Database refreshed successfully!")
        except ValueError as e:
            self.append_log(f"Error during database refresh: {e}")

    def toggle_dark_mode(self):
        if self.dark_mode_switch.get() == 1:
            ctk.set_appearance_mode("dark")
        else:
            ctk.set_appearance_mode("light")

    def update_timeframe_selector_state(self):
        """Enable timeframe selection only for Portfolio Value Over Time plot."""
        if self.plot_choice.get() == 2:
            self.timeframe_combobox.configure(state="readonly")
        else:
            self.timeframe_combobox.set("All")
            self.timeframe_combobox.configure(state="disabled")

    # Further methods would go here...

    def _get_selected_portfolio_id(self):
        selected_portfolio_name = self.portfolio_combobox.get()
        if not selected_portfolio_name:
            return None
        return self.portfolio_name_to_id.get(selected_portfolio_name)

    def save_table_to_csv(self):
        # Get the current portfolio_id selection from a combobox
        portfolio_id = self._get_selected_portfolio_id()

        # Get current date and time, format date as YYYYMMDD
        now = datetime.datetime.now()
        formatted_date = now.strftime('%Y%m%d')

        # Construct the file path for the CSV
        parent_dir = os.path.dirname(os.getcwd())
        # Determine what data to save based on the current plot selection
        if self.plot_choice.get() == 1 or self.plot_choice.get() in (3, 4, 5):
            # Save current portfolio values
            data = default_table(self.plot_data, portfolio_id)
            file_path = os.path.join(parent_dir, f"{formatted_date}_current_assets_output.csv")
        elif self.plot_choice.get() == 6:
            data = current_vs_target_profile_table(self.plot_data, portfolio_id, include_gap=True)
            file_path = os.path.join(parent_dir, f"{formatted_date}_current_vs_target_output.csv")
        elif self.plot_choice.get() == 2:
            # # Save portfolio value over time
            # portfolio_data, transactions_data = get_portfolio_over_time(portfolio_id)
            # selected_timeframe = self.timeframe_combobox.get()
            # data, _ = _filter_data_for_timeframe(portfolio_data, transactions_data, 'All')
            # # data, _ = _filter_data_for_timeframe(portfolio_data, transactions_data, selected_timeframe)
            # file_path = os.path.join(parent_dir, f"{formatted_date}_portfolio_over_time_{selected_timeframe}_output.csv")


            # Save portfolio value over time
            data = aggregated_values_pivoted(None, portfolio_id)
            file_path = os.path.join(parent_dir, f"{formatted_date}_portfolio_over_time_output.csv")



        # Save the data to a CSV file
        data.to_csv(file_path, index=False)
        self.append_log(f"Results saved to {file_path}")

    def on_selection_change(self):

        self.update_timeframe_selector_state()

        selected_portfolio_name = self.portfolio_combobox.get()
        portfolio_id = self._get_selected_portfolio_id()

        self.append_log(f"Portfolio changed to: {selected_portfolio_name}")

        # Get required data

        #TODO verify performance: current value calcs vs stored in table. Create table if needed
        df, asset_value, _, _, return_value, return_rate = calculate_current_values(portfolio_id, return_totals=True)

        self.total_asset_value.configure(text=f"{asset_value:,}")
        self.total_return_value.configure(text=f"{return_value:,}")
        self.total_return_base_value.configure(text=f"{return_rate}%")

        if self.plot_choice.get() == 6:
            self.pivoted_data = current_vs_target_profile_table(df, portfolio_id)
        else:
            self.pivoted_data = default_pivot(df, portfolio_id, save_results=False)
        self.plot_data = df

        # Update table section

        self.display_table_from_dataframe()

        # Update plot section

        self.display_selected_plot(portfolio_id)

    def display_selected_plot(self, portfolio_id):
        self.clear_frame(self.plot_frame)  # Clear any existing content in the frame

        if self.plot_choice.get() == 1:
            fig = plot_portfolio_percentage(self.plot_data)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current asset value of Portfolio: {portfolio_id}")
        elif self.plot_choice.get() == 2:
            portfolio_data, transactions_data = get_portfolio_over_time(portfolio_id)
            self.plot_data = portfolio_data
            selected_timeframe = self.timeframe_combobox.get()
            fig = plot_portfolio_over_time(portfolio_data, transactions_data, timeframe=selected_timeframe)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing asset value over time for Portfolio: {portfolio_id} ({selected_timeframe})")
        elif self.plot_choice.get() == 3:
            fig = plot_asset_value_by_account(self.plot_data, drill_down_profile=False)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current assets return rate by account: {portfolio_id}")
        elif self.plot_choice.get() == 4:
            fig = plot_asset_value_by_account(self.plot_data, drill_down_profile=True)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current assets return rate by profile: {portfolio_id}")
        elif self.plot_choice.get() == 5:
            return_by_asset_data = calculate_return_rate_per_asset(portfolio_id, aggregation_column='PROFILE')
            self.plot_data = return_by_asset_data
            fig = plot_return_values(self.plot_data)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing return by asset: {portfolio_id}")
        elif self.plot_choice.get() == 6:
            fig = plot_current_vs_target_profile(self.plot_data, portfolio_id=portfolio_id)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current portfolio versus target: {portfolio_id}")
        else:
            self.append_log("No plot selected")

    def clear_frame(self, output_frame):
        for widget in output_frame.winfo_children():
            widget.destroy()

    def _format_table_for_display(self, df):
        formatted_df = df.copy()
        value_columns = {'CURRENT_ASSET_VALUE', 'REBALANCING'}
        percentage_columns = {
            'PERCENTAGE',
            'CURRENT_PERCENTAGE',
            'TARGET_PERCENTAGE',
            'GAP_PERCENTAGE',
            'CURRENT_ALLOCATION_PERCENT',
            'ALLOCATION_GAP_PERCENT',
            'CURRENT_PCT',
            'TARGET_PCT',
            'GAP_PCT',
        }

        for col in formatted_df.columns:
            if not pd.api.types.is_numeric_dtype(formatted_df[col]):
                continue

            if col in value_columns:
                formatted_df[col] = formatted_df[col].map(lambda value: f"{value:,.2f}")
            elif col in percentage_columns:
                formatted_df[col] = formatted_df[col].map(lambda value: f"{value:.2f}%")

        return formatted_df

    def _get_tree_column_width(self, column_name):
        fixed_widths = {
            'PROFILE': 140,
            'SUB_CATEGORY': 120,
            'CURRENT_ASSET_VALUE': 120,
            'REBALANCING': 120,
            'PERCENTAGE': 95,
            'CURRENT_PERCENTAGE': 110,
            'TARGET_PERCENTAGE': 110,
            'GAP_PERCENTAGE': 110,
            'CURRENT_ALLOCATION_PERCENT': 110,
            'ALLOCATION_GAP_PERCENT': 110,
            'CURRENT_PCT': 100,
            'TARGET_PCT': 100,
            'GAP_PCT': 100,
        }
        return fixed_widths.get(column_name, 110)

    def _justify_tree_columns(self, tree, column_headers):
        if not column_headers:
            return

        available_width = tree.winfo_width()
        if available_width <= 1:
            return

        preferred_widths = {col: self._get_tree_column_width(col) for col in column_headers}
        total_preferred_width = sum(preferred_widths.values())

        if total_preferred_width >= available_width:
            target_widths = preferred_widths
        else:
            extra_width = available_width - total_preferred_width
            proportional_base = max(total_preferred_width, 1)
            target_widths = {}
            assigned_width = 0

            for col in column_headers[:-1]:
                expanded_width = preferred_widths[col] + round(
                    extra_width * (preferred_widths[col] / proportional_base)
                )
                target_widths[col] = expanded_width
                assigned_width += expanded_width

            last_col = column_headers[-1]
            target_widths[last_col] = max(preferred_widths[last_col], available_width - assigned_width)

        for col in column_headers:
            tree.column(col, width=target_widths[col], minwidth=preferred_widths[col], stretch=tk.NO)

    def _update_tree_scrollbars(self, tree, tree_scroll_x, tree_scroll_y):
        x_start, x_end = tree.xview()
        y_start, y_end = tree.yview()

        if x_start <= 0.0 and x_end >= 1.0:
            tree_scroll_x.grid_remove()
        else:
            tree_scroll_x.grid(row=1, column=0, sticky="ew")

        if y_start <= 0.0 and y_end >= 1.0:
            tree_scroll_y.grid_remove()
        else:
            tree_scroll_y.grid(row=0, column=1, sticky="ns")

    def _refresh_tree_layout(self, tree, column_headers, tree_scroll_x, tree_scroll_y):
        self._justify_tree_columns(tree, column_headers)
        self._update_tree_scrollbars(tree, tree_scroll_x, tree_scroll_y)

    def display_table_from_dataframe(self):
        self.clear_frame(self.table_sub_frame)  # Clear any existing content in the frame
        df = self._format_table_for_display(self.pivoted_data)
        tree_container = ctk.CTkFrame(
            self.table_sub_frame,
            fg_color="transparent",
            width=self.table_sub_frame_width,
            height=self.table_frame_height
        )
        tree_container.pack(fill=tk.BOTH, expand=True)
        tree_container.pack_propagate(False)
        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        tree_scroll_y = ttk.Scrollbar(tree_container, orient=tk.VERTICAL)
        tree_scroll_x = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL)
        tree = ttk.Treeview(
            tree_container,
            yscrollcommand=tree_scroll_y.set,
            xscrollcommand=tree_scroll_x.set
        )
        column_headers = list(df.columns)
        tree["columns"] = column_headers
        tree.column("#0", width=0, stretch=tk.NO)  # Hiding the default column
        for col in column_headers:
            tree.column(col, width=self._get_tree_column_width(col), minwidth=self._get_tree_column_width(col),
                        stretch=tk.NO, anchor=tk.W)
            tree.heading(col, text=col, anchor=tk.W)
        for _, row in df.iterrows():
            tree.insert("", tk.END, values=tuple(row))
        for col in ('CURRENT_ASSET_VALUE', 'REBALANCING', 'PERCENTAGE', 'CURRENT_ALLOCATION_PERCENT',
                    'TARGET_PERCENTAGE', 'ALLOCATION_GAP_PERCENT', 'CURRENT_PERCENTAGE',
                    'GAP_PERCENTAGE', 'CURRENT_PCT', 'TARGET_PCT', 'GAP_PCT'):
            if col in column_headers:
                tree.column(col, anchor='e')

        tree_scroll_y.config(command=tree.yview)
        tree_scroll_x.config(command=tree.xview)

        tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        tree_scroll_x.grid(row=1, column=0, sticky="ew")

        tree.bind(
            "<Configure>",
            lambda event: self._refresh_tree_layout(tree, column_headers, tree_scroll_x, tree_scroll_y)
        )
        self.master.after_idle(lambda: self._refresh_tree_layout(tree, column_headers, tree_scroll_x, tree_scroll_y))


def main():
    root = ctk.CTk()
    app = PortfolioManager(root)
    root.mainloop()


if __name__ == "__main__":
    main()
