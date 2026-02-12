# PortfolioManager.py
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import os
import datetime

from manage_calculations import calculate_current_values, calculate_return_rate_per_asset
from views.custom_views import default_pivot, default_table
from visualization.dynamic_plots import (
    plot_portfolio_percentage,
    plot_portfolio_over_time,
    plot_asset_value_by_account,
    plot_return_values,
    _filter_data_for_timeframe,
)
from manage_database_functions import refresh_all
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_temporary_owners_list, get_portfolio_over_time


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
        self.left_frame = ctk.CTkFrame(master=self.master, width=300)
        self.left_frame.grid(row=0, column=0, sticky="nswe", padx=20, pady=20)

        self.right_frame = ctk.CTkFrame(master=self.master)
        self.right_frame.grid(row=0, column=1, sticky="nswe", padx=20, pady=20)

        self.table_frame = ctk.CTkFrame(master=self.right_frame, width=500)
        self.table_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=False)

        self.details_frame = ctk.CTkFrame(master=self.table_frame, width=250)
        self.details_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.table_sub_frame = ctk.CTkFrame(master=self.table_frame, width=150)
        self.table_sub_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=False)

        self.plot_frame = ctk.CTkFrame(master=self.right_frame, width=650)
        self.plot_frame.pack(side=tk.TOP, fill=tk.X, expand=False)

    def setup_control_widgets(self):

        self.owner_names = get_temporary_owners_list()

        # Buttons
        self.refresh_etl_button = ctk.CTkButton(self.left_frame, text="Run ETL", command=self.gui_run_etl)
        self.refresh_etl_button.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        self.refresh_db_button = ctk.CTkButton(self.left_frame, text="Refresh Database", command=self.gui_refresh_db)
        self.refresh_db_button.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        # Container frame for the label and Combobox
        self.owner_selection_frame = ctk.CTkFrame(self.left_frame)
        self.owner_selection_frame.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(10, 10))

        # Label for "Portfolio Owner:"
        self.owner_label = ctk.CTkLabel(self.owner_selection_frame, text="Portfolio name:")
        self.owner_label.pack(side=tk.LEFT, padx=(0, 10), pady=(10, 10))  # Adjust padding as needed

        # Combobox for selecting the owner's name
        self.owner_combobox = ttk.Combobox(self.owner_selection_frame, values=self.owner_names, state="readonly")
        self.owner_combobox.pack(side=tk.LEFT, fill=tk.X)
        self.owner_combobox.set("All")  # Default selection

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

        self.owner_combobox.bind("<<ComboboxSelected>>", lambda event: self.on_selection_change())
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


        self.plot_option_a.configure(command=self.on_selection_change)
        self.plot_option_b.configure(command=self.on_selection_change)
        self.plot_option_c.configure(command=self.on_selection_change)
        self.plot_option_d.configure(command=self.on_selection_change)
        self.plot_option_e.configure(command=self.on_selection_change)

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

    def save_table_to_csv(self):
        # Get the current owner selection from a combobox
        owner = self.owner_combobox.get()
        if owner == 'All':
            owner = None

        # Get current date and time, format date as YYYYMMDD
        now = datetime.datetime.now()
        formatted_date = now.strftime('%Y%m%d')

        # Construct the file path for the CSV
        parent_dir = os.path.dirname(os.getcwd())
        # Determine what data to save based on the current plot selection
        if self.plot_choice.get() == 1 or self.plot_choice.get() >= 3:
            # Save current portfolio values
            data = default_table(self.plot_data, owner)
            file_path = os.path.join(parent_dir, f"{formatted_date}_current_assets_output.csv")
        elif self.plot_choice.get() == 2:
            # Save portfolio value over time
            portfolio_data, transactions_data = get_portfolio_over_time(owner)
            selected_timeframe = self.timeframe_combobox.get()
            data, _ = _filter_data_for_timeframe(portfolio_data, transactions_data, selected_timeframe)
            file_path = os.path.join(parent_dir, f"{formatted_date}_portfolio_over_time_{selected_timeframe}_output.csv")

        # Save the data to a CSV file
        data.to_csv(file_path, index=False)
        self.append_log(f"Results saved to {file_path}")

    def on_selection_change(self):

        self.update_timeframe_selector_state()

        owner = self.owner_combobox.get()

        self.append_log(f"Owner changed to: {self.owner_combobox.get()}")

        if owner == 'All':
            owner = None

        # Get required data

        #TODO verify performance: current value calcs vs stored in table. Create table if needed
        df, asset_value, _, _, return_value, return_rate = calculate_current_values(owner, return_totals=True)

        self.total_asset_value.configure(text=f"{asset_value:,}")
        self.total_return_value.configure(text=f"{return_value:,}")
        self.total_return_base_value.configure(text=f"{return_rate}%")

        self.pivoted_data = default_pivot(df, owner, save_results=False)
        self.plot_data = df

        # Update table section

        self.display_table_from_dataframe()

        # Update plot section

        self.display_selected_plot(owner)

    def display_selected_plot(self, owner):
        self.clear_frame(self.plot_frame)  # Clear any existing content in the frame

        if self.plot_choice.get() == 1:
            fig = plot_portfolio_percentage(self.plot_data)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current asset value of Portfolio: {owner}")
        elif self.plot_choice.get() == 2:
            portfolio_data, transactions_data = get_portfolio_over_time(owner)
            self.plot_data = portfolio_data
            selected_timeframe = self.timeframe_combobox.get()
            fig = plot_portfolio_over_time(portfolio_data, transactions_data, timeframe=selected_timeframe)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing asset value over time for Portfolio: {owner} ({selected_timeframe})")
        elif self.plot_choice.get() == 3:
            fig = plot_asset_value_by_account(self.plot_data, drill_down_profile=False)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current assets return rate by account: {owner}")
        elif self.plot_choice.get() == 4:
            fig = plot_asset_value_by_account(self.plot_data, drill_down_profile=True)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing current assets return rate by profile: {owner}")
        elif self.plot_choice.get() == 5:
            return_by_asset_data = calculate_return_rate_per_asset(owner, aggregation_column='PROFILE')
            self.plot_data = return_by_asset_data
            fig = plot_return_values(self.plot_data)
            canvas = FigureCanvasTkAgg(fig, master=self.plot_frame)  # Plot section
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.append_log(f"Drawing return by asset: {owner}")
        else:
            self.append_log("No plot selected")

    def clear_frame(self, output_frame):
        for widget in output_frame.winfo_children():
            widget.destroy()

    def display_table_from_dataframe(self):
        self.clear_frame(self.table_sub_frame)  # Clear any existing content in the frame
        df = self.pivoted_data
        tree = ttk.Treeview(self.table_sub_frame)
        tree.pack(expand=False)  # , fill='both')  # Expand the treeview to fill the frame
        column_headers = list(df.columns)
        tree["columns"] = column_headers
        tree.column("#0", width=0, stretch=tk.NO)  # Hiding the default column
        # tree.column("#1", width=100, stretch=tk.NO)
        # tree.column("#2", width=200, stretch=tk.NO)
        # tree.column("#3", width=100, stretch=tk.NO)
        # tree.column("#4", width=100, stretch=tk.NO)
        for col in column_headers:
            tree.column(col, anchor=tk.W)
            tree.heading(col, text=col, anchor=tk.W)
        for _, row in df.iterrows():
            tree.insert("", tk.END, values=tuple(row))
        tree.column('CURRENT_ASSET_VALUE', anchor='e')
        tree.column('PERCENTAGE', anchor='e')


def main():
    root = ctk.CTk()
    app = PortfolioManager(root)
    root.mainloop()


if __name__ == "__main__":
    main()
