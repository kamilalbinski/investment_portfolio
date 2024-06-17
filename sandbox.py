from manage_calculations import *
from views.custom_views import *
from visualization.dynamic_plots import *
from manage_database_functions import *
from manage_pipeline_functions import *
from utils.database_setup import get_temporary_owners_list

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import datetime

refresh_market()
#refresh_fx()