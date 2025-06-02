import csv
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox, filedialog
import pandas as pd
import matplotlib
matplotlib.use("TkAgg") # for backend
import seaborn as sns
sns.set_theme(style="darkgrid")
import psycopg2
import tkinter as tk
from tkinter import messagebox
from gorilla_package_check_data import raw_tracking_data_checking,tr_checking_data_integrity,tr_data_downloading_psql,tr_read_csv,tr_create_engine,tr_connect_to_db,tr_retrieve_data_psql
from gorilla_package_check_data import raw_monitoring_data_checking,mon_checking_data_integrity,mon_data_downloading_psql,mon_read_csv,mon_create_engine,mon_connect_to_db,mon_retrieve_data_psql
from gorilla_package_check_data import raw_delimitation_data_checking,del_checking_data_integrity,del_data_downloading_psql,del_read_csv,del_create_engine,del_connect_to_db,del_retrieve_data_psql
from gorilla_package_check_data import raw_nesting_data_checking,nest_checking_data_integrity,nest_data_downloading_psql,nest_read_csv,nest_create_engine,nest_connect_to_db,nest_retrieve_data_psql
import csv
import psycopg2
import pandas as pd
import os

# Database connection details
def connect_db():

    DB_PARAMS = {
    "dbname": database_entry.get(),
    "user": user_entry.get(),
    "password": password_entry.get(),
    "host": host_entry.get(),
    "port": "5432"
    }

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        populate_table(conn)
        return conn
    except Exception as e:
        messagebox.showerror("Database Connection Fail", str(e))


def fetch_schemas(conn):
    """Fetch all schema names from the PostgreSQL database."""
    try:
       
        cur = conn.cursor()
        
        # Query to get schemas
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cur.fetchall()
        
        # Close connection
        #cur.close()
        #conn.close()
        
        return [schema[0] for schema in schemas]

    except Exception as e:
        print("Error:", e)
        return []

def fetch_tables(schema_name):
    """Fetch table names for a selected schema."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = %s;
        """, (schema_name,))
        
        tables = cursor.fetchall()
        
        #cursor.close()
        #conn.close()
        return [table[0] for table in tables]
    
    except Exception as e:
        print("Error:", e)
        return []

def fetch_records(schema_name, table_name, filter_col=None, filter_value=None):
    """Fetch first 1000 records from a selected table."""
    try:
        conn  = connect_db()
        cursor = conn.cursor()
        
        # Fetch first 1000 records
        query = f'SELECT * FROM "{schema_name}"."{table_name}"'
        params=()

        if filter_col and filter_value:
            query+=f' WHERE "{filter_col}"::text ILIKE %s'
            params = (f'%{filter_value}%',)

        query +=" LIMIT 100;"

        cursor.execute(query, params)
        records = cursor.fetchall()
        
        # Get column names
        col_names = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        return col_names, records
    
    except Exception as e:
        print("Error:", e)
        return [], []


def populate_table(conn):
    """Populate Tkinter Treeview with schema names."""
    schemas = fetch_schemas(conn)
    
    # Clear existing data
    for row in tree_schemas.get_children():
        tree_schemas.delete(row)

    # Insert new schema names
    for schema in schemas:
        tree_schemas.insert("", "end", values=(schema,))

def on_schema_click(event):
    """Fetch and display tables when a schema is clicked."""
    selected_item = tree_schemas.selection()
    if selected_item:
        schema_name = tree_schemas.item(selected_item, "values")[0]
        
        tables = fetch_tables(schema_name)
        
        # Clear old data
        for row in tree_tables.get_children():
            tree_tables.delete(row)
        
        dropdown['menu'].delete(0, 'end') # Clear current options
        dropdown_report['menu'].delete(0, 'end') # Clear current options
        # Insert new tables
        for table in tables:
            tree_tables.insert("", "end", values=(table,schema_name))
            dropdown['menu'].add_command(label=table, command=tk._setit(var, table))
            dropdown_report['menu'].add_command(label=table, command=tk._setit(var_report, table))
        var.set(tables[0])
        var_report.set(tables[0])

def on_table_click(event):
    """Fetch and display the first 1000 records when a table is clicked."""
    selected_item = tree_tables.selection()
    if selected_item:
        values = tree_tables.item(selected_item, "values")

        if len(values) == 2:
            schema_name, table_name = values
            open_table_record_window(table_name,schema_name)

def open_table_record_window(table_name,schema_name):

    # Open a child window
    table_record_window = tk.Toplevel(root)
    table_record_window.title(f"Records - {schema_name}")
    table_record_window.geometry("1400x720")
    
    col_names, records = fetch_records(table_name,schema_name)
            
    # Create a frame for the table records
    frame = tk.Frame(table_record_window)  
    frame.pack(fill="both", expand=True)

    tree_records = ttk.Treeview(frame, columns=col_names, show="headings")

    # Clear old records
        
    for col in col_names:
        max_width = max(len(col), 10) * 10
        tree_records.heading(col, text=col)
        tree_records.column(col, width=max_width, anchor="center", stretch=False)

    # Scrollbars
    vsb = ttk.Scrollbar(frame, orient="vertical", command=tree_records.yview)
    hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree_records.xview)
    tree_records.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    vsb.pack(side="right", fill="y")
    hsb.pack(side="bottom", fill="x")
    tree_records.pack(expand=True, fill="both")

            
    # Insert records
    for record in records:
        tree_records.insert("", "end", values=record)
    
    # Filtering section
    filter_frame = tk.Frame(table_record_window)
    filter_frame.pack(fill="x", padx=10, pady=5)

    tk.Label(filter_frame, text="Filter by Column:").pack(side="left")

    # Dropdown for column selection
    column_var = tk.StringVar()
    column_dropdown = ttk.Combobox(filter_frame, textvariable=column_var, values=col_names, state="readonly")
    column_dropdown.pack(side="left", padx=5)

    # Entry field for filter value
    filter_entry = tk.Entry(filter_frame)
    filter_entry.pack(side="left", padx=5)

    def apply_filter():
        """Apply filtering based on user input."""
        selected_col = column_var.get()
        filter_value = filter_entry.get()
        col_names, records = fetch_records(table_name,schema_name, selected_col, filter_value)

        # Clear existing records
        for row in tree_records.get_children():
            tree_records.delete(row)

        # Insert filtered records
        for record in records:
            tree_records.insert("", "end", values=record)
    
    def download_data_csv():
        """Exports Treeview data to CSV file."""
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])

        if not file_path: # In case the user cancels, do nothing
            return

        # Get all data from the Treeview
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write header (column names)
            columns = [tree_records.heading(col)["text"] for col in tree_records["columns"]]
            writer.writerow(columns)

            #Write data rows
            for row in tree_records.get_children():
                writer.writerow(tree_records.item(row)["values"])
        messagebox.showinfo("Successful Operation",f"Data exported successfully to {file_path}")

    def detect_delimiter(source_raw):
        # Open the file and read the first line to detect the delimiter
        with open(source_raw, 'r', newline='', encoding='utf-8') as file:
            # Read small chunk of the file
            sample = file.readline()
           
            # Use Sniffer to detect the delimiter
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
            #return delimiter
    
        # Read the data to pandas dataframe by assigning the correct delimiter

        if delimiter==',':
            data = pd.read_csv(source_raw, sep = ',', encoding = 'latin1')
        elif delimiter==';':
            data = pd.read_csv(source_raw, sep = ';', encoding = 'latin1')
        return data

    def view_csv_data():
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])

        if not file_path:
            return
    
        try:
            df = detect_delimiter(file_path)
            #print(delimiter)
            #df = pd.read_csv(file_path, delimiter=delimiter)
            # Open child window for records
            restore_data_window = tk.Toplevel(table_record_window)
            restore_data_window.title(f"CSV Records - {file_path}")
            restore_data_window.geometry("900x500")

            # Frame for Table
            frame = tk.Frame(restore_data_window)
            frame.pack(fill="both", expand=True)

            # Scrollbars
            tree_scroll_y = ttk.Scrollbar(frame, orient="vertical")
            tree_scroll_x = ttk.Scrollbar(frame, orient="horizontal")

            # Treeview Table
            tree = ttk.Treeview(frame, yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
            tree_scroll_y.configure(command=tree.yview)
            tree_scroll_x.configure(command=tree.xview)
        
            # Set Colums
            tree["columns"] = list(df.columns)
            tree["show"] = "headings"

            # Add column headers
            df.columns = [str(col).strip() for col in df.columns]
            
            for col in df.columns:
                tree.heading(col, text=col)
                tree.column(col, width=150)
            # Insert rows into the table
            for _, row in df.iterrows():
                tree.insert("", "end", values=list(row))
            
            # Pack widgets
            tree_scroll_y.pack(side="right", fill="y")
            tree_scroll_x.pack(side="bottom", fill="x")
            tree.pack(fill="both", expand=True)

        except Exception as e:
            messagebox.showerror("Error Loading CSV file", str(e))
     

    ttk.Button(filter_frame, text="Filter", command=apply_filter).pack(side="left", padx=5)
    ttk.Button(filter_frame, text="View CSV data", command=view_csv_data).pack(side="right", padx=7)
    ttk.Button(filter_frame, text="Download data", command=download_data_csv).pack(side="right", padx=8)


def import_data():
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])

    if not file_path:
        return
    try:
        source_raw = file_path # data source where csv file containing gorilla monitoring raw data is stored

        source_checked = source_raw.split('raw_data')

        if var.get()=='pistage':
            source_checked = source_checked[0]+"checked_data/tracking" # data source where csv file containing gorilla monitoring checked data is stored
        elif var.get()=='surveillance':
            source_checked = source_checked[0]+"checked_data/monitoring"
        elif var.get()=='delimitation':
            source_checked = source_checked[0]+"checked_data/delimitation"
        # Open the file and read the first line to detect the delimiter
        with open(source_raw, "r") as csvfile:
            # Read the file's content
            sample = csvfile.readline()
    
            # Use Sniffer to detect the delimiter
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter

        # Read the data to pandas dataframe by assigning the correct delimiter

        if delimiter==',':
            data = pd.read_csv(source_raw, sep = ',', encoding = 'latin1')
        elif delimiter==';':
            data = pd.read_csv(source_raw, sep = ';', encoding = 'latin1')

        if var.get()=='pistage':
            data =raw_tracking_data_checking(data)
            df_espece, df_signe, df_nombre, df_foret, df_age, df_chef_equipe, df_type, df_partie_consomme, df_famille_gorille, df_plante_consommee = tr_retrieve_data_psql(database_entry.get(),user_entry.get(),password_entry.get(),host_entry.get(),5432)
            tr_checking_data_integrity(source_raw,source_checked,df_espece,df_signe,df_nombre,df_foret,df_age,df_chef_equipe,df_type,df_partie_consomme,df_famille_gorille,df_plante_consommee,data)
        elif var.get()=='surveillance':
            data =raw_monitoring_data_checking(data)
            df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe = mon_retrieve_data_psql(database_entry.get(),user_entry.get(),password_entry.get(),host_entry.get(),5432)
            mon_checking_data_integrity(source_raw,source_checked,df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe,data)

        elif var.get()=='delimitation':
            data =raw_delimitation_data_checking(data)
            df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe = del_retrieve_data_psql(database_entry.get(),user_entry.get(),password_entry.get(),host_entry.get(),5432)
            del_checking_data_integrity(source_raw,source_checked,df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe,data)

        file_paths = [os.path.join(source_checked, f) for f in os.listdir(source_checked) if os.path.isfile(os.path.join(source_checked,f))]

        listbox.delete(0, tk.END) # Clear Existing listbox items

        if file_paths:
            for file in file_paths:
                listbox.insert(tk.END, file) # Insert each file path into the listbox
        else:
            listbox.insert(tk.END, "No files ready to imported found, in source_checked directory") # Insert each file path into the listbox

    except Exception as e:
        messagebox.showerror("Error Loading CSV file", str(e))

def on_list_double_click(event):
    selected_index = listbox.curselection()
    if selected_index:
        file_path = listbox.get(selected_index)
        df = pd.read_csv(file_path)
        line_count = len(df)
        
        # Ask for confirmation before loading the data in psql
        confirm = messagebox.askyesno("Load Data", f"Do you want to laod the data contenaining in this file into psql server ? This file contain {line_count} data ready to be imported.")
        if confirm:
            
            if var.get()=='pistage':
                tr_data_downloading_psql(file_path, user_entry.get(),password_entry.get(),host_entry.get(),'5432',database_entry.get())
            elif var.get()=='surveillance':
                mon_data_downloading_psql(file_path, user_entry.get(),password_entry.get(),host_entry.get(),'5432',database_entry.get())
            elif var.get()=='delimitation':
                del_data_downloading_psql(file_path, user_entry.get(),password_entry.get(),host_entry.get(),'5432',database_entry.get())
# root window
root = tk.Tk()
root.geometry("1600x820")
root.title('Database administration interface')
#root.resizable(0, 0)

# configure the grid
root.columnconfigure(0, weight=1)
root.columnconfigure(1, weight=3)
root.columnconfigure(2, weight=1)


# Label to display the status

database_label = ttk.Label(root, text="Database:")
database_label.place(relx=0.03, rely=0.03)

database_entry = ttk.Entry(root, width=15)
database_entry.place(relx=0.085, rely=0.03)
database_entry.insert(0, 'dianfossey')

user_label = ttk.Label(root, text="User:")
user_label.place(relx=0.18, rely=0.03)

user_entry = ttk.Entry(root, width=15)
user_entry.place(relx=0.21, rely=0.03)
user_entry.insert(0, 'user')


password_label = ttk.Label(root, text="Password:")
password_label.place(relx=0.31, rely=0.03)

password_entry = ttk.Entry(root, show='*', width=15)
password_entry.place(relx=0.37, rely=0.03)
password_entry.insert(0, 'fgfgfhdhfhjdjd')

host_label = ttk.Label(root, text="Host:")
host_label.place(relx=0.47, rely=0.03)

host_entry = ttk.Entry(root, width=15)
host_entry.place(relx=0.5, rely=0.03)
host_entry.insert(0, 'localhost')


mergin_button = ttk.Button(root, text="Connect to server",width=20, command=connect_db)
mergin_button.place(relx=0.6, rely=0.03)


# Separator object
separator = ttk.Separator(root, orient='horizontal')
separator.place(relx=0, rely=0.1, relwidth=1, relheight=1)


# Create Treeview widget
columns = ("Schema Name",)
tree_schemas = ttk.Treeview(root, columns=columns, show="headings")
tree_schemas.heading("Schema Name", text="Schema Name")

# Pack the treeview
tree_schemas.place(relx=0, rely=0.1,relheight=1)
# Bind click event
tree_schemas.bind("<<TreeviewSelect>>", on_schema_click)

tree_tables = ttk.Treeview(root, columns=("Table Name",), show="headings")
tree_tables.heading("Table Name", text="Table Name")
tree_tables.place(relx=0.18, rely=0.1,relheight=1)
# Bind click event
tree_tables.bind("<<TreeviewSelect>>", on_table_click)

var = tk.StringVar(root)
var_report = tk.StringVar(root)
label_importation = ttk.Label(root, text="Data importation")

# Separator object
separator = ttk.Separator(root, orient='horizontal')
separator.place(relx=0.4, rely=0.18, relwidth=1, relheight=1)

label_importation.place(relx=0.4, rely=0.15)
var.set("Choose table") # Default value
#options = ["Option 1", "Option 2", "Option 3"]
dropdown = ttk.OptionMenu(root, var, [])
dropdown.place(relx=0.4, rely=0.2)

button = ttk.Button(root, text="Import Data", command=import_data)
button.place(relx=0.5, rely=0.2)

# Displayed the checked data file ready to be imported in pg
listbox = tk.Listbox(root, width=100, height=100)
listbox.place(relx=0.7, rely=0.2)
listbox.bind("<Double-Button-1>", on_list_double_click)


label_reporting = ttk.Label(root, text="Data Reporting")
label_reporting.place(relx=0.4, rely=0.37)
# Separator object
separator = ttk.Separator(root, orient='horizontal')
separator.place(relx=0.4, rely=0.4, relwidth=1, relheight=1)

var_report.set("Choose table") # Default value
#options = ["Option 1", "Option 2", "Option 3"]
dropdown_report = ttk.OptionMenu(root, var_report, [])
dropdown_report.place(relx=0.4, rely=0.45)

button_report = ttk.Button(root, text="Reporting", command=import_data)
button_report.place(relx=0.5, rely=0.45)

root.mainloop()





