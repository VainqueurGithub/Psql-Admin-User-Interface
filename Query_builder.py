import tkinter as tk
from tkinter import ttk, messagebox
import psycopg2

def connect_db():
    try:
        conn = psycopg2.connect(
            host="192.168.1.78",
            dbname="dianfossey",
            user="vainqueur",
            password="123"
        )
        return conn
    except Exception as e:
        messagebox.showerror("Connection Error", str(e))
        return None

def load_schemas():
    conn = connect_db()
    if not conn: return
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata;")
    schemas = [row[0] for row in cur.fetchall()]
    schema_combo['values'] = schemas
    cur.close()
    conn.close()

def load_tables(event=None):
    conn = connect_db()
    if not conn: return
    cur = conn.cursor()
    schema = schema_var.get()
    cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", (schema,))
    tables = [row[0] for row in cur.fetchall()]
    table_combo['values'] = tables
    cur.close()
    conn.close()

def run_query():
    conn = connect_db()
    if not conn: return
    cur = conn.cursor()
    table = table_var.get()
    columns = "*"
    where = filter_entry.get()
    sql = f"SELECT {columns} FROM {schema_var.get()}.{table}"
    if where.strip():
        sql += f" WHERE {where}"
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result_table.delete(*result_table.get_children())
        result_table["columns"] = columns
        for col in columns:
            result_table.heading(col, text=col)
            result_table.column(col, width=100)
        for row in rows:
            result_table.insert("", "end", values=row)
    except Exception as e:
        messagebox.showerror("Query Error", str(e))
    finally:
        cur.close()
        conn.close()

# GUI Setup
root = tk.Tk()
root.title("PostgreSQL Query Builder")

schema_var = tk.StringVar()
table_var = tk.StringVar()

tk.Button(root, text="Load Schemas", command=load_schemas).pack()

schema_combo = ttk.Combobox(root, textvariable=schema_var)
schema_combo.bind("<<ComboboxSelected>>", load_tables)
schema_combo.pack()

table_combo = ttk.Combobox(root, textvariable=table_var)
table_combo.pack()

tk.Label(root, text="WHERE clause (e.g., age > 30):").pack()
filter_entry = tk.Entry(root)
filter_entry.pack()

tk.Button(root, text="Run Query", command=run_query).pack()

result_table = ttk.Treeview(root, show='headings')
result_table.pack(fill=tk.BOTH, expand=True)

# Scrollbars
vsb = ttk.Scrollbar(root, orient="vertical", command=result_table.yview)
vsb.pack(side='right', fill='y')
result_table.configure(yscrollcommand=vsb.set)

root.mainloop()
