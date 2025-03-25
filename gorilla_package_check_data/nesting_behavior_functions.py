import csv
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from tkinter import messagebox

def nest_create_engine(database,user,password,host,port):
    try:
        # Attempt to connect to the database
        engine = create_engine('postgresql+psycopg2://'+user+':'+password+'@'+host+':'+port+'/'+database)
        return engine
    except OperationalError as e:
        # Handle connection failure
        if "password authentication failed" in str(e):
            messagebox.showerror("OperationalError:", f"Password authentication failed for user '{user}'.Please check your username and password.")
        else:
            messagebox.showerror("OperationalError:", e)

        # Optional: retry logic or prompt for a new password here
        return None

def nest_connect_to_db(database,user,password,host,port):
    try:
        # Attempt to connect to the database
        connection = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,  # Replace this with the actual password
            host=host,
            port=port
        )
        messagebox.showinfo("Confirmation", "Connection successful!")
        return connection
    except OperationalError as e:
        # Handle connection failure
        if "password authentication failed" in str(e):
            messagebox.showerror("OperationalError:" f"Password authentication failed for user '{user}'. Please check your username and password.")
        else:
            messagebox.showerror("OperationalError:", e)

        # Optional: retry logic or prompt for a new password here
        return None


def nest_read_csv(source_raw):
    # Open the file and read the first line to detect the delimiter
    with open(source_raw+'/data_fail_nesting.csv', "r") as csvfile:
        # Read the file's content
        sample = csvfile.readline()
        
        # Use Sniffer to detect the delimiter
        dialect = csv.Sniffer().sniff(sample)
        delimiter = dialect.delimiter
    
    # Read the data to pandas dataframe by assigning the correct delimiter

    if delimiter==',':
        data = pd.read_csv(source_raw+'/data_fail_nesting.csv', sep = ',', encoding = 'latin1')
    elif delimiter==';':
        data = pd.read_csv(source_raw+'/data_fail_nesting.csv', sep = ';', encoding = 'latin1')
    return data

def raw_nesting_data_checking(data):
    
    ''' This function check the validity of data from csv file. Most importantly the date format and nombre column'''

    # Formating date_surveillance column to '%d/%m/%Y' format
    try:
        # Try parsing with the first format
        data['date_surveillance'] = pd.to_datetime(data.date_surveillance)
        messagebox.showinfo("Confirmation","date parsed succeffully")
    except ValueError:
        # Provide a helpful message and suggest the correct format
        messagebox.showerror("Error Message",f"Make sure date_surveillance '{data['date_surveillance']}' column is parsed to this format '%d/%m/%Y'.")
    
        
    #Cleaning nombre column     
        
    try:
        # Try to convert the value directly to an float
        data['nombre'] = data['nombre'].fillna(0) # Assign 0 to all nan value in this column
        data['nombre'] = data['nombre'].astype(int) # Convert this column to int 
        data['nombre'] = data['nombre'].astype(str) # Convert this column to string
        messagebox.showinfo("nombre Converted succeffully")
    except ValueError:
        messagebox.showerror("Error Message", f"Make sure '{data['nombre']}'is numeric characters.")
    
    return data
        
def nest_retrieve_data_psql(database,user,password,host,port):
    
    '''This function connect python to psql and retrieve data from psql'''
    #establishing the connection
    try:
        conn = nest_connect_to_db(database=database, user=user, password=password, host=host, port=port)
        #Setting auto commit false
        conn.autocommit = True
        #Creating a cursor object using the cursor() method
        cursor_category_materiel = conn.cursor()
        cursor_famille_gorille = conn.cursor()
        cursor_habitat = conn.cursor()
        cursor_nid_id = conn.cursor()
        cursor_sexe_age = conn.cursor()
        cursor_type_nid = conn.cursor()
    
        #Retrieving data
        cursor_sexe_age.execute('''SELECT sexe_age from prog_biodiversite.sexe_age''')
        cursor_nid_id.execute('''SELECT nid from prog_biodiversite.nid''')
        cursor_habitat.execute('''SELECT foret from prog_gorille.foret''')
        cursor_type_nid.execute('''SELECT type_nid from prog_biodiversite.type_nids''')
        cursor_category_materiel.execute('''SELECT categorie_materiel from prog_biodiversite.categorie_materiel''')
        cursor_famille_gorille.execute('''SELECT nom_famille from prog_gorille.famille_gorille''')
    
        #Fetching rows from the table
        sexe_ages = cursor_sexe_age.fetchall();
        nid_ids = cursor_nid_id.fetchall();
        habitats = cursor_habitat.fetchall();
        type_nids = cursor_type_nid.fetchall();
        category_materiels = cursor_category_materiel.fetchall();
        famille_gorilles = cursor_famille_gorille.fetchall();
    
        #Commit your changes in the database
        conn.commit()
        #Closing the connection
        conn.close()
    
        #Creating single dataframe for each cursor, and add nan value if neccessary
        df_sexe_age = pd.DataFrame(sexe_ages, columns=['sexe_age'])
        df_nid_id = pd.DataFrame(nid_ids, columns=['nid'])
        df_habitat = pd.DataFrame(habitats, columns=['habitat'])
        df_type_nid = pd.DataFrame(type_nids, columns=['type_nid'])
        df_category_materiel = pd.DataFrame(category_materiels, columns=['category_materiel'])
        df_famille_gorille = pd.DataFrame(famille_gorilles, columns=['famille_gorille'])
    
        return df_sexe_age, df_nid_id, df_habitat, df_type_nid, df_category_materiel, df_famille_gorille
    except AttributeError:
        df_sexe_age = pd.DataFrame(columns=['sexe_age'])
        df_nid_id = pd.DataFrame(columns=['nid_id'])
        df_habitat = pd.DataFrame(columns=['habitat'])
        df_type_nid = pd.DataFrame(columns=['type_nid'])
        df_category_materiel = pd.DataFrame(columns=['category_materiel'])
        df_famille_gorille = pd.DataFrame(columns=['famille_gorille'])
        return df_sexe_age, df_nid_id, df_habitat, df_type_nid, df_category_materiel, df_famille_gorille


def nest_checking_data_integrity(source_raw,source_checked,df_sexe_age,df_nid_id,df_habitat,df_type_nid,df_category_materiel,df_famille_gorille,data):
    ''' This function check data integrity before downloading the data into psql.'''
    
    data_success = data.loc[(data['sexe_age'].isin(df_sexe_age['sexe_age'])) & (data['nid_id'].isin(df_nid_id['nid_id'])) &
        (data['habitat'].isin(df_habitat['habitat'])) & (data['type_nid'].isin(df_type_nid['type_nid'])) & 
         (data['category_materiel'].isin(df_category_materiel['category_materiel'])) &
         (data['famille_gorille'].isin(df_famille_gorille['famille_gorille']))]
    data_success
    
    
    
    data_fail = data.loc[(~data['sexe_age'].isin(df_sexe_age['sexe_age'])) | (~data['nid_id'].isin(df_nid_id['nid_id'])) |
        (~data['habitat'].isin(df_habitat['habitat'])) | (~data['type_nid'].isin(df_type_nid['type_nid'])) | 
         (~data['category_materiel'].isin(df_category_materiel['category_materiel'])) | 
         (~data['famille_gorille'].isin(df_famille_gorille['famille_gorille']))]

    
    # Check if the data_success_surveillance CSV file exists
    if os.path.exists(source_checked+'/data_success_nesting.csv'):
                      # Read the existing CSV file
                      existing_df = pd.read_csv(source_checked+'/data_success_nesting.csv')
                      # Merge the new DataFrame with the existing DataFrame
                      combined_df = pd.concat([existing_df, data_success], ignore_index=True)
    else:
        # If the file does not exist, use the new DataFrame as the combined DataFrame
        combined_df = data_success

    
    try:
        data_fail.to_csv(source_raw+'/data_fail_nesting.csv', index=False)
        # Write the combined DataFrame back to the CSV file
        combined_df.to_csv(source_checked+'/data_success_nesting.csv', index=False)
        
        if len(data_fail)==0:
            messagebox.showinfo("Success Massage",'ALL YOUR DATA IS VALIDATED, READY TO BE INTEGRETED INTO PSQL')
        
        else:
            messagebox.showwarning("Warning Message",f"YOU STILL HAVE SOME DATA TO VALIDATE, '{len(data_fail)}' raw seem to have issues check your data_fail_surveillance.csv file.")
        
    except PermissionError:
        messagebox.showerror("Error Message","The script fail to write data on the file, make sure both data_fail_surveillance and data_success_surveillance csv files are not opened")
    
            
    #return message
def clear_csv_but_keep_headers(file_path):
    #Deletes all data lines but keeps the header in a CSV file.
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader, None) # Read the header

    if headers:
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers) # Write only the header back    

def nest_data_downloading_psql(source_checked, user,password,host,port,database):
    
    engine = nest_create_engine(database,user,password,host,port)

    try:
        data_success = pd.read_csv(source_checked+'/data_success_nesting.csv', encoding = 'latin1')
        data_success.to_sql('nidification', engine, schema='prog_biodiversite',if_exists='append', index=False)
        clear_csv_but_keep_headers(source_checked)
        messagebox.showinfo("Confirmation",f"{len(data_success)} data, successfully loaded into psql server")
    except Exception as e:
        messagebox.showerror("Error Importing data into psql server",str(e))


    
