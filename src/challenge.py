"""
    This is the main program.
    When executed:
    - Tries to request source .csv files from the urls specified in .env file, and saves them in
        different folders following a structure.
    - Process, normalize and transform the data from those .csv files into DataFrames.
    - Creates and connect to the database specified in the .env file.
    - Create tables in database from the DataFrames, and generate the .sql file associated.
    - Update the tables values, with the DataFrames data.
"""

#Imports
#Data manipulation
import pandas as pd
import numpy as np
import csv
import datetime as dt
#Requests
import requests
from pathlib import Path
import os
#Database connection
import psql
#Logging module
import logger
#Settings module
import settings

#Disable the SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

#Current working directory
cwd = os.getcwd()
#Get current datetime
now = dt.datetime.now()
#Get default .sql file path
default_sql_path = settings.default_sql_path

#CSV urls
url_museos = settings.url_museos
url_salas_cine = settings.url_salas_cine
url_bibliotecas = settings.url_bibliotecas

#Sources info
sources = {
    "museos" : url_museos,
    "salas_cine" : url_salas_cine,
    "bibliotecas" : url_bibliotecas
}

#Processed data
processed_data = {}

#Methods
def save_csv_from_url(url, file_path):
    """ Get a csv file from a given url and saves it to a local path

    Args:
        url (string): Url from where the csv file can be downloaded
        file_path (string): File path to save the csv file locally
    """
    
    #Ensure the directory(folder) exists, if not, it creates it
    folder_path = file_path.split('\\')[0:-1]
    folder_path = ('\\').join(folder_path)
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    #Make the request
    with requests.get(url, stream=True) as r:
        #Get the decoded lines of the csv file
        lines = (line.decode('utf-8') for line in r.iter_lines())
    
        #Create (or overwrite if exists) the file in the file_path path
        with open(file_path, 'w', encoding='UTF8', newline='') as f:
            #Write each row in the file
            writer = csv.writer(f)
            writer.writerows(csv.reader(lines))

def get_month_name(month_number):
    """ Given a month number, return the month name in spanish

    Args:
        month_number (int): Month number, from 1 to 12

    Returns:
        string: Month name in spanish
    """
    #Dict to map
    month_dict = {  1 : "Enero",
                    2 : "Febrero",
                    3 : "Marzo",
                    4 : "Abril",
                    5 : "Mayo", 
                    6 : "Junio",
                    7 : "Julio",
                    8 : "Agosto",
                    9 : "Septiembre",
                    10 : "Octubre",
                    11 : "Noviembre",
                    12 : "Diciembre"}
    return month_dict[month_number]

def get_source_files(sources):
    """Try to make requests to urls from sources dictionary, download those files, and save it
    locally following a structure.

    Args:
        sources (dict): Dictionary with source names as keys (used to build the filename)
            and urls as values.

    Returns:
        bool: True if no exceptions occurred. False otherwise.
    """
    #Iterate the sources
    for name, url in sources.items():
        #File path structure
        file_path = cwd + '\{data_path}\{name}\{year}-{month_name}\{name}-{day}-{month}-{year}.csv'.format(
            data_path=settings.default_data_path, name=name, year=now.year,
            month_name=get_month_name(now.month).lower(), day=now.day, month=now.month
        )
        #Catch possible exceptions making directories, requesting urls or writing csv's
        try:
            #Save the csv file in that directory
            save_csv_from_url(url, file_path)
            #Log
            logger.log('INFO', f'Created "{file_path}" successfully')
        #Log possible exceptions
        except requests.exceptions.InvalidSchema as e:
            logger.log('ERROR', f'Failed to request url "{url}"', str(e))
            return False
        except Exception as e:
            logger.log('ERROR', f'Failed to create "{file_path}"', str(e))
            return False
    #If no exceptions
    return True

def process_data():
    """Look for files in directories following a structure, processing the data from those files,
    normalizing them and making it dataframes ready to be transformed into SQL tables.

    Returns:
        dict: Dictionary containing the processed and normalized dataframes as values,
            and its names (future table names) as keys.
    """
    #Create dataframes
    dataframes = []
    for name in sources.keys():
        file_path = '{data_path}\{name}\{year}-{month_name}\{name}-{day}-{month}-{year}.csv'.format(
            data_path=settings.default_data_path, name=name, year=now.year,
            month_name=get_month_name(now.month).lower(), day=now.day, month=now.month
        )
        dataframes.append(pd.read_csv(file_path))
    #Temporal references to dataframes
    df_museos = dataframes[0]
    df_cines = dataframes[1]
    df_bibliotecas = dataframes[2]

    #Create an unique espacios_culturales dataframe by filtering and normalizing data from the 3 csv
    #Filter columns
    museos_filter_columns = [
        "Cod_Loc", "IdProvincia", "IdDepartamento", "categoria", "provincia", "localidad",
        "nombre", "direccion", "CP", "cod_area", "telefono", "Mail", "Web", "fuente"]
    cines_filter_columns = [
        "Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad",
        "Nombre", "Dirección", "CP", "cod_area", "Teléfono", "Mail", "Web", "Fuente"]
    bibliotecas_filter_columns = [
        "Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad",
        "Nombre", "Domicilio", "CP", "Cod_tel", "Teléfono", "Mail", "Web", "Fuente"]
    #Filtered dataframes
    filtered_dataframes = [
        df_museos[museos_filter_columns],
        df_cines[cines_filter_columns],
        df_bibliotecas[bibliotecas_filter_columns]]
    #Iterate each dataframe
    for df in filtered_dataframes:
        #Rename the columns with appropiate names
        df.columns = [
            "id_localidad", "id_provincia", "id_departamento", "categoria", "provincia",
            "localidad", "nombre", "domicilio", "cp", "cod_area", "telefono", "mail", "web", "fuente"
        ]
        #Set correct column types
        #Numeric columns
        for col in df.columns[0:3]:
            df[col] = pd.to_numeric(df[col])
        #String columns
        for col in df.columns[3:-1]:
            df[col] = df[col].astype(str)
        #Delete decimals on not null cod_area values
        df["cod_area"] = df["cod_area"].apply(lambda x : x.split('.')[0] if pd.notna(x) else x)
        #Replace "s/d" and "nan" by null values
        for col in ["cod_area", "telefono", "mail", "web"]:
            df[col] = df[col].apply(lambda x : np.NaN if x in ['s/d', 'nan'] else x)
        #Modify the column "telefono" to include "cod_area"
        df["telefono"] = '(' + df["cod_area"] + ') ' + df["telefono"]
        #Drop "cod_area" column
        df.drop("cod_area", axis=1, inplace=True)
    
    #Normalize and rename some columns from 'cines' dataframe
    df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].map({'Si' : True, 'SI' : True}, na_action='ignore')
    df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].replace(np.nan, False)
    #Include relevante columns from 'cines' dataframe into the filtered dataframe
    filtered_dataframes[1][["pantallas", "butacas", "espacio_incaa"]] = \
        df_cines[["Pantallas", "Butacas", "espacio_INCAA"]]
    
    #Concatenate the dataframes into one unique dataframe called "espacios_culturales"
    df_espacios_culturales = pd.concat(filtered_dataframes, ignore_index=True)
    df_espacios_culturales.reset_index(inplace=True)
    df_espacios_culturales.rename(columns={"index" : "id_espacio_cultural"}, inplace=True)
    #Set correct dtypes (Int64 is like built-in int but can store null values)
    df_espacios_culturales["pantallas"] = df_espacios_culturales["pantallas"].astype('Int64')
    df_espacios_culturales["butacas"] = df_espacios_culturales["butacas"].astype('Int64')
    df_espacios_culturales["espacio_incaa"] = df_espacios_culturales["espacio_incaa"].astype(bool)

    #Build provinces dataframe
    df_provincias = df_espacios_culturales.groupby("id_provincia", as_index=False)["provincia"].max()
    #Build localidades dataframe
    df_localidades = df_espacios_culturales.groupby("id_localidad", as_index=False)["localidad"].max()
    #Build categorias dataframe
    df_categorias = pd.DataFrame(df_espacios_culturales["categoria"].unique()).reset_index()
    df_categorias.columns = ["id_categoria", "categoria"]
    #Normalize 'categoria' column into 'id_categoria'
    df_espacios_culturales["id_categoria"] = df_espacios_culturales["categoria"].apply(
        lambda x : df_categorias[df_categorias.categoria == x].id_categoria.values[0]
    )
    #Build fuentes dataframe
    df_fuentes = pd.DataFrame(df_espacios_culturales["fuente"].unique()).reset_index()
    df_fuentes.columns = ["id_fuente", "fuente"]
    #Normalize 'fuente' column into 'id_fuente'
    df_espacios_culturales["id_fuente"] = df_espacios_culturales["fuente"].apply(
        lambda x : df_fuentes[df_fuentes.fuente == x].id_fuente.values[0]
    )
    #Drop redundant columns on "espacios_culturales" dataframe
    df_espacios_culturales.drop(["provincia", "localidad", "categoria", "fuente"], axis=1, inplace=True)

    #Group processed data
    #('espacios_culturales' at the end intentionally, this way will create that table last,
    # avoiding foreign key errors)
    data = {
        "provincias" : df_provincias,
        "localidades" : df_localidades,
        "categorias" : df_categorias,
        "fuentes" : df_fuentes,
        "espacios_culturales" : df_espacios_culturales
    }
    #Add 'fecha_carga' column to each dataframe
    for df in data.values():
        df['fecha_carga'] = pd.to_datetime(dt.datetime.now())
    #Return the processed data
    return data

def set_up_database(data):
    """Ask the psql module for a database engine. If get it, it will create a "tables.sql" file 
    (in the current directory) and then ask the psql module to execute that file.
    This will create the tables in the database from the 'data' argument dataframes.

    Args:
        data (dict): Dictionary used to create database tables.
            Containing table names as keys, and DataFrames as values.

    Returns:
        Engine: The database engine. If None, it means the psql module could not
            create/connect to the database.
    """
    #Create database if not exists
    db = psql.get_database()
    #If created succesfully
    if db:
        #Save sql script
        save_sql_tables(default_sql_path, data)
        #Catch possible exceptions
        try:
            #Create the tables executing that file
            psql.execute_from_sql(default_sql_path)
            #Log
            logger.log('INFO', f'SQL script "{default_sql_path}" executed successfully!')
        except Exception as e:
            logger.log('ERROR', f'Failed to execute SQL script from: "{default_sql_path}"', str(e))
    #Return the engine
    return db

def save_sql_tables(file_path, data):
    """Save a .sql file in a given path, that generates SQL tables from 'data' argument.

    Args:
        file_path (str): Path to save the .sql file
        data (dict): Dictionary used to create database tables.
            Containing table names as keys, and DataFrames as values.
    """
    # Write a .sql file that will generate the database tables
    with open(file_path, 'w') as file:
                
        #Dicts to store primary and foreign keys
        primary_keys = {}
        foreign_keys = {}

        #Iterate through processed dataframes
        for df_name, df in data.items():
            #Dictionary to map dtypes
            dtypes_dict = psql.get_dtypes_dict(df)
            #Start the create table query
            file.write("CREATE TABLE IF NOT EXISTS {table_name} ( \n".format(table_name=df_name))
            
            #Iterate through the dataframe columns
            for idx, col_name in enumerate(df.columns):
                #Detect and store primary and foreign keys with its tables
                if idx == 0:
                    primary_keys.update({col_name : df_name})
                elif col_name.startswith('id_'):
                    foreign_keys.update({col_name : df_name})

            #Iterate through the dataframe columns
            for idx, col_name in enumerate(df.columns):
                #Get the correct type
                col_type = dtypes_dict[col_name]
                #Add the column with its correct type
                file.write("\t{col_name} {col_type}".format(\
                    col_name=col_name, col_type=col_type))
                #If it's the first column
                if idx == 0:
                    #Set as primary key
                    file.write(" PRIMARY KEY")
                #If its not the last column
                if idx != len(df.columns)-1:
                    #Put a comma and new line
                    file.write(",\n")
                #If is the last column
                else:
                    #close the statement
                    file.write("\n);\n\n")
        
        #ADD FOREIGN KEY CONSTRAINTS
        #Exclude 'id_departamento' from foreign keys
        #because we dont have a 'departamentos' table
        del foreign_keys['id_departamento']

        for fk, fk_table in foreign_keys.items():
            #Look for the column its referencing
            pk = fk
            pk_table = primary_keys[pk]
            #Drop the constraint if it exists
            file.write('ALTER TABLE {fk_table} DROP CONSTRAINT IF EXISTS fk_{fk};\n'.format( \
                    fk_table=fk_table, fk=fk))
            #Add the constraint to the column
            file.write('ALTER TABLE {fk_table} ADD CONSTRAINT fk_{fk} FOREIGN KEY ({fk}) REFERENCES {pk_table}({pk});\n'.format( \
                    fk_table=fk_table, fk=fk, pk_table=pk_table, pk=pk))
    
    #Log
    logger.log('INFO', f'SQL script for tables creation saved on: "{file_path}"')
    
def update_database(db, data):
    """Try to connect to a given database. If it can, look for tables named as 'data' argument keys,
    and replace all its values with the ones in its corresponding DataFrames ('data' argument values).
    Note that this method does not alter the tables structure, only its values.

    Args:
        db (Engine): Database engine to connect.
        data (dict): Dictionary used to identify database tables and update its values.
            Containing table names as keys, and DataFrames as values.
    """
    #Connect to db
    with db.connect() as con:
        try:
            #Iterate through processed dataframes
            for df_name, df in data.items():
                #Truncate the table
                con.execute('TRUNCATE {table} CASCADE;'.format(table=df_name))
                #Fill the table with its values
                df.to_sql(df_name, con=con, if_exists="append", index=False)
                #Log
                logger.log('INFO', f'Table "{df_name}" updated in database successfully.')
        except Exception as e:
            #Log the exception
            logger.log('ERROR', f'Can\'t update table "{df_name}" in database.', e._message)


#--MAIN--

#Logging message
logger.log('INFO', '--- STARTING PROGRAM ---')

#If can get the data
if get_source_files(sources):
    #Process the data
    processed_data = process_data()
#Database set up and table creation
db = set_up_database(processed_data)
#Update database
if db:
    update_database(db, processed_data)

#Logging message
logger.log('INFO', '---- ENDING PROGRAM ----')