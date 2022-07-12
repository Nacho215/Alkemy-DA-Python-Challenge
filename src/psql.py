"""
    This module manages the connection with the PostgreSQL database.
    It has methods for creating the database and user specified
    in the .env settings file.
    It also can execute a .SQL file.
"""

#Imports
#Database conection
from sqlalchemy import create_engine, text, types
from sqlalchemy_utils import database_exists, create_database
#Settings module
import settings
#Logger module
import logger

#Database engine reference
db = None

#Methods
def get_database():
    """ Connects to database.

    Returns:
        engine (Engine): Database engine or None if failed to connect.
    """
    #Try to connect to database, if cant, raise an exception. Log both cases
    try:
        engine = get_engine_from_settings()
        logger.log('INFO', f'Connected to PostgreSQL database: "{settings.pg_database}"')
    except Exception as e:
        logger.log('ERROR', f'Failed to connect to "{settings.pg_database}"', str(e))
        return None
    #Remember the engine
    global db
    db = engine
    #Return the engine
    return engine

def get_engine_from_settings():
    """
    Sets up database connection from settings.py
    Returns:
        Call to get_database returning engine
    """
    return get_engine(settings.pg_user,
                      settings.pg_password,
                      settings.pg_host,
                      settings.pg_port,
                      settings.pg_database)

def get_engine(user, passwd, host, port, db):
    """
    Get SQLalchemy engine using credentials.
    Args:
        db: database name
        user: Username
        host: Hostname of the database server
        port: Port number
        passwd: Password for the database
    Returns:
        Database engine
    """
    #Get the url
    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    #Create the engine
    engine = create_engine(url, pool_size=50, echo=False)
    #Look for existing database 
    if database_exists(url):
        logger.log('INFO', f'Database "{db}" found!')
    #If not exists
    else:
        #Creates the database
        create_database(url, encoding='utf8')
        logger.log('INFO', f'Created database: "{db}"')
        #Grant user all privileges on that database
        engine.execute(f'GRANT ALL PRIVILEGES ON DATABASE {db} TO {user};')
        logger.log('INFO', f'Granted all privileges on "{db}" to "{user}"')
    #Return engine
    return engine

def get_dtypes_dict(df):
    """Given a dataframe, generate a dictionary used to map the columns to
    its correct SQL types.

    Args:
        df (DataFrame): The dataframe whose columns we want to map

    Returns:
        dict: A dictionary with 'df' columns as keys, and its SQL types as values
    """
    #Initialize empty dict
    dtypes_dict = {}
    #Update its values
    for col_name, col_type in zip(df.columns, df.dtypes):
        if "object" in str(col_type):
            dtypes_dict.update({col_name: types.VARCHAR(length=255)})
                                 
        if "datetime" in str(col_type):
            dtypes_dict.update({col_name: 'TIMESTAMP WITHOUT TIME ZONE'})

        if "float" in str(col_type):
            dtypes_dict.update({col_name: types.Float(precision=3, asdecimal=True)})

        if "int" in str(col_type):
            dtypes_dict.update({col_name: types.INT()})

        if "Int" in str(col_type):
            dtypes_dict.update({col_name: 'BIGINT'})

        if "bool" in str(col_type):
            dtypes_dict.update({col_name: types.BOOLEAN()})
    #Return dict
    return dtypes_dict

def execute_from_sql(sql_file):
    """ Try to execute a .sql file on 'db' database.

    Args:
        sql_file (str): Path to .sql file.
    """
    #Connect to the database, and run query from sql_file
    with db.connect() as con:
        with open(sql_file, 'r') as file:
            query = text(file.read())
            con.execute(query)
