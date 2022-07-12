"""
    This module manages the project settings.
    It reads the settings from the .env file at root folder.
"""

from decouple import config

#Database
pg_user = config('USER', default='postgres')
pg_password = config('PASSWORD', default='admin')
pg_host = config('HOST', default='localhost')
pg_port = config('PORT', cast=int, default='5432')
pg_database = config('DATABASE', default='postgres')

#CSV urls
url_museos = config('URL_MUSEOS')
url_salas_cine = config('URL_CINES')
url_bibliotecas = config('URL_BIBLIOTECAS')

#Path files
default_data_path = config('DEFAULT_DATA_PATH', default='data')
default_sql_path = config('DEFAULT_SQL_PATH', default='tables.sql')
default_log_path = config('DEFAULT_LOG_PATH', default='db.log')