import db_data_provider

from os import getenv
import pyodbc

import re

__connection = None

def request_rows_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
    if not __connection:
        pass
    else:
        pr = db_data_provider.db_data_provider()
        pr.set_connection(__connection)
        return pr.request_row_by_value_in_column(table, column, value)

def try_to_connect(conn_data: dict):
    global __connection
    
    driver = conn_data['driver']
    server = conn_data['server']
    database = conn_data['database']
    uid = conn_data['uid']
    pwd = conn_data['pwd']
    
    if not driver or not server or not database or not uid or not pwd:
        raise ValueError('Missing values.')
    
    __connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}')
    

