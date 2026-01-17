import db_data_provider

from os import getenv
import pyodbc

import re

__connection = None

connection_dict = {
    'driver': getenv('DRIVER', 'ODBC Driver 17 for SQL Server'), 
    'server': getenv('SERVER', 'localhost'), 
    'uid': getenv('UID'), 
    'pwd': getenv('PWD')
}

def set_connection_data(**kwargs):
    global connection_dict
    
    connection_dict = kwargs

def request_rows_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
    """Returns rows, where value of column in parameters is same as value parameter.\n   
    Selects best database to connect based on credentials. but not now(

    Args:
        table (str): What table is requested.
        column (str): Column to check.
        value (int): What column must be equal to.

    Returns:
        list[dict]: Returns list of rows as dict with saved column names.
    """
    
    if not __connection:
        raise RuntimeError('Database is not connected.')
    else:
        pr = db_data_provider.msssql_data_provider()
        pr.connect(connection_dict)
        
        return pr.request_row_by_value_in_column(table, column, value)
    
def set_value_in_column_by_id(table: str, column: str, id: str, value: str):
    if not __connection:
        raise RuntimeError('Database is not connected.')
    else:
        pr = db_data_provider.msssql_data_provider()
        pr.connect(connection_dict)
        pr.set_value_in_column_by_id(table, column, id, value)

def get_value_in_column_by_id(table: str, column: str, id: str):
    if not __connection:
        raise RuntimeError('Database is not connected.')
    else:
        pr = db_data_provider.msssql_data_provider()
        pr.connect(connection_dict)
        pr.get_value_in_column_by_id(table, column, id)

