import db_data_provider
import flat_file_data_provider

from os import getenv
import pyodbc

import re

__connection = None

def request_rows_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
    """Returns rows, where value of column in parameters is same as value parameter

    Args:
        table (str): What table is requested
        column (str): Column to check
        value (int): What column must be equal to

    Returns:
        list[dict]: Returns list of rows as dict with saved column names
    """
    
    if not __connection:
        pr = flat_file_data_provider.flat_file_data_provider()
        return pr.request_rows_by_value_in_column(column, value)
    else:
        pr = db_data_provider.db_data_provider()
        pr.set_connection(__connection)
        return pr.request_row_by_value_in_column(table, column, value)

def connect(conn_data: dict):
    """This method must be executed before request.

    Args:
        conn_data (dict): Connection data, must contain parameters 'driver', 'server', 'database', 'uid', 'pwd'

    Raises:
        ValueError: Raises exception if data is missing
    """
    global __connection
    
    driver = conn_data['driver']
    server = conn_data['server']
    database = conn_data['database']
    uid = conn_data['uid']
    pwd = conn_data['pwd']
    
    if not driver or not server or not database or not uid or not pwd:
        raise ValueError('Missing values.')
    
    __connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}')
    

