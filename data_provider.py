from . import db_data_provider

from os import getenv

__connection = None

connection_dict = {
    'driver': getenv('DRIVER', 'ODBC Driver 17 for SQL Server'), 
    'server': getenv('SERVER', 'localhost'), 
    'uid': getenv('UID'), 
    'pwd': getenv('PWD')
}

def set_connection_data(connection_data: dict):
    """Sets data for connection.

    Args:
        connection_data (dict): Must contain 'uid' - username, 'pwd' - password, 'server' - server address, 'database' - database on server
    """
    
    global connection_dict
    
    connection_dict = connection_data

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
    if connection_dict['driver'] != None:
        pr = db_data_provider.msssql_data_provider()
    else:
        pr = db_data_provider.mysql_data_provider()
    pr.connect(connection_dict)
        
    return pr.request_row_by_value_in_column(table, column, value)
    
def set_value_in_column_by_id(table: str, column: str, id: str, value: str):
    if connection_dict['driver'] != None:
        pr = db_data_provider.msssql_data_provider()
    else:
        pr = db_data_provider.mysql_data_provider()
    
    pr.connect(connection_dict)
    pr.set_value_in_column_by_id(table, column, id, value)

def get_value_in_column_by_id(table: str, column: str, id: str):
    if connection_dict['driver'] != None:
        pr = db_data_provider.msssql_data_provider()
    else:
        pr = db_data_provider.mysql_data_provider()
    pr.connect(connection_dict)
    pr.get_value_in_column_by_id(table, column, id)

def insert(table: str, columns: tuple[str], values: tuple[str]):
    """Insert new record into database. Values must follow exact order of columns.

    Args:
        table (str): Table name. For example: 'employees'
        columns (tuple[str]): Columns to insert data into them. For example: ('name', 'surname', 'wage')
        values (tuple[str]): Values for columns. For example: ('John', 'Doe', '20000')
    """
    if connection_dict['driver'] != None:
        pr = db_data_provider.msssql_data_provider()
    else:
        pr = db_data_provider.mysql_data_provider()
    pr.connect(connection_dict)
    pr.insert(table, columns, values)
    
def delete_by_id(table: str, id: str):
    """Delete record from table by id string.

    Args:
        table (str): Table from which delete record
        id (int): ID of record to delete
    """
    if connection_dict['driver'] != None:
        pr = db_data_provider.msssql_data_provider()
    else:
        pr = db_data_provider.mysql_data_provider()
    pr.connect(connection_dict)
    pr.delete(table, id)

