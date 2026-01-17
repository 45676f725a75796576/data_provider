import re
import pyodbc

class db_data_provider:
    __connection = None

    def request_row_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
        if re.fullmatch('[A-Za-z0-9]', table) or re.fullmatch('[A-Za-z0-9]', column) or re.fullmatch('[A-Za-z0-9]', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = __connection.cursor()
        cxr.execute('SELECT * FROM ? WHERE ? = ?;', table, column, value)

        rows = cxr.fetchall()
        columns = [column[0] for column in cxr.description]
        
        l = []
        for row in rows:
            l.append(dict(zip(columns, row)))
            
        return l

    def set_connection(connection: pyodbc.Connection):
        global __connection
        
        if not isinstance(__connection, pyodbc.Connection):
            raise TypeError("Parameter must be type of pyodbc.connection.")

        __connection = connection