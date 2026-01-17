import re
import pyodbc

class db_data_provider:
    __connection = None

    def request_row_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
        """Returns rows, where value of column in parameters is same as value parameter.\n After executing this method, connection closes.

        Args:
            table (str): What table is requested
            column (str): Column to check
            value (int): What column must be equal to

        Returns:
            list[dict]: Returns list of rows as dict with saved column names
        """
        
        if re.fullmatch('[A-Za-z0-9]', table) or re.fullmatch('[A-Za-z0-9]', column) or re.fullmatch('[A-Za-z0-9]', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = __connection.cursor()
        cxr.execute('SELECT * FROM ? WHERE ? = ?;', table, column, value)

        rows = cxr.fetchall()
        columns = [column[0] for column in cxr.description]
        
        l = []
        for row in rows:
            l.append(dict(zip(columns, row)))
            
        __connection.close()
            
        return l

    def set_connection(connection: pyodbc.Connection):
        """This method must be executed before request.

        Args:
            connection (pyodbc.Connection): Connection to the database.

        Raises:
            TypeError: Raises TypeError if invalid type.
        """
        
        global __connection
        
        if not isinstance(__connection, pyodbc.Connection):
            raise TypeError("Parameter must be type of pyodbc.connection.")

        __connection = connection