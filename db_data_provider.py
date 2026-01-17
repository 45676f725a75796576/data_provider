import re
import pyodbc

class msssql_data_provider:
    """Data provider for Microsoft SQL Server
    """
    __connection = None

    def request_row_by_value_in_column(table: str, column: str, value: int) -> list[dict]:
        """Returns rows, where value of column in parameters is same as value parameter.\n
        After executing this method, connection closes.

        Args:
            table (str): What table is requested
            column (str): Column to check
            value (int): What column must be equal to

        Returns:
            list[dict]: Returns list of rows as dict with saved column names
        """
        
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = __connection.cursor()
        cxr.execute(f'SELECT * FROM {table} WHERE {column} = ?;', (value,))

        rows = cxr.fetchall()
        columns = [column[0] for column in cxr.description]
        
        l = []
        for row in rows:
            l.append(dict(zip(columns, row)))
            
        __connection.close()
            
        return l
    
    def get_value_in_column_by_id(table: str, column: str, id: str) -> tuple:
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', id):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = __connection.cursor()
        cxr.execute(f'SELECT {column} FROM {table} WHERE id = ?', (id,))
        
        row = cxr.fetchone()
        __connection.close()
        
        return row[0] if row else None
        

    def set_value_in_column_by_id(table: str, column: str, id: str, value: str):
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', id) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = __connection.cursor()
        cxr.execute(f'UPDATE {table} SET {column} = ? WHERE id = ?', (value, id,))
        __connection.close()

    def connect(conn_data: dict):
        """This method must be executed before every request.

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
    
    