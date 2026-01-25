import re
import pyodbc
import mysql.connector

class msssql_data_provider:
    """Data provider for Microsoft SQL Server
    """
    __connection = None
    
    def __init__(self):
        pass

    def request_row_by_value_in_column(self, table: str, column: str, value: int) -> list[dict]:
        """Returns rows, where value of column in parameters is same as value parameter.\n
        After executing this method, connection closes.

        Args:
            table (str): What table is requested
            column (str): Column to check
            value (int): What column must be equal to

        Returns:
            list[dict]: Returns list of rows as dict with saved column names
        """
        
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = self.__connection.cursor()
        cxr.execute(f'SELECT * FROM {table} WHERE {column} = ?;', (value,))

        rows = cxr.fetchall()
        columns = [column[0] for column in cxr.description]
        
        l = []
        for row in rows:
            l.append(dict(zip(columns, row)))
            
        self.__connection.close()
            
        return l
    
    def insert(self, table: str, columns: tuple[str], values: tuple[str]):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor()
        cxr.execute(f'INSERT INTO {table} {str(columns)} VALUES {str(values)}')
        
        self.__connection.close()
    
    def get_value_in_column_by_id(self, table: str, column: str, id: str) -> tuple:
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', id):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = self.__connection.cursor()
        cxr.execute(f'SELECT {column} FROM {table} WHERE id = ?', (id,))
        
        row = cxr.fetchone()
        self.__connection.close()
        
        return row[0] if row else None
    
    def insert(self, table: str, columns: tuple[str], values: tuple[str]):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor()
        cxr.execute(f'INSERT INTO {table} {str(columns)} VALUES {str(values)}')
        
        self.__connection.close()
        
    def delete(self, table: str, id: str):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor()
        cxr.execute(f'DELETE FROM {table} WHERE id = {id}')
        
        self.__connection.close()
        

    def set_value_in_column_by_id(self, table: str, column: str, id: str, value: str):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        
        if re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', table) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', column) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', id) or re.fullmatch('^[A-Za-z_][A-Za-z0-9_]*$', value):
            raise ValueError("Parameters cannot contain special symbols.")
        cxr = self.__connection.cursor()
        cxr.execute(f'UPDATE {table} SET {column} = ? WHERE id = ?', (value, id,))
        self.__connection.close()

    def connect(self, conn_data: dict):
        """This method must be executed before every request.

        Args:
            conn_data (dict): Connection data, must contain parameters 'driver', 'server', 'database', 'uid', 'pwd'

        Raises:
            ValueError: Raises exception if data is missing
        """

        driver = conn_data['driver']
        server = conn_data['server']
        database = conn_data['database']
        uid = conn_data['uid']
        pwd = conn_data['pwd']

        if not driver or not server or not database or not uid or not pwd:
            raise ValueError('Missing values.')

        self.__connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}')
        
class mysql_data_provider:
    
    __connection = None
    
    def __init__(self):
        pass
    
    def request_row_by_value_in_column(self, table: str, column: str, value: int) -> list[dict]:
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor(buffered=True, dictionary=True)
        cxr.execute(f'SELECT * FROM {table} WHERE {column} = {value}')
        
        r = cxr.fetchall()
        
        self.__connection.close()
        
        return r
    
    def set_value_in_column_by_id(self, table: str, column: str, id: str, value: str):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor(buffered=True)
        cxr.execute(f'UPDATE {table} SET {column} = {value} WHERE id = {id}')
        
        self.__connection.close()
    
    def get_value_in_column_by_id(self, table: str, column: str, id: str) -> tuple:
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        
        cxr = self.__connection.cursor(buffered=True, dictionary=True)
        cxr.execute(f'SELECT {column} FROM {table} WHERE id = {id}')
        r = cxr.fetchone()
        
        self.__connection.close()
        
        for k, v in r.items():
            return (k, v)
    
    def insert(self, table: str, columns: tuple[str], values: tuple[str]):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor()
        cxr.execute(f'INSERT INTO {table} {str(columns)} VALUES {str(values)}')
        
        self.__connection.close()
        
    def delete(self, table: str, id: int):
        if self.__connection == None:
            raise RuntimeError("No connection to DB.")
        cxr = self.__connection.cursor()
        cxr.execute(f'DELETE FROM {table} WHERE id = {id}')
        
        self.__connection.close()
    
    def connect(self, conn_data: dict):
        """This method must be executed before every request.

        Args:
            conn_data (dict): Connection data, must contain parameters 'server', 'database', 'uid', 'pwd'

        Raises:
            ValueError: Raises exception if data are missing
        """
        
        if not conn_data['server'] or not conn_data['database'] or not conn_data['uid'] or not conn_data['pwd']:
            raise ValueError('Missing data.')
        
        self.__connection = mysql.connector.connect(host=conn_data['server'] ,user=conn_data['uid'], password=conn_data['pwd'], database=conn_data['database'])
    
    