import os
import csv

class flat_file_data_provider:
    def __init__(self):
        pass
    
    def request_rows_by_value_in_column(self, column: str, value: int, *args, **kwargs) -> list[dict]:
        """Returns rows, where value of column in parameters is same as value parameter.\n
        Shouldn't be used most of the time, because doesn't support tables.

        Args:
            column (str): Column to check
            value (int): What column must be equal to

        Returns:
            list[dict]: Returns list of rows as dict with saved column names
        """
        file_exists = os.path.exists('../data.csv')
        with open('../data.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if not file_exists and kwargs['headers']:
                writer.writerow(kwargs['headers'])
        
        
        with open('../data.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            l = []
            for row in reader:
                if row[column] == value:
                    l.append(row)
            return l