import oracledb
import pyodbc
import mysql.connector
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('config.ini')


class Rms:
    def __init__(self):
        try:
            self.connect = oracledb.connect(dsn=config['rms']['conn_string'])
            self.cursor = self.connect.cursor()
        except Exception as e:
            print(e)

    def get_data(self, query, return_type='j'):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        if return_type == 'j':
            return data
        elif return_type == 't':
            return pd.DataFrame(data)
        else:
            raise ValueError('Not valid value of param return_type')

    def change_data(self, query, rows):
        try:
            self.cursor.executemany(query, rows)
            self.connect.commit()
            return True, len(rows)
        except Exception as e:
            print(e)

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)

    def __del__(self):
        # close connection
        self.connect.close()


class MsSql:
    def __init__(self, srv, db=None, user=None, pwd=None):
        try:
            c_str = config[srv]['conn_string']
        except KeyError:
            c_str = 'DRIVER={SQL Server};'+f'SERVER={srv};DATABASE={db};UID={user};PWD={pwd}'
        try:
            self.connect = pyodbc.connect(c_str)
            self.cursor = self.connect.cursor()
        except Exception as e:
            print(e)

    def get_data(self, query, return_type='j'):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        if return_type == 'j':
            return data
        elif return_type == 't':
            return pd.DataFrame(data)
        else:
            raise ValueError('Not valid value of param return_type')

    def modify_data(self, query, rows):
        try:
            self.cursor.executemany(query, rows)
            self.connect.commit()
            return True, len(rows)
        except Exception as e:
            print(e)

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)

    def __del__(self):
        # close connection
        self.connect.close()


class MySql:
    def __init__(self, srv, db=None, user=None, pwd=None, port=None):
        try:
            self.connect = mysql.connector.Connect(host=config[srv]['host'],
                                                   user=config[srv]['user'],
                                                   password=config[srv]['password'],
                                                   port=config[srv]['port'],
                                                   database=config[srv]['database'])
            self.cursor = self.connect.cursor()
        except KeyError:
            try:
                self.connect = mysql.connector.Connect(host=srv,
                                                       user=user,
                                                       password=pwd,
                                                       port=port,
                                                       database=db)
                self.cursor = self.connect.cursor()
            except Exception as e:
                print(e)

    def get_data(self, query, return_type='j'):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        if return_type == 'j':
            return data
        elif return_type == 't':
            return pd.DataFrame(data)
        else:
            raise ValueError('Not valid value of param return_type')

    def modify_data(self, query, rows):
        try:
            self.cursor.executemany(query, rows)
            self.connect.commit()
            return True, len(rows)
        except Exception as e:
            print(e)

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)

    def __del__(self):
        # close connection
        self.connect.close()




