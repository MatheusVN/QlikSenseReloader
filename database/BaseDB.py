import mysql.connector
from mysql.connector import Error
import configparser
import os
from pathlib import Path


class BaseDB:

    def __init__(self, source_path: Path) -> None:
        self.__source_path = source_path
        self.__config = configparser.ConfigParser()
        self.__config.read(os.path.join(self.__source_path, 'config.ini'))
        self.__host = self.__config['database']['host']
        self.__user = self.__config['database']['user']
        self.__password = self.__config['database']['password']
        self.__database = self.__config['database']['database']
        self.connection = None

    def connect(self) -> None:
        try:
            self.connection = mysql.connector.connect(
                host=self.__host,
                user=self.__user,
                password=self.__password,
                database=self.__database
            )
            if self.connection.is_connected():
                print("MySQL server connected successfully")
        except Error as e:
            print(f'Error on connecting to MySQL: {e}')

    def close(self) -> None:
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")

    def create_table(self, file_path: str = 'create_log.sql') -> None:
        print("Starting table creation...")
        try:
            self.connect()
            print("Connected to the database.")
            creation_file = os.path.join(self.__source_path, file_path)
            with open(creation_file, 'r') as sql_file:
                sql_query = sql_file.read()
                cursor = self.connection.cursor()
                cursor.execute(sql_query)
                self.connection.commit()
                print("Table created successfully.")
        except Error as e:
            print(f'Error on creating table: {e}')
        except Exception as e:
            print(f'Unexpected error: {e}')
        finally:
            if self.connection:
                self.close()

    def execute_query(self, query: str, data: list = None) -> None:
        self.connect()
        try:
            if self.connection is not None and self.connection.is_connected():
                cursor = self.connection.cursor()
                if data:
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query)
                self.connection.commit()
        except Error as e:
            print(f'Error on executing query: {e}')
        finally:
            if cursor:
                cursor.close()
            self.close()

