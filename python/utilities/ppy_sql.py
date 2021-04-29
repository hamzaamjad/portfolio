#!/usr/bin/env python3
"""Objects that provide an interface to interact with various SQL databases
"""

# -- Imports --------------------------------------------------------------------------------
import json, pprint, sys
from datetime import datetime

import psycopg2

class PostgreSQL():
    """Creates a connection to a PostgreSQL database. Layers on the psycopg2 package.
       Expects the following parameters, which can be supplied using ppy_auth:
           username -- user name use to authenticate
           password -- password used to authenticate
           engine -- 
           host -- database host address
           port -- connection port number (defaults to 5432 if not provided)
           dbname -- the database name
           db_cluster_identifier -- 
           print_info -- 
           
       Example:
           >>> db = ppy_sql.PostgreSQL(**connection_parameters)
           Connected
    """

    def __init__(self, username: str = '', password: str = '', engine: str = '', host: str = '', port: str = '5432', dbname: str = '', dbInstanceIdentifier: str = '', print_info: bool = False):
        """Created with user-specified parameters. By default, runs db.connect() to create connection & cursor at instantiation.
           Internally creates the following additional variables:
               connection_string -- the connection string used in the db.connect() method
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = username
        self.password = password
        self.last_error = ''
        
        self.connection_string = f'host={host} port={port} dbname={dbname} user={username} password={password}'
        self.connect(print_info = print_info)

    def connect(self, print_info:bool = False) -> None:
        """Create a connection & cursor based on the credentials provided.
           This method is run by default upon instantiation of the object.
           
           Set print_info = False to disable output of connection_info
           
           >>> db.connect()
           Connected
        """
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            print("Connected.")
            
            self.connection_info = {
                'host':self.host,
                'port':self.port,
                'dbname':self.dbname,
                'user':self.user,
                'timestamp':datetime.now()
            }
            if print_info:
                pprint.pprint(self.connection_info)
        except:
            print("Connection failed.")
            self.connection = json.dumps(sys.exc_info()[0])
    
    def disconnect(self) -> None:
        """Close the connection & cursor.
           Run at the end of a script to safely close connections & wrap-up.
           
           >>> db.disconnect()
           Disconnected.
        """
        self.cursor.close()
        self.connection.close()
        print("Disconnected.")
    
    def error_message(self, pgcode: str, pgerror: str) -> None:
        """Helper Function -- Store last error in self, print to console & rollback the connection
        """
        error = {'error_code' : pgcode, 'error_message' : pgerror}
        self.last_error = error
        print(error)
        self.connection.rollback()
        
    def execute_commit(self, sql: str) -> None:
        """Utility Function -- Execute user-specifed SQL & commit to connection
        
           >>> db.execute_commit()
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except psycopg2.Error as e:
            self.error_message(e.pgcode, e.pgerror)
    
    def execute_fetchall(self, sql: str) -> list:
        """Utility Function -- Execute user-specified SQL & fetch entire result set
        
           >>> db.execute_fetchall('SELECT * FROM table_name')
        """
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            return(rows)
        except psycopg2.Error as e:
            self.error_message(e.pgcode, e.pgerror)

    def copy_from(self, f, table: str, sep: str = '\t', null: str = '\\N') -> None:
        """Wrapper for psycopg2.cursor.copy_from method.
           Provides the following absraction:
               0. Assumes default sep is tab, and nulls are \\N
               1. Runs psycopyg2.cursor.copy_from on user-specified table using defaults
               2. Commits the copy_from
               3. Outputs status if successful, self.error_message() if not
               
           >>> db.copy_from(f, 'table_name')
           Data copied successfully to: table_name
        """
        try:
            self.cursor.copy_from(f, table, sep = sep, null = null)
            self.connection.commit()
            print(f"Data copied successfully to: {table}")
        except psycopg2.Error as e:
            self.error_message(e.pgcode, e.pgerror)

    def drop_table(self, table: str) -> None:
        """SQL Helper Function -- Drop the user-specified table
           Uses IF EXISTS to avoid errors
           
           >>> db.drop_table('table_name')
           Table table_name was dropped successfully
        """
        self.execute_commit(f'DROP TABLE IF EXISTS {table}')
        print(f'Table {table} was dropped successfully')
    
    def list_tables(self) -> list:
        """SQL Helper Function -- List tables available to the active user
        
           >>> db.list_tables()
        """
        sql = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = 'public'
        ORDER BY
            table_name
        """
        tables = self.execute_fetchall(sql)
        return(tables)