"""
Filename : loadData.py
Author : Archit Joshi, Parth Sethia
Description : Creating database and schema for NYC Crash data
Data Set: https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95
Language : python3
"""
import os
import psycopg2
import pandas as pd
import numpy as np


#
# def parseData():
#     path = os.getcwd() + "//Motor_Vehicle_Collisions_-_Crashes_20231120.csv"
#     borough = "BROOKLYN"
#     dataframe = pd.read_csv(path, query=f'BOROUGH == "{borough}"')
#     print(len(dataframe))

def databaseConnection():
    """
    Helper function to connect to a database db_720 to load NYC Crash data to.
    If database doesn't exist, the function will create the database and
    re-attempt to estabilish connection.

    :return: database connection object
    """
    host = 'localhost'
    username = 'postgres'
    password = 'Archit@2904'
    port = '5432'
    database = 'db_720'

    try:
        # Attempt to connect to the existing database
        conn = psycopg2.connect(database=database, user=username,
                                password=password, host=host,
                                port=port)
        return conn
    except psycopg2.Error as e:
        # If the database doesn't exist, create it
        if 'database "db_720" does not exist' in str(e):
            print("Provided database does not exist. Creating database db_720")
            conn = psycopg2.connect(user=username, password=password,
                                    host=host, port=port)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {database};")
            cursor.close()
            conn.close()

            # Reconnect to the newly created database
            conn = psycopg2.connect(database=database, user=username,
                                    password=password, host=host,
                                    port=port)
            return conn
        else:
            print(f"Connection failed : {e}")


def createSchema(conn):
    """
    Creates a POSTGRES database with attributes taken from column names from
    csv and datatype as varchar for everything (can be sorted later). Reading
    all this data into pandas dataframe is expensive.

    :param conn: database connection object
    :return: None
    """
    path = os.getcwd() + "//Motor_Vehicle_Collisions_-_Crashes_20231120.csv"
    dataframe = pd.read_csv(path, nrows=0)

    # Add '_' between column names and assign varchar datatype, set 'CRASH_DATE'
    # to date type
    columns = [
        (col.replace(' ', '_'),
         'date' if 'DATE' in col else 'varchar')
        for col in dataframe.columns
    ]
    print(columns)

    # Create table with data
    try:
        cursor = conn.cursor()
        create_table = f"CREATE TABLE NYC_CRASHES ({', '.join([f'{col} {data_type}' for col, data_type in columns])});"
        cursor.execute(create_table)
        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print(f"Table already exists. No action taken.\nError Message --> {e}")


def main():
    connection = databaseConnection()
    createSchema(connection)


if __name__ == "__main__":
    main()
