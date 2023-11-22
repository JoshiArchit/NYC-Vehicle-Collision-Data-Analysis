"""
Filename : loadData.py
Author : Archit Joshi
Description :
Language : python3
"""
import os
import psycopg2
import pandas as pd
import numpy as np


def connectDB():
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
        print("Connection error, check credentials or run createDB.py")


def loadData(connection):
    """
    Function to load data from csv file into database.

    :param connection: database connection object
    :return: None
    """
    # Load data with all attributes
    path = os.getcwd() + "//Motor_Vehicle_Collisions_-_Crashes_20231120.csv"
    copy_query = f"COPY nyc_crashes from \'{path}\' DELIMITER ',' CSV HEADER"
    try:
        cursor = connection.cursor()
        cursor.execute(copy_query)
        connection.commit()
    except psycopg2.Error as e:
        print(f"ERROR in loading data : {e}")
        return False
    return "== Data loaded =="


def filterBoroughs(connection, borough):
    """
    Helper function to clear data which doesnt have BROOKLYN as the chosen
    borough for analysis. Removes entries with null as the boroughs as well.

    :param connection: database connection object
    :param borough: New york borough to analyse
    :return: Success message if query worked
    """
    # Filter data for only one borough - BROOKLYN
    delete_query = f"DELETE FROM nyc_crashes WHERE borough != '{borough}'"
    try:
        connection.cursor().execute(delete_query)
    except psycopg2.Error as e:
        print(f"Error encountered : {e}")
        return False

    # Delete entries with null as boroughs
    delete_null_boroughs = "DELETE FROM nyc_crashes WHERE borough is null"
    try:
        connection.cursor().execute(delete_null_boroughs)
    except psycopg2.Error as e:
        print(f"Error encountered while removing nulls :{e}")
        return False
    connection.commit()
    connection.cursor().close()
    return f"== Boroughs other than {borough} removed from data =="


def filterTime(connection):
    """
    Function to filter data for June, July 2019 and 2020. All other data can be removed.

    :param connection: database connection object
    :return: None
    """
    filter_dates_2019 = "DELETE FROM nyc_crashes WHERE crash_date NOT BETWEEN '06/01/2019' AND '07/31/2019'"
    try:
        connection.cursor().execute(filter_dates_2019)
    except psycopg2.Error as e:
        print(f"Filtering for year 2019 failed: {e}")
        return False

    filter_dates_2020 = "DELETE FROM nyc_crashes WHERE crash_date NOT BETWEEN '06/01/2020' AND '07/31/2020'"
    try:
        connection.cursor().execute(filter_dates_2020)
    except psycopg2.Error as e:
        print(f"Filtering for year 2020 failed: {e}")
        return False

    connection.commit()
    connection.cursor().close()

    return "== Records beyond summer 2019 and summer 2020 deleted. =="


def filterLongitudeLatitude(connection):
    """
    Helper function to filter data and remove null longitude and latitude values.

    :param connection: database connection object
    :return: None
    """
    remove_nulls = "DELETE FROM nyc_crashes WHERE longitude is null or latitude is null"
    try:
        connection.cursor().execute(remove_nulls)
    except psycopg2.Error as e:
        print(f"Error while removing nulls : {e}")
        return False
    connection.commit()
    connection.cursor().close()
    return "== Null latitude and longitude values removed. =="


def cleanData(connection, borough):
    """
    Wrapper function to call the other functions that clean the data.
    :param connection: database connection object
    :param borough: borough to analyse data for
    :return: None
    """
    print(filterBoroughs(connection, borough))
    print(filterTime(connection))
    print(filterLongitudeLatitude(connection))


def main():
    conn = connectDB()
    analyse_borough = "BROOKLYN"
    loadData(conn)
    cleanData(conn, analyse_borough)


if __name__ == "__main__":
    main()
