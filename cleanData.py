"""
Filename : loadData.py
Author : Archit Joshi, Parth Sethia
Description : Cleaning NYC Crash data for outliers, missing data.
Language : python3
"""

import os
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import config_template


def connectDB():
    """
    Helper function to connect to a database db_720 to load NYC Crash data to.
    If database doesn't exist, the function will create the database and
    re-attempt to estabilish connection.

    :return: database connection object
    """
    host = 'localhost'
    username = 'postgres'
    password = config_template.DB_PASSWORD
    port = '5432'
    database = config_template.DB_NAME

    try:
        # Attempt to connect to the existing database
        conn = psycopg2.connect(database=database, user=username,
                                password=password, host=host,
                                port=port)
        return conn
    except psycopg2.Error as e:
        print("Connection error, check credentials or run createDB.py")


def filterBoroughs(connection, borough):
    """
    Helper function to clear data which doesnt have BROOKLYN as the chosen
    borough for analysis. Removes entries with null as the boroughs as well.

    :param connection: database connection object
    :param borough: New york borough to analyse
    :return: Success message if query worked
    """
    # Filter data for only one borough - BROOKLYN and push to new table
    delete_boroughs = f"""
            CREATE TABLE clean_nyc_crashes AS
            SELECT * FROM nyc_crashes WHERE borough = '{borough}'
        """
    try:
        connection.cursor().execute(delete_boroughs)
    except psycopg2.Error as e:
        print(f"Error encountered : {e}")
        return False

    # Delete entries with null as boroughs
    delete_null_boroughs = "DELETE FROM clean_nyc_crashes WHERE borough is null"
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

    filter_dates = """
    DELETE FROM clean_nyc_crashes
    WHERE NOT 
        ((crash_date BETWEEN '2019-01-01' AND '2019-12-31')
        OR
        (crash_date BETWEEN '2020-01-01' AND '2020-12-31'));
"""
    try:
        connection.cursor().execute(filter_dates)
    except psycopg2.Error as e:
        print(f"Filtering for year 2020 failed: {e}")
        return False

    connection.commit()
    connection.cursor().close()

    return "== Records beyond 2019 and 2020 deleted. =="


def filterLongitudeLatitude(connection):
    """
    Helper function to filter data and remove null longitude and latitude values.

    :param connection: database connection object
    :return: None
    """
    remove_nulls = "DELETE FROM clean_nyc_crashes WHERE longitude is null or latitude is null"
    try:
        connection.cursor().execute(remove_nulls)
    except psycopg2.Error as e:
        print(f"Error while removing nulls : {e}")
        return False
    connection.commit()
    connection.cursor().close()

    remove_zeroes = "DELETE FROM clean_nyc_crashes WHERE cast(longitude as float) = 0 or cast(latitude as float) = 0"
    try:
        connection.cursor().execute(remove_zeroes)
    except psycopg2.Error as e:
        print(f"Error while removing nulls : {e}")
        return False
    connection.commit()
    connection.cursor().close()

    return "== Null latitude and longitude values removed. =="


def wipeOldTable(connection):
    cursor = connection.cursor()
    table_name = 'clean_nyc_crashes'
    cursor.execute(sql.SQL(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"),
                   (table_name,))
    table_exists = cursor.fetchone()[0]

    # If the table exists, drop it
    if table_exists:
        cursor.execute(
            sql.SQL("DROP TABLE {}").format(sql.Identifier(table_name)))
        print(f"Table '{table_name}' has been deleted.")
        connection.commit()
        cursor.close()
    else:
        print(f"Table '{table_name}' does not exist.")


def cleanData(connection, borough):
    """
    Wrapper function to call the other functions that clean the data.
    :param connection: database connection object
    :param borough: borough to analyse data for
    :return: None
    """
    # Delete old table if it already exists
    wipeOldTable(connection)

    # Clean data and push to new table
    print(filterBoroughs(connection, borough))
    print(filterLongitudeLatitude(connection))
    print(filterTime(connection))


def main():
    conn = connectDB()
    analyse_borough = "BROOKLYN"
    cleanData(conn, analyse_borough)


if __name__ == "__main__":
    main()
