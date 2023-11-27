"""
Filename : dataAnalysis.py
Author : Archit Joshi, Parth Sethia
Description : Analysis of the NYC crash dataset
Language : python3
"""
import psycopg2

def connectDB():
    """
    Helper function to connect to a database db_720 to load NYC Crash data to.
    If database doesn't exist, the function will create the database and
    re-attempt to estabilish connection.

    :return: database connection object
    """
    host = 'localhost'
    username = 'postgres'
    password = 'RITPostGreSQL'
    port = '5432'
    database = 'NYC_Crash_Database'

    try:
        # Attempt to connect to the existing database
        conn = psycopg2.connect(database=database, user=username,
                                password=password, host=host,
                                port=port)
        return conn
    except psycopg2.Error as e:
        print("Connection error, check credentials or run createDB.py")


def dayWithMostAccidents(connection):
    aggregation_script = "select trim(to_char(crash_date, 'Day')) as Day, " \
                         "count(*) as count " \
                         "from nyc_crashes " \
                         "group by Day " \
                         "order by count desc"
    try:
        connection_cursor = connection.cursor()
        connection_cursor.execute(aggregation_script)
        result = connection_cursor.fetchone()
    except psycopg2.Error as e:
        print(e)
        return False
    connection.cursor().close()
    print("5. Which day of the week has the most accidents?")
    print("Answer:", result[0])
    print()

def hourWithMostAccidents(connection):
    aggregation_script = "select trim(split_part(crash_time, ':', 1)) as hour, " \
                         "count(*) as count " \
                         "from nyc_crashes " \
                         "group by hour " \
                         "order by count desc;"
    try:
        connection_cursor = connection.cursor()
        connection_cursor.execute(aggregation_script)
        result = connection_cursor.fetchone()
    except psycopg2.Error as e:
        print(e)
        return False
    connection.cursor().close()
    print("6. Which hour of the day has the most accidents?")
    print("Answer:", result[0])
    print()

def twelveDaysWithMostAccidentsIn2020(connection):
    aggregation_script = "select case " \
                         "when extract(Month from x.crash_date)=7 then concat('July ', extract(Day from x.crash_date)) " \
                         "when extract(Month from x.crash_date) =6 then concat('June ', extract(Day from x.crash_date)) " \
                         "end from (" \
                         "select crash_date, count(*) as count from nyc_crashes " \
                         "where extract(year from crash_date) = '2020' " \
                         "group by crash_date " \
                         "order by count desc limit 12) x " \
                         "order by x.crash_date;"
    try:
        connection_cursor = connection.cursor()
        connection_cursor.execute(aggregation_script)
        result = connection_cursor.fetchall()
    except psycopg2.Error as e:
        print(e)
        return False
    connection.cursor().close()
    june = []
    july = []
    for days in result:
        if 'June' in days[0]:
            june.append(days[0].replace("June ",""))
        elif "July" in days[0]:
            july.append(days[0].replace("July ",""))
    print("7. In the year 2020, which 12 days had the most accidents?\nCan you speculate about why this is?")
    print("Ans: ",", ".join(day for day in june),"June", "\n     ", ", ".join(day for day in july), "July")


def top100ConsecutiveDaysWithMostAccidents(connection):
    aggregation_script = "select crash_date, " \
                         "count(*) as number_of_crash " \
                         "from nyc_crashes " \
                         "where crash_date between '2019-01-01' and '2020-10-31' " \
                         "group by crash_date " \
                         "order by crash_date;"

    try:
        connection_cursor = connection.cursor()
        connection_cursor.execute(aggregation_script)
        result = connection_cursor.fetchall()
    except psycopg2.Error as e:
        print(e)
        return False
    connection.cursor().close()

    print("4. For the year of January 2019 to October of 2020, which 100 consecutive days had the most accidents?")
    all_days = []
    accidents_in_a_day = []
    max_accidents = 0
    start_index_for_max_accidents = None
    for values in result:
        all_days.append(str(values[0]))
        accidents_in_a_day.append(values[1])

    for index in range(0, 122):
        total_accidents_in_current_range = sum(accidents_in_a_day[index:index+100])
        if total_accidents_in_current_range > max_accidents:
            max_accidents = total_accidents_in_current_range
            start_index_for_max_accidents = index
    print("Answer: The 100 consecutive days with most accidents start from", all_days[start_index_for_max_accidents], "till", all_days[start_index_for_max_accidents+99])
    print()


def main():
    conn = connectDB()
    top100ConsecutiveDaysWithMostAccidents(conn)
    dayWithMostAccidents(conn)
    hourWithMostAccidents(conn)
    twelveDaysWithMostAccidentsIn2020(conn)
    pass

if __name__ == '__main__':
    main()