"""
Filename : analyseData.py
Author : Archit Joshi
Description : Clustering, classifying and everything else.
Language : python3
"""

import folium
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from branca.colormap import linear
from sklearn.cluster import KMeans
from folium.plugins import HeatMap
from folium.plugins import MarkerCluster


def connectDB():
    """
    Helper function to connect to a database db_720 to load NYC Crash data to.
    If database doesn't exist, the function will create the database and
    re-attempt to establish connection.

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
        data = pd.read_sql("SELECT * FROM clean_nyc_crashes", conn)
        return data
    except psycopg2.Error as e:
        print(
            f"Connection error, check credentials or run createDB.py --> {e}")


def separateData(data):
    """
    Function to split the filtered database data into 2019 and 2020 data for
    comparison and analysis.

    :param data: original data in a dataframe
    :return: 2019 and 2020 data split
    """
    data['crash_date'] = pd.to_datetime(data['crash_date'], format='%Y-%m-%d')

    # Create masks to filter data for 2019 and 2020
    mask_2019 = (data['crash_date'].dt.year == 2019)
    mask_2020 = (data['crash_date'].dt.year == 2020)

    # Create separate DataFrames for 2019 and 2020
    data_2019 = data[mask_2019].copy()
    data_2020 = data[mask_2020].copy()

    # Reset index if needed
    data_2019.reset_index(drop=True, inplace=True)
    data_2020.reset_index(drop=True, inplace=True)

    return data_2019, data_2020


def generateHeatMap(data, year):
    """
    Function to visualize the data as a heatmap using the folium library.

    :param data: cleaned nyc crash data
    :param year: year value (2019/2020)
    :return: None
    """
    # Convert latitude and longitude columns to float
    data['latitude'] = data['latitude'].astype(float)
    data['longitude'] = data['longitude'].astype(float)

    # Extract necessary columns
    data = data[['latitude', 'longitude']].dropna().values.tolist()

    # Create a base map centered at an average location
    m = folium.Map(location=[sum(p[0] for p in data) / len(data),
                             sum(p[1] for p in data) / len(data)],
                   zoom_start=12)

    # Convert the data into a format that HeatMap can understand
    heat_data = [[point[0], point[1]] for point in data]

    # Create a color scale legend
    color_scale = linear.RdYlBu_03.scale(0, len(heat_data))
    m.add_child(color_scale)

    # Create HeatMap layer
    HeatMap(heat_data, gradient={0.4: 'blue', 0.65: 'lime', 1: 'red'},
            opacity=0.4).add_to(
        m)

    # Save the map
    filename = f"heatmap_{year}.html"
    m.save(filename)
    print(f"{filename} generated and saved successfully")


def clusterData(data, year):
    """
    Using folium to perform clustering on terrain map of Brooklyn.

    :param data: cleaned nyc crash data
    :param year: year value (2019/2020)
    :return: None
    """
    # Convert latitude and longitude columns to float
    data['latitude'] = data['latitude'].astype(float)
    data['longitude'] = data['longitude'].astype(float)

    # Extract necessary columns
    data = data[['latitude', 'longitude']].dropna().values.tolist()

    # Create a base map centered at an average location
    m = folium.Map(location=[sum(p[0] for p in data) / len(data),
                             sum(p[1] for p in data) / len(data)],
                   zoom_start=12)

    # Convert the data into a format that MarkerCluster can understand
    marker_data = [[point[0], point[1]] for point in data]

    # Create a MarkerCluster layer
    marker_cluster = MarkerCluster().add_to(m)

    # Add markers to the MarkerCluster
    for point in marker_data:
        folium.Marker(location=[point[0], point[1]],
                      popup=f"Location: {point[0]}, {point[1]}").add_to(
            marker_cluster)

    # Save the map
    filename = f"clustered_map_{year}.html"
    m.save(filename)
    print(f"{filename} generated and saved successfully")


def kMeansClustering(data, year):
    """
    Function to perform k-means clustering on the data.

    :param data: cleaned nyc crash data
    :param year: year value (2019/2020)
    :return: None
    """
    # Convert latitude and longitude columns to float with high precision
    data['latitude'] = data['latitude'].astype(float)
    data['longitude'] = data['longitude'].astype(float)

    # Extract necessary columns
    data_1 = data[['latitude', 'longitude']]

    # Specify the number of clusters (adjust as needed)
    k = 3

    # Perform K-Means clustering
    kmeans = KMeans(n_clusters=k, random_state=42)
    data['cluster'] = kmeans.fit_predict(data_1)

    # Visualize the clusters
    plt.figure(figsize=(12, 8))
    plt.scatter(data['longitude'], data['latitude'], c=data['cluster'],
                cmap='viridis', s=50, edgecolors='black')
    plt.title(f'K-Means Clustering of Car Crashes (k={k}) for year {year}')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.show()


def main():
    dataframe = connectDB()

    # Sort data into 2019 data and 2020 data
    crash_data_2019, crash_data_2020 = separateData(dataframe)

    # Data visualization and clustering
    generateHeatMap(crash_data_2019, '2019')
    generateHeatMap(crash_data_2020, '2020')
    clusterData(crash_data_2020, '2019')
    clusterData(crash_data_2019, '2020')
    kMeansClustering(crash_data_2019, '2019')
    kMeansClustering(crash_data_2020, '2020')


if __name__ == "__main__":
    main()
