"""
Filename : bdAnalytics.py
Author : Archit Joshi (aj6082), Parth Sethia
Description : Wrapper script to run the CSCI720 Project scripts for New York crash data analysis.
Language : python3
"""
import cleanData
import createDB
import visualiseData
import dataAnalysis


def main():
    # Create database, schema and load raw data
    createDB.main()
    # Clean the data and filter for years 2019 & 2020
    cleanData.main()
    # Visualize the data using folium heatmaps and cluster maps
    visualiseData.main()
    # Perform analysis and visualizations on the data
    dataAnalysis.main()


if __name__ == "__main__":
    main()
