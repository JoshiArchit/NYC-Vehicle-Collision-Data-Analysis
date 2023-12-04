# CSCI720-Big-Data-Analytics-Project
CSCI720-Big Data Analytics Project

The following dependencies need to be installed on the machine - folium, pandas, psycopg2, branca, sklearn and postgreSQL.
1. If you want to execute everything in one place run bdAnalytics.py script.
2. Edit the config_template.py file and add your database user, port, host and password which will be used to establish connection in the other python scripts.
3. The first script that needs to be ran is the createDB.py. It will create the database and schema and load the raw data into the database table.
4. The next step is to run the cleanData.py script. It will perform the cleaning steps and move the clean data to a new table which can be further used for analysis.
5. Then you can review/execute the dataAnalysis.py and visualizeData.py scripts which perform the analysis queries and data visualization using heat maps respectively for section 2.
