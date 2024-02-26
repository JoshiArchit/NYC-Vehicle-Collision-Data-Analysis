# CSCI720-Big-Data-Analytics-Project
CSCI720-Big Data Analytics Project

# The Idea
<aside>
🚗 NYC sure has a lot of vehicle collisions. You can find a large amount of data pooled from NYC Open Data -

[Motor Vehicle Collisions - Crashes | NYC Open Data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)

The above data pool contains over a million records. Due to hardware and time-constraints we choose to only with data between summer 2019 and summer 2020.

</aside>

# The Objective
<aside>
🗯️ We aim to design a solution to extract, clean and efficiently store this data so that it can be queried intelligently using SQL. Through this process of data mining we answer the below fundamental questions -

1. For the two years given, figure out what has changed in the summer from one year to the next.
Figure out how to visualize the difference, in some way.
2. How was June of 2019 different then June of 2020?
Figure out how to show or demonstrate the difference.
3. How was July of 2019 different then July of 2020?
Figure out how to show or demonstrate the difference.
4. For the year of January 2019 to October of 2020, which 100 consecutive days had the most accidents?
5. Which day of the week has the most accidents?
6. Which hour of the day has the most accidents?
7. In the year 2020, which 12 days had the most accidents?
Can you speculate about why this is?
</aside>

# The Tech Stack
The following dependencies need to be installed on the machine - folium, pandas, psycopg2, branca, sklearn and postgreSQL.
1. If you want to execute everything in one place run bdAnalytics.py script.
2. Edit the config_template.py file and add your database user, port, host and password which will be used to establish connection in the other python scripts.
3. The first script that needs to be ran is the createDB.py. It will create the database and schema and load the raw data into the database table.
4. The next step is to run the cleanData.py script. It will perform the cleaning steps and move the clean data to a new table which can be further used for analysis.
5. Then you can review/execute the dataAnalysis.py and visualizeData.py scripts which perform the analysis queries and data visualization using heat maps respectively for section 2.
