OBJECTIVE
Sparkify is a music streaming app and as part of user-analysis, they analyze the data which are mainly composed of log files on songs and user activity. The primary objective of the project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

DATABASE DESIGN
So as part of this, the database must be designed in such a way that that we can create a optimized query on both the songs and the users. Star schema will be used for the schema design with 5 tables namely songplay, which will be the fact table containing information about the user usage and hence a fact table, users, which will have information about the users using the app, songs, which will have information about the songs played in the app, artists, which will have information about the artists playing the songs played in the app, and time, which will have information about the timestamps of the records in songplay broken into time,day,month,week,weekday and year. The songplay table will be associated through the primary keys of the dimension tables, namely user_id from the users table, song_id from the song table, artist_id from the artists table, and start_time from time table.Since the primary objective of the analysis is to understand what the users are listening to, the songplay table will be helpful in gving us facts related to usage by combining the information referred from other dimension tables.

ETL PIPELINING
Before ETL is peformed, two staging tables are created in Redshift to which the datasets from S3 bucket created for songs and user log respectively will be copied using copy command. Once the staging tables are created, ETL is done to extract the data from the tables, transform it to remove redundant columns and change datatype, and loaded into the dimensional model created in star schema with songplays as the fact table and users, artists, songs and time as dimension tables.

CODE EXECUTION
In order to execute the python files:


1) First create the iam roles, cluster, database and tables using create_tables.py

python create_tables.py

2) Then run the etl.py for processing the data and inserting them to the tables

python etl.py
