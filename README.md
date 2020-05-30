{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "# OBJECTIVE"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "Sparkify is a music streaming app and as part of user-analysis, they analyze the data which are mainly composed of log files on songs and user activity. The primary objective of the project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## DATABASE DESIGN"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "So as part of this, the database must be designed in such a way that that we can create a optimized query on both the songs and the users. Star schema will be used for the schema design with 5 tables namely songplay, which will be the fact table containing information about the user usage and hence a fact table, users, which will have information about the users using the app, songs, which will have information about the songs played in the app, artists, which will have information about the artists playing the songs played in the app, and time, which will have information about the timestamps of the records in songplay broken into time,day,month,week,weekday and year. The songplay table will be associated through the primary keys of the dimension tables, namely user_id from the users table, song_id from the song table, artist_id from the artists table, and start_time from time table.Since the primary objective of the analysis is to understand what the users are listening to, the songplay table will be helpful in gving us facts related to usage by combining the information referred from other dimension tables."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## ETL PIPELINING"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "Before ETL is peformed, two staging tables are created in Redshift to which the datasets from S3 bucket created for songs and user log respectively will be copied using copy command. Once the staging tables are created, ETL is done to extract the data from the tables, transform it to remove redundant columns and change datatype, and loaded into the dimensional model created in star schema with songplays as the fact table and users, artists, songs and time as dimension tables."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## CODE EXECUTION"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "In order to execute the python files:\n",
    "\n",
    "    1) First create the iam roles, cluster, database and tables using create_tables.py\n",
    "    \n",
    "    python create_tables.py\n",
    "    \n",
    "    2) Then run the etl.py for processing the data and inserting them to the tables\n",
    "    \n",
    "    python etl.py db\n",
    "    \n",
    "    3) To delete the cluster\n",
    "    \n",
    "    python etl.py delete_cluster"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
