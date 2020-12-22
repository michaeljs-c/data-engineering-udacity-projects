# Postgres ETL Project - Sparkify

Project: Data Modeling with Postgres.

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We have been hired to create a Postgres database with tables designed to optimize queries on song play analysis. Our role is to create a database schema and ETL pipeline for this analysis. We will test our database and ETL pipeline by running queries given by the analytics team from Sparkify and compare results with their expected results.

To complete the project, we need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Schema for Song Play Analysis
We will create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- users - users in the app
  - user_id, first_name, last_name, gender, level
- songs - songs in music database
  - song_id, title, artist_id, year, duration
- artists - artists in music database
  - artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

### Purpose of Star Schema
A star schema consists of a fact table at its centre and dimension tables surround the fact table representing the stars points.
<p> Benefits in this case: <p>

- De-normalised
- Simplifies queries for analytics
- Fast aggregations
- Data is structured and consistent, so the transformations remain the same and therefore we do not need a more flexible schema
- Dataset not big enough to justify nosql solutions

### Project Structure

1. **test.ipynb** displays the first few rows of each table to let you check your database.
2. **create_tables.py** drops and creates tables. Run this file to reset tables before running the ETL scripts.
3. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** reads and processes files from song_data and log_data and loads them into the tables.
5. **sql_queries.py** contains sql queries, imported into the last three files above.
6. **README.md** provides a detailed discussion of the project.

### Project Steps

1. Write CREATE statements in sql_queries.py to create each table.
2. Write DROP statements in sql_queries.py to drop each table if it exists.
3. Run create_tables.py to create your database and tables.
4. Run test.ipynb to confirm the creation of tables with the correct columns.
5. Run etl.py to insert all data into the database. The steps taken in this file are based on the validated code in etl.ipynb.
6. Perform queries in test.ipynb to confirm data was entered correctly with no issues.
7. Complete README.md documentation

### ETL Pipeline
1. etl.py starts by connecting to the Postgres database, sparkifydb.
2. We accumulate a list of song files by walking through the directories. For each json file we process using the function process_song_file.
3. pd.read_json is used to create a DataFrame and select the columns we need from the files.
4. Processed data from song files are inserted into tables 'songs' and 'artists'.
5. Repeat second step for log data while using the function process_log_file.
6. Data are processed and inserted into tables 'songplays', 'users', 'time'.
