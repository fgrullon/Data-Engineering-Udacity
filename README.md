
# Projects

Project Folder | Description | Done
------------ | ------------- | -------------
[Project 1a - PostgreSQL] | Building a star schema in PostgreSQL and inserting data via Python| :heavy_multiplication_x:
[Project 1b - Cassandra] | Building a star schema in Cassandra and inserting data via Python | :heavy_multiplication_x:
[Project 2 - AWS Redshift]| Building a star schema in AWS Redshift and inserting data from AWS S3 via Python | :heavy_multiplication_x:
[Project 3 - Spark]| Reading and transforming data from AWS S3 with Spark to parse them in partitioned parquet files | :heavy_multiplication_x:
[Project 4 - Airflow Pipelines]| Building an Airflow Pipeline to automate parsing and transforming files from AWS S3 to AWS Redshift | :heavy_multiplication_x:
[Project 5 - Capstone Project]| Integrating files from S3 into PostgreSQL via Spark | :heavy_multiplication_x:


Status | Symbol 
------------ | ------------- 
Completed |  :heavy_check_mark:
Pending | :heavy_multiplication_x:



# Data Modeling with Postgres

## **Overview**
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## **Requirements**
Create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## **Song Dataset**
Songs dataset is a subset of [Million Song Dataset](http://millionsongdataset.com/).


## **Log Dataset**
Logs dataset is generated by [Event Simulator](https://github.com/Interana/eventsim).


## Schema

#### Fact Table 
**songplays** - records in log data associated with song plays

```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

#### Dimension Tables
**users**  - users in the app
```
user_id, first_name, last_name, gender, level
```
**songs**  - songs in song_data files
```
song_id, title, artist_id, year, duration
```
**artists**  - artists in song_data files
```
artist_id, name, location, latitude, longitude
```
**time**  - timestamps of records in  **songplays**  broken down into specific units
```
start_time, hour, day, week, month, year, weekday
```


## How to run

First we run the ```create_tables.py``` to drop if the tables exists and create then. 
```
python create_tables.py 
```

Then we run ```etl.py``` the execute the pipeline:
```
python etl.py 
```

For test purpose we hasve the we run ```test.ipynb``` to test if the data was processed and stored correctly in the database.
