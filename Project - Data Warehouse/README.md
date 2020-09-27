## Project Summary
This projects extracts data from S3, stages the data in Redshift Cluster, and transforms data into a set of dimensional tables for the analytics team.

The data on S3 contains song and log information from a music store. 

## Purpose of this project
This solution provides the analysis team with data in an optimized structure for analysis purposes.


## Project instructions
1. Setup a redshift cluster on AWS `create_cluster.cfg`.
2. Create the staging, facts and dimension tables `create_tables.py`.
4. Load data from S3 bucket to staging tables and process this data and insert it on facts and dimension tables `etl.py`.
5. we analyze the total of records in stagings, fact and dimension tables `analytics.py`.


## Project Datasets
| Dataset | path |
| ---- | ---- |
| Song data | s3://udacity-dend/song_data |
| Log data | s3://udacity-dend/log_data |
| Log data JSON | s3://udacity-dend/log_json_path.json | 


## Database schema
| Table | Type | Structure |
| ---- | ---- | ---- |
| songplays | Fact | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent |
| users | Dimension | user_id, first_name, last_name, gender, level |
| songs | Dimension | song_id, title, artist_id, year, duration |
| artists | Dimension | artist_id, name, location, lattitude, longitude |
| time | Dimension | start_time, hour, day, week, month, year, weekday |



