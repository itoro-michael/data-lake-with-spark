# Data Lakes with Spark

A music streaming startup, has grown their user base and song database and wants to move their data processing onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity
on the app, as well as a directory with JSON metadata on the songs in their app.

The data lake project builds an ETL pipeline that extracts Sparkify's data from S3, processes them in Spark, and loads the data back into S3 as a set of dimensional tables. The analytics team will continue finding insights on what songs are listened to, by Sparkify users. 


## Files in the project:

1. dl.cfg: Contains Spark config settings.
2. etl.py: Extracts data from S3, processes with Spark, and writes it back to S3.


## Data lake schema
The data lake follows a star schema design because it is simple, efficient and reduces the number of
joins required to execute a business query.

| Table file | Description |
| ---- | ---- |
| songplays.parquet | Fact table for song playing events | 
| users.parquet | Dimension table for app users | 
| songs.parquet | Dimension table for songs played in app | 
| artists.parquet | Dimension table for song artists | 
| time_table.parquet | Dimension table for timestamps | 

### Instructions:

Run the following command in the project's root directory to run spark job.

    - Create fact and dimensional tables from json logs
        `python etl.py`