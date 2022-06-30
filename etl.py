import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates spark session object.
    Parameters:
        None
    Returns:
        SparkSession: Spark session object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Reads JSON files from S3, creates dimension tables from the files, 
    and writes the tables back to S3 as parquet files.
    Parameters:
        spark (SparkSession): A spark session object.
        input_data (str): The S3 bucket url for data input.
        output_data (str): The S3 bucket url for data output.
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file from s3
    df = spark.read.json(song_data)
    df.persist()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id') \
               .parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", 
                               "artist_latitude", "artist_longitude"]) \
                      .dropDuplicates() \
                      .withColumnRenamed("artist_name", "name") \
                      .withColumnRenamed("artist_location", "location") \
                      .withColumnRenamed("artist_latitude", "latitude") \
                      .withColumnRenamed("artist_longitude", "longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data + 'artists'))


def process_log_data(spark, input_data, output_data):
    """ Reads JSON files from S3, creates dimension and fact tables from the files, 
    and writes the tables back to S3 as parquet files.
    Parameters:
        spark (SparkSession): A spark session object.
        input_data (str): The S3 bucket url for data input.
        output_data (str): The S3 bucket url for data output.
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # get filepath to song data file
    song_data = output_data + "songs"
    
    # get filepath to artist data file
    artist_data = output_data + "artists"

    # read log data file from s3
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.select(df.columns).where(df.page == "NextSong") \
           .dropDuplicates()
    

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column    
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column    
    get_datetime = udf(lambda x: x)
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    df = df.withColumn("hour", hour("timestamp"))
    
    df = df.withColumn("day", dayofmonth("timestamp"))
    
    df = df.withColumn("week",  weekofyear("timestamp"))
    
    df = df.withColumn("month", month("timestamp"))
    
    df = df.withColumn("year", year("timestamp"))
    
    df = df.withColumn("weekday", dayofweek("timestamp"))
    
    # extract columns to create time table
    time_table = df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year', 'month') \
              .parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(song_data)
    
    # read in artists data to use for songplays table
    artist_df = spark.read.parquet(artist_data).withColumnRenamed("artist_id", "id") \
                                               .withColumnRenamed("location", "artist_location")
    
    # Join song and artist table
    song_artist_df = song_df.join(artist_df, song_df.artist_id == artist_df.id, how='inner')

    # extract columns from joined song_artist and log datasets to create songplays table 
    songplays_table = df.join(song_artist_df, \
                                (df.artist == song_artist_df.name) & \
                                (df.length == song_artist_df.duration) & \
                                (df.song == song_artist_df.title), how='inner')
    
    songplays_table = songplays_table.select(['start_time','userId', 'level', 'sessionId', \
                                              'location', 'userAgent', 'artist_id','song_id'])
    
    # Rename songplays columns
    songplays_table = songplays_table.withColumnRenamed("userId", "user_id") \
                                     .withColumnRenamed("sessionId", "session_id") \
                                     .withColumnRenamed("userAgent", "user_agent")
    
    
    # Create songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", \
                                                 monotonically_increasing_id())
    
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output_data + 'songplays')


def main():
    """ The module main function.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://spark-data-1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
