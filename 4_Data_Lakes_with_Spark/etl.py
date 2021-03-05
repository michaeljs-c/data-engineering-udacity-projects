import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This functions reads in song and artist data, then loads them into
    parquet files within the s3 bucket
    
    Inputs: 
        spark : spark session
        input_data : data on the s3 bucket to be retrieved
        output_data : s3 bucket where tables will be stored
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("title", "artist_id", "year", "duration").withColumn("song_id", monotonically_increasing_id).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", 
                                  "artist_name as name", 
                                  "artist_location as location", 
                                  "artist_lattitude as lattitude", 
                                  "artist_longitude as longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    This functions reads in log data, then loads tables into
    parquet files within the s3 bucket
    
    Inputs: 
        spark : spark session
        input_data : data on the s3 bucket to be retrieved
        output_data : s3 bucket where tables will be stored
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", 
                                  "firstName as first_name", 
                                  "lastName as last_name", 
                                  "gender", 
                                  "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + '/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), T.TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time").\
                withColumn("hour", hour(col("start_time"))).\
                withColumn("day", dayofmonth(col("start_time"))).\
                withColumn("week", weekofyear(col("start_time"))).\
                withColumn("year", year(col("start_time"))).\
                withColumn("weekday", date_format("start_time", 'EEEE'))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_date + 'songs/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, 
                              (df.song == song_df.title) \
                              & (df.artist == song_df.artist_name) \
                              & (df.length == song_df.duration))\
    
    songplays_table = songplays_table.select(
                                        "start_time",
                                        "user_id",
                                        "level",
                                        "song_id",
                                        "artist_id",
                                        "session_id",
                                        "location",
                                        "user_agent",
                                        "year",
                                        "month",        
    )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """
    main function to extract sparkify data using above functions,
    transform into dimensional tables and load back into s3 bucket
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-spark-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
