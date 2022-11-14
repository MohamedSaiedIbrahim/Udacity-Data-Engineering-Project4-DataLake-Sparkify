import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import (TimestampType)


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Spark session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function reads in song data in JSON format from S3; defines the schema
    of songs and artists analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to song data file
    log_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(log_data)
    df.printSchema()
    df.show(3)

    # extract columns to create songs table
	#song_id, title, artist_id, year, duration
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("{}/songs.parquet".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude")

    # write artists table to parquet files
    artists_table.write.parquet("{}/artists.parquet".format(output_data), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in JSON format from S3; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read log data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to log data file
    log_data = "{}log-data/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.printSchema()
    df.show(3)

    # extract columns for users table    
	#user_id, first_name, last_name, gender, level   
    users_table = df.filter(df.userId.isNotNull()).selectExpr('userId as user_id','firstName as first_name','lastName as last_name','gender','level').distinct()
    
	# write users table to parquet files
    users_table.write.parquet("{}/users.parquet".format(output_data), mode="overwrite")

    # create timestamp column from original timestamp column
    #get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),TimestampType())
    #df = df.withColumn("start_time", get_timestamp("ts"))
    
    #workaround as the function gives an error and couldn't fix
    df.createOrReplaceTempView("df") 
    df = spark.sql('select artist, auth, firstName, gender, \
                   itemInSession, lastName, length, level, location, \
                   method, page, registration, \
                   sessionId, song, status, ts, \
                   userAgent, userId, from_unixtime(ts/1000) as start_time \
                   from df')

    # extract columns to create time table (4th dimension table)
    #start_time, hour, day, week, month, year, weekday
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofmonth("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("weekday", dayofweek("start_time"))
    
    #To avoid duplications, please use distinct
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday").distinct()

	# write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet("{}/time.parquet".format(output_data), mode="overwrite")

	# read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))
    song_df.show(3)
    
    #We need the Artist Name from Artist table also
    artists_df = spark.read.parquet("{}/artist.parquet".format(output_data))
    artists_df.show(3)
    
    songs = (
        song_df
        .join(artists_df, "artist_id", "full")
        .select("song_id", "title", "artist_id", "name", "duration")
    )

    # extract columns from joined song and log datasets to create songplays table 
	#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_df = df.join(
        songs,
        [
            df.song == songs.title,
            df.artist == songs.name,
            df.length == songs.duration
        ],
        "left"
    ) 
    
    songplays_table = (songplays_df.join(time_table, "start_time", "left") \
        .select( \
            "start_time", \
            col("userId").alias("user_id"), \
            "level", \
            "song_id", \
            "artist_id", \
            col("sessionId").alias("session_id"), \
            "location", \
            col("userAgent").alias("user_agent"), \
            songplays_df.year, \
            songplays_df.month \
            ).withColumn("songplay_id", monotonically_increasing_id())
        )
    
	# write songplays table to parquet files partitioned by year and month 
    songplays_table.write.partitionBy(['year','month']).parquet("{}/songplays.parquet".format(output_data), mode="overwrite")


def main():
    """Run ETL pipeline"""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-msaied/Project4_Sparkify_DataLake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
