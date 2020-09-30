import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data, processes songs and artist tables and loaded them back.
    Parameters:
            spark       = Spark Session
            input_data  = location of song_data where the file is loaded to process
            output_data = location of the results stored
    """
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    print('start reading song data')
    start = datetime.now()
    df = spark.read.json(song_data)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    df.printSchema()
    print('------------')

    # extract columns to create songs table
    
    print('start songs columns extractaction')
    start = datetime.now()
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
        SELECT distinct song_id, title, artist_id, year, duration 
        FROM songs_table
    """)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    songs_table.printSchema()
    songs_table.show(5)
    print('------------')
    
    # write songs table to parquet files partitioned by year and artist
    print('Start songs table writting')
    start = datetime.now()
    songs_path = output_data + 'songs_table'
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_path)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')
    
    # extract columns to create artists table
    print('start atrists columns extraction')
    start = datetime.now()
    df.createOrReplaceTempView("artists_table")
    artists_table = spark.sql("""
        SELECT distinct artist_id, 
            artist_name as name,
            artist_location as location, 
            artist_latitude as lattitude,
            artist_longitude as longitude
        FROM artist_data
    """)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    artists_table.printSchema()
    artists_table.show(5)
    print('------------')
    
    # write artists table to parquet files
    print('Start songs table writting')
    start = datetime.now()
    atrists_path = output_data + 'atrists_table'
    artists_table.write.mode("overwrite").parquet(atrists_path)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')


def process_log_data(spark, log_input, song_input, output_data):
    """
    This function loads log_data and song_data, processes users, time_table and songplays tables and loaded them back.
    Parameters:
            spark       = Spark Session
            log_input   = location of log_data where the file is loaded to process
            song_input  = location of song_data where the file is loaded to process
            output_data = location of the results stored
    """
    
    # get filepath to log data file
    log_data = log_input

    # read log data file
    print('Start reading logs')
    start = datetime.now()
    df = spark.read.json()
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    df.printSchema()
    print('------------')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table   
    print('Start users columns extracting')
    start = datetime.now()
    createOrReplaceTempView("users_table")
    users_table = spark.sql"""
        SELECT  DISTINCT userId    AS user_id,
            firstName AS first_name,
            astName  AS last_name,
            gender,
            level
        FROM users_table""")
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    users_table.printSchema()
    users_table.show(5)
    print('------------')
    
    # write users table to parquet files
    print('Start artists writting')
    start = datetime.now()
    users_path = output_data + "users_table"
    users_table.write.mode("overwrite").parquet(users_path)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')

    # create timestamp and datetime columns from original timestamp column
    print('start adding new timestamp and datetime columns')
    start = datetime.now()
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')
    
    df = df.withColumn('timestamp', get_timestamp('ts'))
    df = df.withColumn('datetime', get_datetime('ts'))
    
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    df.printSchema()
    print('------------')
    
    # extract columns to create time table
    print('Start time columns extracting')
    start = datetime.now()
    df.createOrReplaceTempView("time_table")
    time_table = spark.sql("""SELECT  DISTINCT datetime AS start_time,
                             hour(timestamp) AS hour,
                             day(timestamp)  AS day,
                             weekofyear(timestamp) AS week,
                             month(timestamp) AS month,
                             year(timestamp) AS year,
                             dayofweek(timestamp) AS weekday
                          FROM time_table""")
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    time_table.prinSchema()
    time_table.show(5)
    print('------------')                                         
    
    # write time table to parquet files partitioned by year and month
    print('Start time table writting')
    start = datetime.now()
    time_table_path = output_data + "time_table"
    time_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(time_table_path)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')   
    
    # read in song data to use for songplays table
    print('Start song data reading')
    start = datetime.now()
    song_df = spark.read.json(song_input)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')   

    # extract columns from joined song and log datasets to create songplays table 
    print('Start songplays columns exctraction')
    start = datetime.now()
    joined = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))
    joined.createOrReplaceTempView("songplays_table")
    songplays_table = spark.sql("""
                                SELECT  songplay_id AS songplay_id,
                                        timestamp   AS start_time,
                                        year(timestamp) AS year,
                                        month(timestamp) AS month, 
                                        userId      AS user_id,
                                        level       AS level,
                                        song_id     AS song_id,
                                        artist_id   AS artist_id,
                                        sessionId   AS session_id,
                                        location    AS location,
                                        userAgent   AS user_agent
                                FROM songplays_table
                                """)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    songplays_table.printSchema()
    songplays_table.show(5)
    print('------------')   

    # write songplays table to parquet files partitioned by year and month
    print('Start songplays writting')
    start = datetime.now()
    songplays_table_path = output_data + "songplays_table"
    songplays_table.wtime_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(songplays_table_path)
    print('finished:', f"{(datetime.now() - start).total_seconds()} s")
    print('------------')   

def main():
    spark = create_spark_session()
    
    # for local run
    sd_input = config['LOCAL']['SONG_DATA']
    ld_input = config['LOCAL']['LOG_DATA']
    output_data = config['LOCAL']['OUTPUT_DATA']
    
    # for AWS run
#     sd_input = config['AWS']['SONG_DATA']
#     ld_input = config['AWS']['LOG_DATA']
#     output_data = config['AWS']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
