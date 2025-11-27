from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys
assert sys.version_info >= (3, 5)
import os
from pyspark.sql import SparkSession, functions, types, Window
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import boto3
import pytz

#glue passes RDS credentials as job parameters which are specified in glue job parameters through AWS console
args = getResolvedOptions(sys.argv, [
    'DB_HOST',
    'DB_NAME',
    'DB_PASSWORD',
    'DB_PORT',
    'DB_USER'
])

#extracting these variables
DB_HOST = args['DB_HOST']
DB_NAME = args['DB_NAME']
DB_PASSWORD = args['DB_PASSWORD']
DB_PORT = args['DB_PORT']
DB_USER = args['DB_USER']

#JDBC configuragion for Spark -> PostgreSQK connection
POSTGRES_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
POSTGRES_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

#jar in S3 so that glue can load PostgreSQL driver
POSTGRES_JAR = "s3://c732-sfu-parking-data-lake/scripts/postgresql-42.7.8.jar"
#input source
PROCESSED_PREFIX = "s3://c732-sfu-parking-data-lake/processed/historical/simulator_data/"
#output source
OUTPUT_TABLE = "historical_metrics"

#parking capacities
CAPACITIES_DATA = [
    ("NORTH", 2000), ("EAST", 1500), ("CENTRAL", 600), ("WEST", 500), ("SOUTH", 400),
    ("SRYC", 450), ("SRYE", 450)
]
#occupancy threshhold above which a peak skike is detected
PEAK_THRESHOLD = 0.80


#getting yesterday's Vancouver time 
def get_previous_day_vancouver():
    tz = pytz.timezone("America/Vancouver")
    now_van = datetime.now(tz)
    yesterday = now_van - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


#checking if data partition exists
def s3_path_has_files(base_prefix, target_date):
    s3 = boto3.client("s3")

    bucket = base_prefix.replace("s3://", "").split("/")[0]
    prefix_root = "/".join(base_prefix.replace("s3://", "").split("/")[1:])

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix_root, Delimiter="/")
    if "CommonPrefixes" not in resp:
        return False
    campus_prefixes = [cp["Prefix"] for cp in resp["CommonPrefixes"]]

    for campus_prefix in campus_prefixes:
        date_prefix = f"{campus_prefix}date={target_date}/"
        resp2 = s3.list_objects_v2(Bucket=bucket, Prefix=date_prefix)
        if "Contents" in resp2:
            return True
    return False

#connecting to postgresql
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=5
    )

#creating a table if not exists (used only when the code is run for the first time)
def create_table(cur, output_table):
    create_table_query = sql.SQL(f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            lot_id VARCHAR(10) NOT NULL,
            day_of_week VARCHAR(3) NOT NULL,
            hour_of_day INTEGER NOT NULL,
            average_occupancy_lot NUMERIC(10, 4),
            average_duration_minutes NUMERIC(10, 2),
            peak_spikes_count INTEGER,
            PRIMARY KEY (lot_id, day_of_week, hour_of_day)
        );
    """)
    cur.execute(create_table_query)

#upsert logic: write to staging table -> merge into target
def upsert_the_data(df, output_table, properties):
    temporary_table = "staging_" + output_table

    #write Spark DF to temporary postgresql table
    df.write.jdbc(
        url=POSTGRES_URL,
        table=temporary_table,
        mode="overwrite",
        properties=properties
    )

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        #ensuring output table exists
        create_table(cur, output_table)

        #UPSERT merges existing rows on (lot, day, hour)
        upsert_query = sql.SQL(f"""
            INSERT INTO {output_table} (lot_id, day_of_week, hour_of_day, average_occupancy_lot, average_duration_minutes, peak_spikes_count)
            SELECT
                t.lot_id, t.day_of_week, t.hour_of_day, t.average_occupancy_lot, t.average_duration_minutes, t.peak_spikes_count
            FROM {temporary_table} t
            ON CONFLICT (lot_id, day_of_week, hour_of_day) DO UPDATE SET
                average_occupancy_lot = EXCLUDED.average_occupancy_lot,
                average_duration_minutes = EXCLUDED.average_duration_minutes,
                peak_spikes_count = EXCLUDED.peak_spikes_count;
        """)

        cur.execute(upsert_query)
        conn.commit()
        print(f"UPSERTED data from {temporary_table} to {output_table}.")

    except Exception as e:
        print(f"UPSERT failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

#hourly summarization
def historical_summarizer(spark):
    
    yesterday_str = get_previous_day_vancouver()
    base_prefix = "s3://c732-sfu-parking-data-lake/processed/historical/simulator_data/"
    print(f"Checking partitions for {yesterday_str} ...")
    
    if not s3_path_has_files(base_prefix, yesterday_str):
        print(f"No data found for yesterday ({yesterday_str}). ETL terminated.")
        return
    
    raw_root = base_prefix  

    #loading all simulator data from S3
    df_all = spark.read.parquet(raw_root)
    #filter specifically for yesterday's date
    raw_events_df = df_all.filter(functions.col("date") == yesterday_str)

    expected_cols = ["session_id", "timestamp", "event_type", "lot_id", "student_id", "plate_number", "date"]
    raw_events_df = raw_events_df.select(*expected_cols)

    #ensuring proper dat type and caching for future use
    raw_events_df = raw_events_df.withColumn("date", functions.to_date("date"))
    raw_events_df.cache()

    #building arrival/departure pairs
    sessions_df = (raw_events_df.filter(functions.col("event_type").isin("ARRIVAL", "DEPARTURE"))
                   .select("session_id", "timestamp", "event_type"))

    session_pairs = (sessions_df.groupBy("session_id").pivot("event_type").agg(functions.max("timestamp"))
                     .withColumnRenamed("ARRIVAL", "arrival_ts")
                     .withColumnRenamed("DEPARTURE", "departure_ts")
                     .dropna(subset=["arrival_ts", "departure_ts"]))
    #duration = difference between timestamps in min
    duration_df = (session_pairs.withColumn(
        "duration_minutes",
        (functions.col("departure_ts").cast("long") - functions.col("arrival_ts").cast("long")) / 60
    ).select("session_id", "duration_minutes"))
    #hourly occupancy : arrival +1, departure -1
    occupancy_change_df = raw_events_df.withColumn(
        "occupancy_change",
        functions.when(functions.col("event_type") == "ARRIVAL", 1).otherwise(-1)
    ).filter(functions.col("event_type").isin("ARRIVAL", "DEPARTURE"))

    #for each lot on each date compute running occupancy level
    window_spec = Window.partitionBy("lot_id", "date").orderBy("timestamp")

    running_occupancy_df = occupancy_change_df.withColumn(
        "current_occupancy",
        functions.sum("occupancy_change").over(window_spec)
    )
    #left-join durations
    occupied_duration_df = running_occupancy_df.join(duration_df, on="session_id", how="left_outer")
    #adding lot capacity
    capacities_df = spark.createDataFrame(CAPACITIES_DATA, ["lot_id", "capacity"])
    final_join_df = occupied_duration_df.join(capacities_df, on="lot_id", how="left")

    #adding time dimensions for grouping
    time_events_df = (final_join_df
                      .withColumn("day_of_week", functions.date_format(functions.col("date"), "E"))
                      .withColumn("hour_of_day", functions.hour(functions.col("timestamp"))))
    #group by lot/weekday/hour to compute avg duration, avg occupancy, MAX occupancy, capacity
    final_metrics_df = (time_events_df.groupBy("lot_id", "day_of_week", "hour_of_day")
                        .agg(
                            functions.avg("duration_minutes").alias("average_duration_minutes"),
                            functions.avg("current_occupancy").alias("average_occupancy_lot"),
                            functions.max("current_occupancy").alias("max_occupancy_hourly"),
                            functions.max("capacity").alias("lot_capacity")
                        ))
    #count peak spikes (occupancy > 80%)
    final_df_with_spikes = final_metrics_df.withColumn(
        "peak_spikes_count",
        functions.when(
            (functions.col("max_occupancy_hourly") / functions.col("lot_capacity")) >= PEAK_THRESHOLD,
            1
        ).otherwise(0)
    )
    #columns to upsert
    final_df_for_upsert = final_df_with_spikes.select(
        "lot_id",
        "day_of_week",
        "hour_of_day",
        "average_occupancy_lot",
        "average_duration_minutes",
        "peak_spikes_count"
    )

    #upsert
    upsert_the_data(final_df_for_upsert, OUTPUT_TABLE, POSTGRES_PROPERTIES)
    #release cache
    raw_events_df.unpersist()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("SFU History Summarizer").config("spark.jars", POSTGRES_JAR).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    try:
        historical_summarizer(spark)
    except Exception as e:
        print(f"History Summarizer ETL failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()