import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions as F, types
from datetime import datetime, timedelta
import pytz


#configurations
# Raw simulator event logs: s3://.../raw/realtime/<lot_id>/<YYYY-MM-DD>/
# Output : s3://.../processed/historical/simulator_data/
INPUT_PREFIX = "s3://c732-sfu-parking-data-lake/raw/realtime/"
OUTPUT_S3_PATH = "s3://c732-sfu-parking-data-lake/processed/historical/simulator_data/"

#PURPOSE: compute yesterday according to the Vancouver timezone
def get_previous_day_vancouver():
    tz = pytz.timezone("America/Vancouver")
    now_van = datetime.now(tz)
    yesterday_van = now_van - timedelta(days=1)
    return yesterday_van.strftime("%Y-%m-%d")

#Main ETL
def run_etl_job(spark):

    print("Starting Processing Raw events ETL job...")
    
    #selecting the date folder to process
    yesterday = get_previous_day_vancouver()
    full_day_path = f"{INPUT_PREFIX}*/{yesterday}/"
    print(f"Yesterday's data: {full_day_path}")

    #events are written as individual Parquet files
    try:
        raw_df = spark.read.parquet(full_day_path)
    except Exception:
        #if there is no data for yesterday - terminate the job
        print(f"No data found for yesterday ({yesterday}). ETL terminated.")
        return
    #if folder is empty - terminate the job
    if raw_df.rdd.isEmpty():
        print(f"No files for {yesterday}.")
        return

    print("Raw data loaded successfully.")

    #consistent schema
    expected_cols = [
        "session_id", "timestamp", "event_type",
        "lot_id", "campus", "student_id", "plate_number"
    ]
    
    #converting strings to timestamp type
    raw_df = raw_df.select(*expected_cols)
    raw_df = raw_df.withColumn("timestamp",F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))

    raw_df = raw_df.withColumn("date",F.to_date(F.regexp_extract(F.input_file_name(), r".*/(\d{4}-\d{2}-\d{2})/", 1)))
            
            

    #adding missing departures 
    arrivals = raw_df.filter("event_type = 'ARRIVAL'").select(
        "session_id", F.col("timestamp").alias("arrival_ts"),
        "lot_id", "campus", "student_id", "plate_number", "date"
    )
    departures = raw_df.filter("event_type = 'DEPARTURE'").select(
        "session_id", F.col("timestamp").alias("departure_ts")
    )
    #keeping all arrival sessions even if there is no departure (left join)
    paired = arrivals.join(departures, on="session_id", how="left")

    # Missing departure -> departure = arrival + 4 hours
    missing_dep = paired.filter("departure_ts IS NULL") \
        .withColumn(
            "departure_ts",
            F.col("arrival_ts") + F.expr("INTERVAL 4 HOURS")
        )
    #combining pairs that already have valid departures
    complete_pairs = paired.filter("departure_ts IS NOT NULL") \
        .unionByName(missing_dep)
    #converting departures into proper event rows
    fixed_departures = complete_pairs.select(
        "session_id",
        F.col("departure_ts").alias("timestamp"),
        F.lit("DEPARTURE").alias("event_type"),
        "lot_id", "campus", "student_id", "plate_number", "date"
    )
    #preserving original arrival order and timestamps
    fixed_arrivals = raw_df.filter("event_type = 'ARRIVAL'").select(
        "session_id", "timestamp", "event_type",
        "lot_id", "campus", "student_id", "plate_number", "date"
    )
    #combining arrival with departure evengts
    fixed_events = fixed_arrivals.unionByName(fixed_departures)
    #ordering by time
    fixed_events = fixed_events.orderBy("timestamp")

    print("Writing processed data...")
    #writing clean events partitioned by campus and date
    (fixed_events
        .write
        .mode("overwrite")
        .partitionBy("campus", "date")
        .parquet(OUTPUT_S3_PATH))

    print("ETL job completed successfully.")

def main():
    spark = SparkSession.builder.appName("Realtime-to-Historical-ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        run_etl_job(spark)
    except Exception as e:
        print(f"ETL job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()