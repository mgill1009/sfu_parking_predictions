import json
import os
import base64
import psycopg2
from psycopg2 import sql
from typing import Dict, Any

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

def get_db_connection():
    # establishes and returns a PostgreSQL database connection
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        raise EnvironmentError("Database connection variables are not set.")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=5
    )

#Inserting the new event into 'recent_events' and cleaning up old records (keep MAX of 5 events per lot)
def update_recent_events(event_data: Dict[str, Any], conn) -> None:

    lot_id = event_data['lot_id']
    campus = event_data['campus']
    event_type = event_data['event_type']
    # Format timestamp for PostgreSQL compatibility
    timestamp = event_data['timestamp'].replace('Z', '+00:00')

    #inserting the new event
    insert_sql = sql.SQL("""
        INSERT INTO recent_events 
            (lot_id, campus_name, event_type, event_timestamp)
        VALUES 
            (%s, %s, %s, %s)
    """)

    #deleting records that are NOT in the top 5 most recent for the specific lot
    delete_sql = sql.SQL("""
        WITH ranked_events AS (
            SELECT 
                event_id,
                ROW_NUMBER() OVER (PARTITION BY lot_id ORDER BY event_timestamp DESC) as rn
            FROM 
                recent_events
            WHERE
                lot_id = %s -- Only consider the current lot
        )
        DELETE FROM 
            recent_events
        WHERE 
            event_id IN (SELECT event_id FROM ranked_events WHERE rn > 5)
    """)
    
    with conn.cursor() as cur:
        cur.execute(insert_sql, (lot_id, campus, event_type, timestamp))
        cur.execute(delete_sql, (lot_id,))
        deleted_count = cur.rowcount

    conn.commit() 
    print(f"SUCCESS: recent_events updated for {lot_id}. Cleaned up {deleted_count} old records.")


def lambda_handler(kinesis_event, context):
    # Kinesis stream processor function. Reads records and updates PostgreSQL
    logger.info("psycopg2 loaded from: %s", psycopg2.__file__)
    records_processed = 0
    try:
        logger.info("Connecting to Postgres...")
        conn = get_db_connection()
        logger.info("Connected.")
        # process records from the Kinesis batch
        for record in kinesis_event['Records']:
            try:
                payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                event_data = json.loads(payload)

                #processing the individual event
                update_recent_events(event_data, conn)

                records_processed += 1

            except Exception as e:
                print(f"Error processing single record: {e} | Record: {record.get('kinesis', {}).get('data')}")

    except EnvironmentError as e:
        print(f"FATAL: Configuration error. {e}")
        raise

    except Exception as e:
        print(f"FATAL: Database or connection error during batch: {e}")
        raise

    finally:
        if 'conn' in locals() and conn:
            conn.close()

    print(f"Batch completed. Total records processed: {records_processed}")

    return f"Successfully processed {records_processed} records."
