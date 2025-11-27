from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
import os
import pytz

# configurations, loaded from environment details specified in EB
DB_HOST = os.environ.get('DB_HOST') 
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

#Establishing a connection to PostgreSQL 
def get_db_connection():
    try:
        return connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5
        )
    except Exception as e:
        print(f"ERROR: Couldn't connect to database: {e}")
        return None


#Returns: real-time occupancy for all lots within a campus
#Used by: /api/lots/<campus>
def fetch_current_lot_status(campus: str) -> list:
    conn = get_db_connection()
    if not conn:
        return []
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            lot_id,
            campus_name,
            capacity,
            current_occupancy,
            free_spots,
            occupancy_rate,
            last_updated_ts
        FROM current_lot_occupancy
        WHERE campus_name = UPPER(%s)
        ORDER BY lot_id;
    """
    try:
        cur.execute(query, (campus.upper(),))
        lots = cur.fetchall()
        return lots
    except Exception as e:
        print(f"Real-time Occupancy Database query error: {e}")
        return []
    finally:
        cur.close()
        conn.close()

#Returns: 5 most recent events for the selected lot
#Used by: /api/lot_details
def fetch_recent_activities(lot_id: str) -> list:
    conn = get_db_connection()
    if not conn:
        return []
    
    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = """
        SELECT 
            lot_id,
            campus_name,
            event_type,       
            event_timestamp   
        FROM recent_events     
        WHERE lot_id = %s
        ORDER BY event_timestamp DESC
        LIMIT 10;
    """
    try:
        cur.execute(query, (lot_id,))
        rows = cur.fetchall()
        for r in rows:
            r['date'] = r['event_timestamp'].strftime('%Y-%m-%d')
            r['time'] = r['event_timestamp'].strftime('%H:%M:%S') 
            r['event'] = r['event_type'] 
        return rows
    except Exception as e:
        print(f"Recent Activities Database query error for activities: {e}")
        return []
    finally:
        cur.close()
        conn.close()

#Returns: avg occupancy per hour for the current weekday
def fetch_historical_occupancy(lot_id: str):
    conn = get_db_connection()
    if not conn:
        return []
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # today’s weekday 
    today = datetime.now()
    day_name = today.strftime('%a')

    query = """
        SELECT
            hour_of_day,
            average_occupancy_lot,
            peak_spikes_count
        FROM historical_metrics
        WHERE lot_id = %s AND day_of_week = %s
        ORDER BY hour_of_day ASC;
    """
    try:
        cur.execute(query, (lot_id, day_name))
        return cur.fetchall()
    except Exception as e:
        print(f"Historical Occupancy Database query error: {e}")
        return []
    finally:
        cur.close()
        conn.close()

#Returns: avg occupancy and stay duration for the current hour
def fetch_current_hour_metrics(lot_id: str):
    conn = get_db_connection()
    if not conn:
        return {"avg_occ": 0, "avg_dur": 0}

    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Convert now → Pacific Time
    pacific = pytz.timezone("America/Vancouver")
    now = datetime.now(pacific)

    day_name = now.strftime('%a')
    hour = now.hour

    query = """
        SELECT average_occupancy_lot, average_duration_minutes
        FROM historical_metrics
        WHERE lot_id = %s AND day_of_week = %s AND hour_of_day = %s;
    """

    try:
        cur.execute(query, (lot_id, day_name, hour))
        row = cur.fetchone()

        if not row:
            return {"avg_occ": 0, "avg_dur": 0}

        return {
            "avg_occ": float(row["average_occupancy_lot"]),
            "avg_dur": float(row["average_duration_minutes"])
        }

    except:
        return {"avg_occ": 0, "avg_dur": 0}

    finally:
        cur.close()
        conn.close()

#Returns: most recent ML prediction for a lot
def fetch_forecast(lot_id: str):
    conn = get_db_connection()
    if not conn:
        return None

    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = """
    SELECT 
        predicted_occupancy_count,
        predicted_num_departures,
        predicted_occupancy_pct,
        forecast_time,
        run_time
    FROM parking_forecast
    WHERE lot_id = %s
    ORDER BY forecast_time DESC
    LIMIT 1;
    """

    try:
        cur.execute(query, (lot_id,))
        row = cur.fetchone()
        return row
    except Exception as e:
        print(f"Forecast query error: {e}")
        return None
    finally:
        cur.close()
        conn.close()