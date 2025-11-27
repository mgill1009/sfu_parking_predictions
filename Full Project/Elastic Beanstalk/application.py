from flask import Flask, render_template, jsonify, request
from psycopg2.extras import RealDictCursor
from database_connection import fetch_current_lot_status, fetch_recent_activities, fetch_historical_occupancy, fetch_current_hour_metrics, fetch_forecast
import os

app = Flask(__name__)
application = app #EB expects the variable called "application"

#Returns: the latest snapshot of all parking lots for a specific campus
#Powers: lot dropdown and donut chart
@app.route("/api/lots/<campus>")
def api_lots(campus):
    lots_data = fetch_current_lot_status(campus)

    if not lots_data:
        return jsonify({"error": "Failed to retrieve lot data"}), 500

    #converting occupancy_rate to percentage for the front-end
    formatted_lots = []
    for lot in lots_data:
        lot['occupancy_rate_pct'] = round(float(lot['occupancy_rate']) * 100, 2)
        formatted_lots.append(lot)
    return jsonify({"campus": campus.lower(), "lots": formatted_lots})

#Returns: full set of analytics for a single lot
#           - past occupancy trend
#           - recent events
#           - avg occupancy and stay
#           - predictions for occupancy and expected departures
#Powers: app.js
@app.route("/api/lot_details/<lot_id>")
def api_lot_details(lot_id):
    #DB calls
    historical_data = fetch_historical_occupancy(lot_id)
    activities_data = fetch_recent_activities(lot_id)
    current_hour_metrics = fetch_current_hour_metrics(lot_id)
    forecast = fetch_forecast(lot_id)

    chart_points = []
    for row in historical_data:
        chart_points.append({
            'time': f"{row['hour_of_day']:02d}:00", #HH:00
            'pct': float(row.get('average_occupancy_lot', 0))
        })
    
    return jsonify({
        "lot_id": lot_id,
        "historical_metrics": chart_points,
        "activities": activities_data,
        "current_avg_occ": current_hour_metrics["avg_occ"], 
        "current_avg_duration": current_hour_metrics["avg_dur"],
        "forecast": forecast  
    })

#logging endpoint used for testing
@app.route("/api/log", methods=["POST"])
def api_log():
    if not request.is_json:
        return jsonify({"ok": False, "error": "expected application/json"}), 400
    payload = request.get_json(silent=True) or {}
    
    return jsonify({"ok": True, "received": payload}), 201

#Routes: home page
@app.route("/")
def home():
    return render_template("index.html")
#Routes: burnaby campus
@app.route("/burnaby")
def burnaby():
    return render_template(
        "campus.html",
        campus_key="burnaby",
        campus_name="Burnaby Campus",
        accent="#A6192E")
#Routes: surrey campus
@app.route("/surrey")
def surrey():
    return render_template(
        "campus.html",
        campus_key="surrey",
        campus_name="Surrey Campus",
        accent="#54585A")
#Routes: log event (demo only)
@app.route("/log_event")
def log_event():
    return render_template("log_event.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080)) 
    app.run(host="0.0.0.0", port=port, debug=True)