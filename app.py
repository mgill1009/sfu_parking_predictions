from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

#LOTS[campus_key][lot_name] = { total: int, occupied: int }
LOTS = {
    "burnaby": {
                "West Parkade": {"total": 300, "occupied": 204},
                "South Parkade": {"total": 150, "occupied": 150},
                "East Parking": {"total": 320, "occupied": 5},
                "North Parking": {"total": 100, "occupied": 56},
                "South Parking": {"total": 67, "occupied": 20},
                "Residence West Lot": {"total": 298, "occupied": 230}
            },
    "surrey": {
                "Surrey South": {"total": 234, "occupied": 204},
                "Surrey City Centre Main Lot": {"total": 150, "occupied": 30}
            }
}

# ---------- Page routes ----------

# WELCOME Page
@app.route("/") 
def home():
    return render_template("index.html")

# BURNABY Page
@app.route("/burnaby")
def burnaby():
    return render_template(
        "campus.html",
        campus_key="burnaby",
        campus_name="Burnaby Campus",
        accent="#A6192E")


# SURREY Page
@app.route("/surrey")
def surrey():
    return render_template(
        "campus.html",
        campus_key="surrey",
        campus_name="Surrey Campus",
        accent="#54585A")


# LOG Page
@app.route("/log_event")
def log_event():
    return render_template("log_event.html")

# creating a dynamic route (/api/lots/burnaby -> campus = "burnaby")
@app.route("/api/lots/<campus>")
def api_lots(campus):
    campus = campus.lower() 
    data = LOTS.get(campus) # look up the campus
    
    if not data: # campus doesnt exist?
        return jsonify({"error": "unknown campus"}), 404
    
    # normalize to array for stable order on FE
    lots = [{"name": k, **v} for k, v in sorted(data.items())]
    
    return jsonify({"campus": campus, "lots": lots}) # JSON HTTP response

# receiving POST request from the front-end form when someone logs a parking event (entry or exit)
@app.route("/api/log", methods=["POST"]) 
def api_log():
    if not request.is_json:
        return jsonify({"ok": False, "error": "expected application/json"}), 400
    payload = request.get_json(silent=True) or {}
    
    # Extracting campus, lot, and type from JSON
    campus = (payload.get("campus") or "").lower() 
    lot    = payload.get("lot")
    typ    = payload.get("type")  # "entry" | "exit"

    if campus not in LOTS or lot not in LOTS[campus] or typ not in {"entry", "exit"}:
        return jsonify({"ok": False, "error": "invalid campus/lot/type"}), 400

    return jsonify({"ok": True, "received": payload}), 201

if __name__ == "__main__":
    app.run(debug=True)