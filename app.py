import redis
import json
import time
from smartWebSocketV2 import SmartWebSocketV2
from logzero import logger
from flask import Flask, jsonify, request
import requests
import threading
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import pandas as pd


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*")
# Redis configuration
redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, decode_responses=True)

AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeExDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaVlqWmhPVGRqWVdNdE4yUmlPUzB6WVdVeExUZzNaV1F0Wm1KbU9ERmtNV1JpTVRBNElpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXhJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVEVzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOWZTd2lhWE56SWpvaWRISmhaR1ZmYkc5bmFXNWZjMlZ5ZG1salpTSXNJbk4xWWlJNklrRTFNemMwTVRJaUxDSmxlSEFpT2pFM016UXhOVEUwTkRFc0ltNWlaaUk2TVRjek5EQTJORGcyTVN3aWFXRjBJam94TnpNME1EWTBPRFl4TENKcWRHa2lPaUkwWWpNeU5XSXdZaTB6WW1FMExUUmpNVE10T1RGaE5TMW1OV1l4TkRaaU5qWTVObU1pTENKVWIydGxiaUk2SWlKOS5EMW9CZ3Ntd2ZKc1h0cDNPUjlhWC1sNjNxelRURUFILWszUVdrckN1SGlJQUhWMDhOZzN4TmdyMnhld01wcjVTQWthLU9GZlVXUmlEMllodmZSQ2xqU3RFYmtyVlp0MjhFbzFnZm04QVdsLXFzT0ZLSi1Bb0EzWEJ3NHAzeEFXS1NKU1I5czA3S1VHeEhNZy1uQ1p0TFJ5RWNDOWVZWE5kOFFBNFJCYVBuRzAiLCJBUEktS0VZIjoiOUg3WHZQQ2siLCJpYXQiOjE3MzQwNjUwNDEsImV4cCI6MTczNDE1MTQ0MX0.5-XnOeeoDjVGoBEjDWyvZ1oCZWrrvahqy6Y1wYHTfygqRDdQRhx2YwEceDfT6bwD2erHp0vgNzgwGAXxFyJW_w"
API_KEY = "9H7XvPCk"
CLIENT_CODE = "A537412"
FEED_TOKEN="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJpYXQiOjE3MzQwNjUwNDEsImV4cCI6MTczNDE1MTQ0MX0.7JtOB1YNsEzXIeuRi05jffS5qSmyeJmC4cWAi_NLlbNuLHY0BMWHF0Td6W5JdDC-k6SCTS1r9FzgLOayEZ71WQ"

correlation_id = "abc_123"
action = 1
mode = 3

# Token list for subscription
token_list = [
    {
        "exchangeType": 1,
        "tokens": ["22"]
    }
]

# Initialize SmartWebSocketV2 instance
sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)

# Retry configuration
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 5
retry_count = 0



# Callback to handle incoming data
def on_data(wsapp, message):
    logger.info("Received Ticks: {}".format(message))
    try:
        if isinstance(message, dict):
            message_json = json.dumps(message)  # Convert dictionary to JSON
        else:
            message_json = message

        # Store live data in Redis
        redis_client.lpush("live_data_list", message_json)  # Push new data to the list
        redis_client.ltrim("live_data_list", 0, 99)  # Keep only the last 100 entries
        redis_client.set("live_data", message_json)  # Store the latest live data

        # Emit data to WebSocket clients
        socketio.emit("live_data_updated", json.loads(message_json))
        logger.info("Data stored in Redis and emitted to WebSocket clients successfully.")
    except redis.RedisError as e:
        logger.error("Failed to store data in Redis: {}".format(e))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# Callback for WebSocket connection open
def on_open(wsapp):
    logger.info("WebSocket connection opened.")
    try:
        sws.subscribe(correlation_id, mode, token_list)
    except Exception as e:
        logger.error(f"Subscription error: {e}")


# Callback for WebSocket error
def on_error(wsapp, error):
    global retry_count
    logger.error(f"WebSocket error: {error}")
    if retry_count < MAX_RETRY_ATTEMPTS:
        retry_count += 1
        logger.info(f"Retrying connection (attempt {retry_count}/{MAX_RETRY_ATTEMPTS})...")
        time.sleep(RETRY_DELAY)
        try:
            sws.connect()
        except Exception as e:
            logger.error(f"Reconnect attempt failed: {e}")
    else:
        logger.error("Max retry attempts reached. Unable to reconnect.")


# Callback for WebSocket close
def on_close(wsapp):
    global retry_count
    logger.warning("WebSocket connection closed.")
    if retry_count < MAX_RETRY_ATTEMPTS:
        retry_count += 1
        logger.info(f"Attempting to reconnect (attempt {retry_count}/{MAX_RETRY_ATTEMPTS})...")
        time.sleep(RETRY_DELAY)
        try:
            sws.connect()
        except Exception as e:
            logger.error(f"Reconnect attempt failed: {e}")
    else:
        logger.error("Max retry attempts reached. Unable to reconnect.")

@socketio.on("get_live_data")
def handle_update_token_list(data):
    """
    Handles updates to the token list from the frontend.
    """
    global token_list, correlation_id, mode

    try:
        # Validate input data
        if "exchangeType" not in data or "tokens" not in data:
            emit("error", {"message": "Invalid data format. Must include 'exchangeType' and 'tokens'."})
            return

        # Update the token list dynamically
        new_token_entry = {
            "exchangeType": data["exchangeType"],
            "tokens": data["tokens"]
        }
        token_list = [new_token_entry]  # Replace the existing token_list
        logger.info(f"Updated token_list: {token_list}")

        # Resubscribe with the updated token list
        try:
            sws.subscribe(correlation_id, mode, token_list)
            emit("live_data_updated", {"success": True, "message": "Token list updated and subscribed successfully."})
            logger.info("Successfully resubscribed to the updated token list.")
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            emit("live_data_updated", {"success": False, "message": "Failed to resubscribe with the new token list."})

    except Exception as e:
        logger.error(f"Error updating token list: {e}")
        emit("error", {"message": "An error occurred while updating the token list."})

# # Flask endpoint to fetch live and historical data
# @socketio.on("get_live_data")
# def handle_get_live_data(data):
#     """
#     Handles client subscription to specific exchangeType and token data.
#     """
#     global token_list
#     try:
#         # Validate input data
#         if "exchangeType" not in data or "token" not in data:
#             emit("error", {"message": "Invalid data format. Must include 'exchangeType' and 'token'."})
#             return

#         # Update the token list dynamically
#         new_token_entry = {
#             "exchangeType": data["exchangeType"],
#             "tokens": data["tokens"]
#         }
#         token_list = [new_token_entry]  # Replace the existing token_list
#         logger.info(f"Updated token_list: {token_list}")

        

#         # Fetch historical data and filter it based on exchangeType and token
#         historical_data = redis_client.lrange("live_data_list", 0, -1)
#         filtered_data = [
#             json.loads(item) for item in historical_data
#             if json.loads(item).get("exchangeType") == exchange_type and json.loads(item).get("token") == token
#         ]

#         # Fetch the latest live data
#         live_data = redis_client.get("live_data")
#         if live_data:
#             live_data = json.loads(live_data)
#             # Check if it matches the exchangeType and token
#             if live_data.get("exchangeType") == exchange_type and live_data.get("token") == token:
#                 emit("live_data_filtered", {"live_data": live_data, "historical_data": filtered_data})
#             else:
#                 emit("live_data_filtered", {"live_data": None, "historical_data": filtered_data})
#         else:
#             emit("live_data_filtered", {"live_data": None, "historical_data": filtered_data})
#     except Exception as e:
#         logger.error(f"Error handling subscription: {e}")
#         emit("error", {"message": "An error occurred while processing the subscription."})

# Load JSON data from the URL

URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = requests.get(URL)

if response.status_code == 200:
    json_data = response.json()
    df = pd.DataFrame(json_data)  # Convert JSON to DataFrame
else:
    df = pd.DataFrame()  # Empty DataFrame if fetch fails
    print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")

@app.route('/search', methods=['GET'])
def search():
    name = request.args.get('name')  # Get the 'name' parameter from the query string

    if not name:
        return jsonify({"error": "Missing 'name' parameter"}), 400

    try:
        # Filter the DataFrame
        filtered_result = df[df["name"].str.contains(name, case=False, na=False)].copy()  # Add .copy() here to avoid the warning

        if filtered_result.empty:
            return jsonify({"message": f"No results found for name: {name}"}), 404

        # Add a custom sort to prioritize rows with 'exch_seg' == 'NSE' and last character of 'symbol' == 'EQ'
        filtered_result["priority"] = filtered_result.apply(
            lambda row: (
                0 if row["exch_seg"] == "NSE" else 1,  # Priority 0 for NSE, 1 otherwise
                0 if row["symbol"].endswith("EQ") else 1  # Priority 0 for symbols ending with 'EQ', 1 otherwise
            ),
            axis=1
        )
        sorted_result = filtered_result.sort_values(by="priority").drop(columns=["priority"])

        # Limit to the first 30 rows
        limited_result = sorted_result.head(30)

        # Return JSON response
        return limited_result.to_json(orient="records"), 200

    except Exception as e:
        # Log the error for debugging
        logger.error(f"Error in search endpoint: {e}")
        return jsonify({"error": "Internal server error. Please try again later."}), 500




@app.route('/api/live-data', methods=['GET'])
def get_live_data():
    try:
        # Retrieve the current live data
        live_data = redis_client.get("live_data")
        
        # Retrieve historical data (up to the last 100 entries)
        historical_data = redis_client.lrange("live_data_list", 0, -1)

        # Format the response
        response = {
            "success": True,
            "live_data": json.loads(live_data) if live_data else None,
            "historical_data": [json.loads(data) for data in historical_data]
        }

        # Emit the live data to WebSocket clients
        socketio.emit("live_data_update", response)
        return jsonify(response)
    except redis.RedisError as e:
        logger.error(f"Error fetching data from Redis: {e}")
        return jsonify({"success": False, "message": "Error fetching data from Redis."}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"success": False, "message": "Unexpected server error."}), 500

# Flask-SocketIO event for clients to join
@socketio.on("connect")
def handle_connect():
    logger.info("Client connected to WebSocket.")
    emit("connection_status", {"status": "connected"})

@socketio.on("disconnect")
def handle_disconnect():
    logger.info("Client disconnected from WebSocket.")




# Assign the callbacks
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close



# Start the watchdog in a separate thread
watchdog_thread = threading.Thread(target=sws.connect, daemon=True)
watchdog_thread.start()

# Start the WebSocket thread and Flask app
if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)