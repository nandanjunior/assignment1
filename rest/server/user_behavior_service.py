# userbehavior_service_rest.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import json
from collections import defaultdict, Counter
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = int(os.getenv("USERBEHAVIOR_PORT", 5003))


def now():
    return time.time()


@app.route("/userbehavior", methods=["POST"])
def analyze():
    start = now()
    data = request.get_json()
    if not data or "records" not in data:
        return jsonify({"error": "Missing 'records' in request"}), 400

    records = data["records"]

    user_time = defaultdict(int)
    user_artists = defaultdict(list)

    for r in records:
        # ensure types are correct
        uid = r.get("user_id")
        duration = int(r.get("duration", 0))
        artist = r.get("artist", "")
        user_time[uid] += duration
        user_artists[uid].append(artist)

    # Build user_stats list
    user_stats = []
    for uid, t in user_time.items():
        top_artist = Counter(user_artists[uid]).most_common(1)[0][0] if user_artists[uid] else ""
        user_stats.append({
            "user_id": uid,
            "total_time": t,
            "top_artist": top_artist
        })

    # Top 5 users by total_time
    top_users_sorted = sorted(user_time.items(), key=lambda x: x[1], reverse=True)[:5]
    top_users = [uid for uid, _ in top_users_sorted]

    processing_time = now() - start

    resp = {
        "processing_time": processing_time,
        "user_stats": user_stats,
        "top_users": top_users
    }

    return jsonify(resp)


if __name__ == "__main__":
    print(f"[UserBehavior REST] server starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
