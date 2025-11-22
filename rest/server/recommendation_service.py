# recommendation_service_rest.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import json
from collections import Counter
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = int(os.getenv("RECOMMENDATION_PORT", 5007))


def now():
    return time.time()


@app.route("/recommend", methods=["POST"])
def recommend():
    start = now()
    data = request.get_json()
    # Expecting: {"play_counts": {...}, "user_stats": [...]}
    play_counts = data.get("play_counts", {}) if data else {}
    user_stats = data.get("user_stats", []) if data else []

    # top 5 trending songs
    counter = Counter(play_counts)
    top5 = [k for k, _ in counter.most_common(5)]

    # recommendations: for each user, pick top5 excluding their top_artist
    recommendations = {}
    for us in user_stats:
        uid = us.get("user_id")
        fav = us.get("top_artist", "")
        # recommend songs that do not include fav artist string
        recs = [s for s in top5 if fav == "" or fav not in s]
        # keep same ordering as top5
        recommendations[uid] = recs

    processing_time = now() - start

    resp = {
        "processing_time": processing_time,
        "trending_songs": top5,
        "recommendations": recommendations
    }

    return jsonify(resp)


if __name__ == "__main__":
    print(f"[Recommendation REST] server starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
