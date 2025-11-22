# genre_analysis_service_rest.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = int(os.getenv("GENRE_ANALYSIS_PORT", 5005))


def now():
    return time.time()


@app.route("/genre_analysis", methods=["POST"])
def genre_analysis():
    start = now()
    data = request.get_json()
    if not data or "records" not in data:
        return jsonify({"error": "Missing 'records' in request"}), 400

    records = data["records"]

    # Count how many times each genre appears
    genre_counter = Counter()
    for r in records:
        genre = r.get("genre")
        if genre:
            genre_counter[genre] += 1

    # Top genres
    top_genres = [g for g, _ in genre_counter.most_common(10)]

    processing_time = now() - start

    resp = {
        "processing_time": processing_time,
        "top_genres": top_genres,
        "genre_counts": dict(genre_counter)
    }

    return jsonify(resp)


if __name__ == "__main__":
    print(f"[GenreAnalysis REST] server starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
