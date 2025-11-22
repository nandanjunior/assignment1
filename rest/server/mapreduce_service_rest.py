# mapreduce_service_rest.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify


app = Flask(__name__)
PORT = int(os.getenv("MAPREDUCE_PORT", 5001))


def now():
    return time.time()


@app.route("/mapreduce", methods=["POST"])
def aggregate():
    start = now()
    data = request.get_json()
    if not data or "records" not in data:
        return jsonify({"error": "Missing 'records' in request"}), 400

    records = data["records"]

    # Map phase (parallel) -> key = "artist - song_id"
    def map_fn(rec):
        key = f"{rec['artist']} - {rec['song_id']}"
        return (key, 1)

    with ThreadPoolExecutor(max_workers=4) as ex:
        mapped = list(ex.map(map_fn, records))

    # Reduce
    result = defaultdict(int)
    for k, v in mapped:
        result[k] += v

    processing_time = now() - start

    resp = {
        "processing_time": processing_time,
        "play_counts": dict(result)
    }

    return jsonify(resp)


if __name__ == "__main__":
    print(f"[MapReduce REST] gRPC-like MapReduce REST server starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
