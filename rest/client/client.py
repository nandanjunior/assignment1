# client_rest.py
import os
import sys
import time
import csv
import json
from datetime import datetime
import requests

# ───────────────────────────────────────────────
# Environment Variables
# ───────────────────────────────────────────────
MAPREDUCE_HOST = os.getenv("MAPREDUCE_HOST", "localhost")
MAPREDUCE_PORT = os.getenv("MAPREDUCE_PORT", "5001")
USERBEHAVIOR_HOST = os.getenv("USERBEHAVIOR_HOST", "localhost")
USERBEHAVIOR_PORT = os.getenv("USERBEHAVIOR_PORT", "5003")
GENRE_ANALYSIS_HOST = os.getenv("GENRE_ANALYSIS_HOST", "localhost")
GENRE_ANALYSIS_PORT = os.getenv("GENRE_ANALYSIS_PORT", "5005")
RECOMMENDATION_HOST = os.getenv("RECOMMENDATION_HOST", "localhost")
RECOMMENDATION_PORT = os.getenv("RECOMMENDATION_PORT", "5007")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_CSV = os.getenv('DATA_CSV', os.path.join(PROJECT_ROOT, 'data', 'stream_data.csv'))

RESULTS_DIR = os.getenv(
    "RESULTS_DIR",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
)
os.makedirs(RESULTS_DIR, exist_ok=True)

# ───────────────────────────────────────────────
# Utility Functions
# ───────────────────────────────────────────────
def load_data(csv_path):
    records = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            records.append({
                "user_id": r["user_id"],
                "song_id": r["song_id"],
                "artist": r["artist"],
                "duration": int(r["duration"]),
                "timestamp": r["timestamp"],
                "genre": r.get("genre", "")
            })
    return records


def post_with_retry(url, payload, retries=5, delay=1):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"[Warning] Request to {url} failed: {e}, retrying ({attempt}/{retries})...")
            time.sleep(delay)
    raise RuntimeError(f"Failed to connect to {url} after {retries} attempts")


# ───────────────────────────────────────────────
# MapReduce Service
# ───────────────────────────────────────────────
def call_mapreduce(records):
    url = f"http://{MAPREDUCE_HOST}:{MAPREDUCE_PORT}/mapreduce"
    print("=" * 70)
    print("[MapReduce REST] Global Song Play Analysis")
    print("=" * 70)

    payload = {"records": records}
    start = time.time()
    resp = post_with_retry(url, payload)
    roundtrip = time.time() - start

    print(f"Processing Time: {resp['processing_time']:.4f}s (Roundtrip {roundtrip:.4f}s)")
    print("-" * 70)
    print("Top Song Play Counts:")
    for i, (song, count) in enumerate(sorted(resp["play_counts"].items(), key=lambda x: x[1], reverse=True)[:10], 1):
        print(f"  {i}. {song}: {count} plays")
    print()
    return resp


# ───────────────────────────────────────────────
# UserBehavior Service
# ───────────────────────────────────────────────
def call_userbehavior(records):
    url = f"http://{USERBEHAVIOR_HOST}:{USERBEHAVIOR_PORT}/userbehavior"
    print("=" * 70)
    print("[UserBehavior REST] User Listening Analytics")
    print("=" * 70)

    payload = {"records": records}
    start = time.time()
    resp = post_with_retry(url, payload)
    roundtrip = time.time() - start

    print(f"Processing Time: {resp['processing_time']:.4f}s (Roundtrip {roundtrip:.4f}s)")
    print("-" * 70)
    print("User Activity Summary:")
    for stat in resp.get("user_stats", []):
        print(f"  • User {stat['user_id']}: Total {stat['total_time']}s, Top Artist = {stat['top_artist']}")
    if resp.get("top_users"):
        print("\nTop Active Users:")
        for u in resp["top_users"]:
            print(f"  - {u}")
    print()
    return resp


# ───────────────────────────────────────────────
# GenreAnalysis Service
# ───────────────────────────────────────────────
def call_genre_analysis(records):
    url = f"http://{GENRE_ANALYSIS_HOST}:{GENRE_ANALYSIS_PORT}/genre_analysis"
    print("=" * 70)
    print("[GenreAnalysis REST] Top Genre Analytics")
    print("=" * 70)

    payload = {"records": records}
    start = time.time()
    resp = post_with_retry(url, payload)
    roundtrip = time.time() - start

    print(f"Processing Time: {resp['processing_time']:.4f}s (Roundtrip {roundtrip:.4f}s)")
    print("-" * 70)
    print("Top Genres:")
    for i, g in enumerate(resp.get("top_genres", []), 1):
        print(f"  {i}. {g}")
    print()
    return resp


# ───────────────────────────────────────────────
# Recommendation Service
# ───────────────────────────────────────────────
def call_recommendation(play_counts, userbehavior_resp, genre_analysis_resp):
    url = f"http://{RECOMMENDATION_HOST}:{RECOMMENDATION_PORT}/recommend"
    print("=" * 70)
    print("[Recommendation REST] Personalized Song Suggestions")
    print("=" * 70)

    payload = {
        "play_counts": play_counts,
        "user_stats": userbehavior_resp.get("user_stats", []),
        "top_genres": genre_analysis_resp.get("top_genres", [])  # optional extra info
    }

    start = time.time()
    resp = post_with_retry(url, payload)
    roundtrip = time.time() - start

    print(f"Processing Time: {resp['processing_time']:.4f}s (Roundtrip {roundtrip:.4f}s)")
    print("-" * 70)
    print("Trending Songs:")
    for i, song in enumerate(resp.get("trending_songs", []), 1):
        print(f"  {i}. {song}")

    if resp.get("recommendations"):
        print("\nUser Recommendations:")
        for user, recs in resp["recommendations"].items():
            print(f"  • {user}: {', '.join(recs)}")
    print()
    return resp


# ───────────────────────────────────────────────
# Save Results
# ───────────────────────────────────────────────
def save_result(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ───────────────────────────────────────────────
# Main Function
# ───────────────────────────────────────────────
def main():
    print("=" * 70)
    print("🎵 MUSIC STREAMING ANALYTICS CLIENT (REST)")
    print("=" * 70)
    print("Workflow: Client → MapReduce → UserBehavior → GenreAnalysis → Recommendation → Client")
    print("=" * 70)
    print()

    print(f"[Client] Loading data from {DATA_CSV}")
    records = load_data(DATA_CSV)
    print(f"[Client] ✓ Loaded {len(records)} streaming records")
    print()

    total_start = time.time()

    mapreduce_resp = call_mapreduce(records)
    userbehavior_resp = call_userbehavior(records)
    genre_analysis_resp = call_genre_analysis(records)
    recommendation_resp = call_recommendation(mapreduce_resp["play_counts"], userbehavior_resp, genre_analysis_resp)

    total_time = time.time() - total_start

    print("=" * 70)
    print("🎯 WORKFLOW SUMMARY")
    print("=" * 70)
    print(f"MapReduce Time:      {mapreduce_resp['processing_time']:.4f}s")
    print(f"UserBehavior Time:   {userbehavior_resp['processing_time']:.4f}s")
    print(f"GenreAnalysis Time:  {genre_analysis_resp['processing_time']:.4f}s")
    print(f"Recommendation Time: {recommendation_resp['processing_time']:.4f}s")
    print(f"Total Workflow Time: {total_time:.4f}s")
    print("=" * 70)
    print("✓ All services completed successfully!")
    print("=" * 70)

    # ================== SAVE DETAILED METRICS ==================
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "workflow": "Client → MapReduce → UserBehavior → GenreAnalysis → Recommendation → Client",
        "performance": {
            "mapreduce_time": mapreduce_resp["processing_time"],
            "userbehavior_time": userbehavior_resp["processing_time"],
            "genre_analysis_time": genre_analysis_resp["processing_time"],
            "recommendation_time": recommendation_resp["processing_time"],
            "total_workflow_time": total_time,
        },
        "mapreduce_results": {
            "top_songs": dict(sorted(mapreduce_resp["play_counts"].items(), key=lambda x: x[1], reverse=True)[:10]),
        },
        "userbehavior_results": {
            "users": userbehavior_resp.get("user_stats", []),
            "top_users": userbehavior_resp.get("top_users", []),
        },
        "genre_analysis_results": {
            "top_genres": genre_analysis_resp.get("top_genres", []),
            "genre_counts": genre_analysis_resp.get("genre_counts", {}),
        },
        "recommendation_results": {
            "trending_songs": recommendation_resp.get("trending_songs", []),
            "recommendations": recommendation_resp.get("recommendations", {}),
        },
    }

    save_result(os.path.join(RESULTS_DIR, "rest_performance_metrics.json"), metrics)
    print(f"[Client] Performance metrics saved to {RESULTS_DIR}/rest_performance_metrics.json\n")


if __name__ == "__main__":
    main()
