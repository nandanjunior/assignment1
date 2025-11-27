import os
import sys
import time
import csv
import json
from datetime import datetime
import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
from generated import music_service_pb2, music_service_pb2_grpc

# ───────────────────────────────────────────────
# Environment Variables (works for local + Docker)
# ───────────────────────────────────────────────
MAPREDUCE_HOST = os.getenv("MAPREDUCE_HOST", "localhost")
MAPREDUCE_PORT = os.getenv("MAPREDUCE_PORT", "50051")
USERBEHAVIOR_HOST = os.getenv("USERBEHAVIOR_HOST", "localhost")
USERBEHAVIOR_PORT = os.getenv("USERBEHAVIOR_PORT", "50053")
GENRE_ANALYSIS_HOST = os.getenv("GENRE_ANALYSIS_HOST", "localhost")
GENRE_ANALYSIS_PORT = os.getenv("GENRE_ANALYSIS_PORT", "50055")
RECOMMENDATION_HOST = os.getenv("RECOMMENDATION_HOST", "localhost")
RECOMMENDATION_PORT = os.getenv("RECOMMENDATION_PORT", "50057")


DATA_CSV = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "stream_data.csv")
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
            rec = music_service_pb2.StreamRecord(
                user_id=r["user_id"],
                song_id=r["song_id"],
                artist=r["artist"],
                duration=int(r["duration"]),
                timestamp=r["timestamp"],
                genre=r.get("genre", "")
            )
            records.append(rec)
    return records


# ───────────────────────────────────────────────
# MapReduce Service
# ───────────────────────────────────────────────
def call_mapreduce(records):
    addr = f"{MAPREDUCE_HOST}:{MAPREDUCE_PORT}"
    print("=" * 70)
    print("[MapReduce Service] Global Song Play Analysis")
    print("=" * 70)
    start = time.time()
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.MapReduceServiceStub(channel)
        resp = stub.AggregateStream(music_service_pb2.StreamList(records=records))
    elapsed = time.time() - start

    print(f"Processing Time: {resp.processing_time:.4f}s (Roundtrip {elapsed:.4f}s)")
    print("-" * 70)
    print("Top Song Play Counts:")
    sorted_counts = sorted(resp.play_counts.items(), key=lambda x: x[1], reverse=True)
    for i, (song, count) in enumerate(sorted_counts[:10], 1):
        print(f"  {i}. {song}: {count} plays")
    print()
    return resp


# ───────────────────────────────────────────────
# UserBehavior Service
# ───────────────────────────────────────────────
def call_userbehavior(records):
    addr = f"{USERBEHAVIOR_HOST}:{USERBEHAVIOR_PORT}"
    print("=" * 70)
    print("[UserBehavior Service] User Listening Analytics")
    print("=" * 70)
    start = time.time()
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.UserBehaviorServiceStub(channel)
        resp = stub.AnalyzeUsers(music_service_pb2.StreamList(records=records))
    elapsed = time.time() - start

    print(f"Processing Time: {resp.processing_time:.4f}s (Roundtrip {elapsed:.4f}s)")
    print("-" * 70)
    print("User Activity Summary:")

    for stat in resp.user_stats:
        print(
            f"  • User {stat.user_id}: "
            f"Total Time {stat.total_time}s, Top Artist: {stat.top_artist}"
        )

    if resp.top_users:
        print("\nTop Active Users:")
        for u in resp.top_users:
            print(f"  - {u}")

    print()
    return resp


# ───────────────────────────────────────────────
# Genre Analysis Service
# ───────────────────────────────────────────────
def call_genre_analysis(records):
    addr = f"{GENRE_ANALYSIS_HOST}:{GENRE_ANALYSIS_PORT}"
    print("=" * 70)
    print("[Genre Analysis Service] Global Genre Play Analysis")
    print("=" * 70)
    start = time.time()
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.GenreAnalysisServiceStub(channel)
        resp = stub.AnalyzeGenres(music_service_pb2.StreamList(records=records))
    elapsed = time.time() - start

    print(f"Processing Time: {resp.processing_time:.4f}s (Roundtrip {elapsed:.4f}s)")
    print("-" * 70)
    print("Top Genres:")
    for i, genre in enumerate(resp.top_genres, 1):
        print(f"  {i}. {genre} ({resp.genre_counts[genre]} plays)")
    print()
    return resp


# ───────────────────────────────────────────────
# Recommendation Service
# ───────────────────────────────────────────────
def call_recommendation(play_counts, user_stats):
    addr = f"{RECOMMENDATION_HOST}:{RECOMMENDATION_PORT}"
    print("=" * 70)
    print("[Recommendation Service] Personalized Song Suggestions")
    print("=" * 70)
    start = time.time()
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.RecommendationServiceStub(channel)

        # Build PlayCounts message properly
        pc = music_service_pb2.PlayCounts(processing_time=play_counts.processing_time)
        for k, v in play_counts.play_counts.items():
            pc.play_counts[k] = v

        req = music_service_pb2.RecommendationRequest(play_counts=pc, user_stats=user_stats)
        resp = stub.Recommend(req)
    elapsed = time.time() - start

    print(f"Processing Time: {resp.processing_time:.4f}s (Roundtrip {elapsed:.4f}s)")
    print("-" * 70)
    print("Trending Songs:")
    for i, song in enumerate(resp.trending_songs, 1):
        print(f"  {i}. {song}")

    # If detailed recommendations are provided
    if resp.recommendations:
        print("\nUser Recommendations:")
        for user, rec in resp.recommendations.items():
            print(f"  • {user}: {', '.join(rec.values)}")

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
    print("🎵 MUSIC STREAMING ANALYTICS CLIENT")
    print("=" * 70)
    print("Workflow: Client → MapReduce → UserBehavior → Genre Analysis → Recommendation → Client")
    print("=" * 70)
    print()

    print(f"[Client] Loading data from {DATA_CSV}")
    records = load_data(DATA_CSV)
    print(f"[Client] ✓ Loaded {len(records)} streaming records")
    print()

    total_start = time.time()

    # 1️⃣ MapReduce
    mapreduce_resp = call_mapreduce(records)

    # 2️⃣ UserBehavior
    userbehavior_resp = call_userbehavior(records)

    # 3️⃣ Genre Analysis
    genre_resp = call_genre_analysis(records)

    # 4️⃣ Recommendation
    recommendation_resp = call_recommendation(mapreduce_resp, userbehavior_resp)


    total_time = time.time() - total_start

    print("=" * 70)
    print("🎯 WORKFLOW SUMMARY")
    print("=" * 70)
    print(f"MapReduce Time:      {mapreduce_resp.processing_time:.4f}s")
    print(f"UserBehavior Time:   {userbehavior_resp.processing_time:.4f}s")
    print(f"Genre Analysis Time: {genre_resp.processing_time:.4f}s")
    print(f"Recommendation Time: {recommendation_resp.processing_time:.4f}s")
    print(f"Total Workflow Time: {total_time:.4f}s")
    print("=" * 70)
    print("✓ All services completed successfully!")
    print("=" * 70)

 # ================== SAVE DETAILED METRICS ==================
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "workflow": "Client → MapReduce → UserBehavior → Genre Analysis → Recommendation → Client",
        "performance": {
            "mapreduce_time": mapreduce_resp.processing_time,
            "userbehavior_time": userbehavior_resp.processing_time,
            "genre_time": genre_resp.processing_time,
            "recommendation_time": recommendation_resp.processing_time,
            "total_workflow_time": total_time,
        },
        "mapreduce_results": {
            "top_songs": dict(sorted(mapreduce_resp.play_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
        },
        "userbehavior_results": {
            "users": [
                {
                    "user_id": s.user_id,
                    "total_time": s.total_time,
                    "top_artist": s.top_artist,
                }
                for s in userbehavior_resp.user_stats
            ],
            "top_users": list(userbehavior_resp.top_users),
        },
        "genre_analysis_results": {
            "top_genres": list(genre_resp.top_genres),
            "genre_counts": dict(genre_resp.genre_counts),
        },
        "recommendation_results": {
            "trending_songs": list(recommendation_resp.trending_songs),
            "recommendations": {
                user: list(recs.values)
                for user, recs in recommendation_resp.recommendations.items()
            },
        },
        

    }

    save_result(os.path.join(RESULTS_DIR, "grpc_performance_metrics.json"), metrics)
    print(f"[Client] Performance metrics saved to {RESULTS_DIR}\grpc_performance_metrics.json\n")


if __name__ == "__main__":
    main()
