"""
Music Streaming Microservices Client
Workflow: Client → MapReduce → UserBehavior → Recommendation → Client
Performs end-to-end streaming data analysis and recommendation
"""

import os
import sys
import time
import csv
import json
from datetime import datetime
import grpc

# Load generated gRPC stubs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))
from generated import music_service_pb2, music_service_pb2_grpc

# Configuration via environment variables (compatible with local + Docker)
MAPREDUCE_HOST = os.getenv("MAPREDUCE_HOST", "localhost")
MAPREDUCE_PORT = os.getenv("MAPREDUCE_PORT", "50051")
USERBEHAVIOR_HOST = os.getenv("USERBEHAVIOR_HOST", "localhost")
USERBEHAVIOR_PORT = os.getenv("USERBEHAVIOR_PORT", "50053")
RECOMMENDATION_HOST = os.getenv("RECOMMENDATION_HOST", "localhost")
RECOMMENDATION_PORT = os.getenv("RECOMMENDATION_PORT", "50055")

# File paths
DATA_CSV = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "stream_data.csv")
RESULTS_DIR = os.getenv("RESULTS_DIR", "./results")
os.makedirs(RESULTS_DIR, exist_ok=True)


# --------------------------------------------------------------------
# Utility functions
# --------------------------------------------------------------------

def load_data(csv_path):
    """Load music streaming data from CSV"""
    print(f"[Client] Loading data from {csv_path}")
    records = []
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = music_service_pb2.StreamRecord(
                    user_id=row["user_id"],
                    song_id=row["song_id"],
                    artist=row["artist"],
                    duration=int(row["duration"]),
                    timestamp=row["timestamp"]
                )
                records.append(record)
        print(f"[Client] ✓ Loaded {len(records)} streaming records\n")
        return records
    except Exception as e:
        print(f"[Client] ✗ Error loading data: {e}")
        return []


def save_result(file_path, data):
    """Save JSON results"""
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"[Client] ✓ Results saved to {file_path}")


# --------------------------------------------------------------------
# gRPC Service Calls
# --------------------------------------------------------------------

def call_mapreduce(records):
    """Call MapReduce microservice for global play counts"""
    addr = f"{MAPREDUCE_HOST}:{MAPREDUCE_PORT}"
    print(f"[Client] → Connecting to MapReduce at {addr}")
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.MapReduceServiceStub(channel)
        start = time.time()
        resp = stub.AggregateStream(music_service_pb2.StreamList(records=records))
        elapsed = time.time() - start
    print(f"[MapReduce] Completed in {resp.processing_time:.4f}s (Client roundtrip {elapsed:.4f}s)")
    print(f"[MapReduce] Found {len(resp.play_counts)} unique songs\n")
    return resp


def call_userbehavior(records):
    """Call UserBehavior microservice for user analytics"""
    addr = f"{USERBEHAVIOR_HOST}:{USERBEHAVIOR_PORT}"
    print(f"[Client] → Connecting to UserBehavior at {addr}")
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.UserBehaviorServiceStub(channel)
        start = time.time()
        resp = stub.AnalyzeUsers(music_service_pb2.StreamList(records=records))
        elapsed = time.time() - start
    print(f"[UserBehavior] Completed in {resp.processing_time:.4f}s (Client roundtrip {elapsed:.4f}s)")
    print(f"[UserBehavior] Processed {len(resp.user_stats)} unique users\n")
    return resp


def call_recommendation(play_counts, user_stats):
    """Call Recommendation microservice for trending songs"""
    addr = f"{RECOMMENDATION_HOST}:{RECOMMENDATION_PORT}"
    print(f"[Client] → Connecting to Recommendation at {addr}")
    with grpc.insecure_channel(addr) as channel:
        stub = music_service_pb2_grpc.RecommendationServiceStub(channel)
        # Build request
        pc = music_service_pb2.PlayCounts(processing_time=play_counts.processing_time)
        for k, v in play_counts.play_counts.items():
            pc.play_counts[k] = v
        request = music_service_pb2.RecommendationRequest(play_counts=pc, user_stats=user_stats)
        start = time.time()
        resp = stub.Recommend(request)
        elapsed = time.time() - start
    print(f"[Recommendation] Completed in {resp.processing_time:.4f}s (Client roundtrip {elapsed:.4f}s)")
    print(f"[Recommendation] Trending songs: {list(resp.trending_songs)}\n")
    return resp


# --------------------------------------------------------------------
# Main client workflow
# --------------------------------------------------------------------

def main():
    print("=" * 70)
    print("🎵 MUSIC STREAMING ANALYTICS CLIENT")
    print("=" * 70)
    print("Workflow: Client → MapReduce → UserBehavior → Recommendation → Client")
    print("=" * 70)

    # Load streaming data
    records = load_data(DATA_CSV)
    if not records:
        print("[Client] ✗ No records found. Exiting.")
        return

    # Run the workflow
    total_start = time.time()

    # 1️⃣ MapReduce
    play_counts = call_mapreduce(records)

    # 2️⃣ User Behavior Analysis
    user_stats = call_userbehavior(records)

    # 3️⃣ Recommendation
    recommendations = call_recommendation(play_counts, user_stats)

    total_end = time.time()
    total_time = total_end - total_start

    # ----------------------------------------------------------------
    # Performance Summary
    # ----------------------------------------------------------------
    print("=" * 70)
    print("📊 PERFORMANCE SUMMARY")
    print("=" * 70)
    print(f"MapReduce Time:       {play_counts.processing_time:.4f}s")
    print(f"UserBehavior Time:    {user_stats.processing_time:.4f}s")
    print(f"Recommendation Time:  {recommendations.processing_time:.4f}s")
    print(f"Client Total Time:    {total_time:.4f}s")
    print("=" * 70)

    # ----------------------------------------------------------------
    # Save Results
    # ----------------------------------------------------------------
    result_data = {
        "timestamp": datetime.now().isoformat(),
        "architecture": "MusicStreaming Microservices",
        "workflow": "Client → MapReduce → UserBehavior → Recommendation → Client",
        "records_processed": len(records),
        "results": {
            "mapreduce_time": play_counts.processing_time,
            "userbehavior_time": user_stats.processing_time,
            "recommendation_time": recommendations.processing_time,
            "client_total_time": total_time
        },
        "recommendations": list(recommendations.trending_songs)
    }

    output_path = os.path.join(RESULTS_DIR, "music_run_metrics.json")
    save_result(output_path, result_data)

    print("\n✓ Workflow completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
