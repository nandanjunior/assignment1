import os,sys
import time
import json
from collections import Counter
from concurrent import futures

current_dir = os.path.dirname(os.path.abspath(__file__))
grpc_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(grpc_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(current_dir, 'generated'))

import grpc
from generated import music_service_pb2, music_service_pb2_grpc

def now():
    return time.time()

def save_metrics(path, metrics):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)

PORT = int(os.getenv("RECOMMENDATION_PORT", "50055"))

class RecommendationHandler(music_service_pb2_grpc.RecommendationServiceServicer):
    def Recommend(self, request, context):
        start = now()
        play_counts = request.play_counts.play_counts  # map<string, int>
        # play_counts is a map<string, int>
        # Convert to Counter
        counter = Counter(play_counts)
        top5 = [k for k, _ in counter.most_common(5)]
        # Build recommendations: for each user in user_stats, recommend top songs not associated with their top_artist
        recommendations_map = {}
        for us in request.user_stats.user_stats:
            fav = us.top_artist
            recs = [s for s in top5 if fav and fav not in s]
            recommendations_map[us.user_id] = recs

        # Build response
        resp = music_service_pb2.RecommendationResponse(processing_time=0.0)
        for s in top5:
            resp.trending_songs.append(s)
        # map<string, repeated_string>
        for uid, recs in recommendations_map.items():
            rs = music_service_pb2.repeated_string()
            rs.values.extend(recs)
            resp.recommendations[uid].CopyFrom(rs)

        processing_time = now() - start
        resp.processing_time = processing_time

        try:
            save_metrics("/tmp/results_recommendation.json", {"processing_time": processing_time, "num_trending": len(top5)})
        except Exception:
            pass
        return resp

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    music_service_pb2_grpc.add_RecommendationServiceServicer_to_server(RecommendationHandler(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    print(f"[Recommendation] gRPC server started on port {PORT}")
    serve()
