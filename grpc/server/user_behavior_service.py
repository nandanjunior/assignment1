import os,sys
import time
import json
from collections import defaultdict, Counter
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

PORT = int(os.getenv("USERBEHAVIOR_PORT", "50053"))

class UserBehaviorHandler(music_service_pb2_grpc.UserBehaviorServiceServicer):
    def AnalyzeUsers(self, request, context):
        start = now()
        records = request.records
        user_time = defaultdict(int)
        user_artists = defaultdict(list)

        for r in records:
            user_time[r.user_id] += r.duration
            user_artists[r.user_id].append(r.artist)

        user_stats_list = music_service_pb2.UserStatsList(processing_time=0.0)
        for uid, t in user_time.items():
            top_artist = Counter(user_artists[uid]).most_common(1)[0][0] if user_artists[uid] else ""
            us = music_service_pb2.UserStat(
                user_id=uid,
                total_time=t,
                top_artist=top_artist
            )
            user_stats_list.user_stats.append(us)
        # top users (ids) sorted by total_time desc, limit 5
        top_users_sorted = sorted(user_time.items(), key=lambda x: x[1], reverse=True)[:5]
        for uid, _ in top_users_sorted:
            user_stats_list.top_users.append(uid)

        processing_time = now() - start
        user_stats_list.processing_time = processing_time

        try:
            save_metrics("/tmp/results_userbehavior.json", {"processing_time": processing_time, "num_users": len(user_time)})
        except Exception:
            pass
        return user_stats_list

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    music_service_pb2_grpc.add_UserBehaviorServiceServicer_to_server(UserBehaviorHandler(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    print(f"[UserBehavior] gRPC server started on port {PORT}")
    serve()
