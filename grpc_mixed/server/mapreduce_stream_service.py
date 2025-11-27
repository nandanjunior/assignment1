import os,sys
import time
import json
from concurrent import futures
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

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

PORT = int(os.getenv("MAPREDUCE_PORT", "50051"))

# MapReduce logic (in-memory)
class MapReduceHandler(music_service_pb2_grpc.MapReduceServiceServicer):

    def AggregateStream(self, request, context):
        start = now()
        records = request.records
        # Map phase in parallel
        def map_fn(rec):
            key = f"{rec.artist} - {rec.song_id}"
            return (key, 1)
        with ThreadPoolExecutor(max_workers=4) as ex:
            mapped = list(ex.map(map_fn, records))
        # Reduce
        result = defaultdict(int)
        for k, v in mapped:
            result[k] += v
        processing_time = now() - start
        # Build PlayCounts message
        play_counts = music_service_pb2.PlayCounts(processing_time=processing_time)
        for k, v in result.items():
            play_counts.play_counts[k] = v
        # Optionally save metrics
        try:
            save_metrics("/tmp/results_mapreduce.json", {"processing_time": processing_time, "count_keys": len(result)})
        except Exception:
            pass
        return play_counts

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    music_service_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceHandler(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    print(f"[MapReduce] gRPC server started on port {PORT}")
    serve()
