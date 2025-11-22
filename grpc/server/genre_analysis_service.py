import os
import sys
import time
from concurrent import futures
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import grpc

# ───────────────────────────────────────────────
# Python path setup (important!)
# ───────────────────────────────────────────────
current_dir = os.path.dirname(os.path.abspath(__file__))   # grpc/server
grpc_dir = os.path.dirname(current_dir)                     # grpc
project_root = os.path.dirname(grpc_dir)                    # assignment1

sys.path.insert(0, project_root)                            # <-- allows import from services/
sys.path.insert(0, os.path.join(current_dir, 'generated'))  # <-- gRPC generated code

# ───────────────────────────────────────────────
# Imports
# ───────────────────────────────────────────────
from generated import music_service_pb2, music_service_pb2_grpc
from services.genre_analysis_service import GenreAnalysisStreamService

# ───────────────────────────────────────────────
# Server port
# ───────────────────────────────────────────────
PORT = int(os.getenv("GENRE_ANALYSIS_PORT", "50055"))

# ───────────────────────────────────────────────
# Utility
# ───────────────────────────────────────────────
def now():
    return time.time()

# ───────────────────────────────────────────────
# gRPC Handler
# ───────────────────────────────────────────────
class GenreAnalysisHandler(music_service_pb2_grpc.GenreAnalysisServiceServicer):

    def AnalyzeGenres(self, request, context):
        start = now()
        records = request.records

        # Map phase in parallel
        def map_fn(rec):
            return (rec.genre, 1)

        with ThreadPoolExecutor(max_workers=4) as ex:
            mapped = list(ex.map(map_fn, records))

        # Reduce
        result = defaultdict(int)
        for genre, count in mapped:
            result[genre] += count

        # Top genres sorted
        top_genres = sorted(result.items(), key=lambda x: x[1], reverse=True)
        top_genres_list = [g for g, _ in top_genres]

        processing_time = now() - start

        # Build gRPC response
        resp = music_service_pb2.GenreAnalysisResponse(
            processing_time=processing_time,
            top_genres=top_genres_list
        )
        for g, c in result.items():
            resp.genre_counts[g] = c

        return resp

# ───────────────────────────────────────────────
# Serve
# ───────────────────────────────────────────────
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    music_service_pb2_grpc.add_GenreAnalysisServiceServicer_to_server(GenreAnalysisHandler(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print(f"[GenreAnalysis] gRPC server started on port {PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
