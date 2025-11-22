"""
XML-RPC GenreAnalysis Service (Music)
Analyzes top genres from streaming records and forwards to Recommendation Service.
"""
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# allow importing from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Config
HOST = os.getenv('GENRE_ANALYSIS_HOST', '0.0.0.0')
PORT = int(os.getenv('GENRE_ANALYSIS_PORT', '8005'))
NEXT_URL = os.getenv('RECOMMENDATION_URL', 'http://localhost:8007')

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

# ────────────── Genre Analysis Logic ──────────────
def map_genre(record):
    return (record.get('genre'), 1)

def reduce_counts(mapped):
    result = defaultdict(int)
    for genre, count in mapped:
        if genre is not None:
            result[genre] += count
    return dict(result)

def perform_genre_analysis(records, max_workers=4):
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        mapped = list(executor.map(map_genre, records))
    genre_counts = reduce_counts(mapped)
    top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)
    top_genres_list = [g for g, _ in top_genres]
    processing_time = time.time() - start_time
    return {
        "genre_counts": genre_counts,
        "top_genres": top_genres_list,
        "processing_time": processing_time
    }

# ────────────── XML-RPC Handler ──────────────
class GenreAnalysisXMLHandler:
    def __init__(self, next_service_url):
        self.next_service_url = next_service_url
        print(f"[GenreAnalysis Service] Initialized. Next service: {next_service_url}")

    def process(self, records_data, accumulated_results):
        start = time.time()
        print(f"[GenreAnalysis] Received {len(records_data)} records")

        result = perform_genre_analysis(records_data)
        accumulated_results['genre_analysis'] = result

        print(f"[GenreAnalysis] Top genres: {result['top_genres']}")
        print(f"[GenreAnalysis] Processing time: {result['processing_time']:.4f}s")

        # Forward to Recommendation service
        next_svc = ServerProxy(self.next_service_url, allow_none=True)
        final = next_svc.process(records_data, accumulated_results)
        return final

# ────────────── Main ──────────────
def main():
    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True, logRequests=False)
    server.register_introspection_functions()
    handler = GenreAnalysisXMLHandler(NEXT_URL)
    server.register_instance(handler)

    print("=" * 70)
    print(f"GenreAnalysis XML-RPC Service started at {HOST}:{PORT}")
    print(f"Next service: {NEXT_URL}")
    print("=" * 70)
    server.serve_forever()

if __name__ == "__main__":
    main()
