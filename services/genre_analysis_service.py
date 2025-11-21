import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

class GenreAnalysisStreamService:
    @staticmethod
    def map_genre(record):
        # returns (genre, 1) for counting
        return (record.genre, 1)

    @staticmethod
    def reduce_counts(mapped):
        result = defaultdict(int)
        for genre, count in mapped:
            result[genre] += count
        return dict(result)

    @staticmethod
    def perform_genre_analysis(stream_data):
        start = time.time()
        # Map in parallel
        with ThreadPoolExecutor(max_workers=4) as ex:
            mapped = list(ex.map(GenreAnalysisStreamService.map_genre, stream_data))
        # Reduce
        reduced = GenreAnalysisStreamService.reduce_counts(mapped)
        processing_time = time.time() - start

        # Determine top genres (sorted by count descending)
        top_genres = sorted(reduced.items(), key=lambda x: x[1], reverse=True)
        top_genres = [g for g, _ in top_genres]

        return {
            "genre_counts": reduced,
            "top_genres": top_genres,
            "processing_time": processing_time
        }
