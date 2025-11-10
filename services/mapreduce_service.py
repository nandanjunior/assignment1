"""
MapReduce Service for Music Streaming
Aggregates play counts per song and artist
"""
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

class MapReduceStreamService:
    @staticmethod
    def map_stream(record):
        key = f"{record.artist} - {record.song_id}"
        return (key, 1)

    @staticmethod
    def reduce_counts(mapped):
        result = defaultdict(int)
        for key, val in mapped:
            result[key] += val
        return dict(result)

    @staticmethod
    def perform_mapreduce(stream_data):
        start = time.time()
        with ThreadPoolExecutor(max_workers=4) as ex:
            mapped = list(ex.map(MapReduceStreamService.map_stream, stream_data))
        reduced = MapReduceStreamService.reduce_counts(mapped)
        processing_time = time.time() - start
        return {"play_counts": reduced, "processing_time": processing_time}
