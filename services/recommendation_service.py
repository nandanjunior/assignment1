"""
Recommendation Service
Generates trending songs and simple recommendations
"""
import time
from collections import Counter

class RecommendationService:
    @staticmethod
    def recommend(play_counts, user_stats):
        start = time.time()
        # Get top 5 songs by total plays
        trending = [k for k, _ in Counter(play_counts).most_common(5)]

        # Recommend trending songs to users who don't have them as favorites
        recommendations = {}
        for user in user_stats:
            fav = user["top_artist"]
            recommendations[user["user_id"]] = [
                s for s in trending if fav not in s
            ]

        processing_time = time.time() - start
        return {
            "trending_songs": trending,
            "recommendations": recommendations,
            "processing_time": processing_time
        }
