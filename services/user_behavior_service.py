"""
User Behavior Analysis Service
Finds total time listened and favorite artist per user
"""
import time
from collections import defaultdict, Counter

class UserBehaviorService:
    @staticmethod
    def analyze_behavior(stream_data):
        start = time.time()
        user_time = defaultdict(int)
        user_artist = defaultdict(list)

        for record in stream_data:
            user_time[record.user_id] += record.duration
            user_artist[record.user_id].append(record.artist)

        user_stats = []
        for uid in user_time:
            fav_artist = Counter(user_artist[uid]).most_common(1)[0][0]
            user_stats.append({
                "user_id": uid,
                "total_time": user_time[uid],
                "top_artist": fav_artist
            })

        # Top 5 active users
        top_users = sorted(user_stats, key=lambda x: x["total_time"], reverse=True)[:5]
        processing_time = time.time() - start
        return {"user_stats": user_stats, "top_users": top_users, "processing_time": processing_time}
