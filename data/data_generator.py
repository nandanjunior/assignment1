import csv
import random
from datetime import datetime, timedelta

# Output CSV file
csv_file = "stream_data.csv"

# Sample data
users = ["U001", "U002", "U003", "U004", "U005"]
songs = [
    {"song_id": "S001", "artist": "Coldplay", "genre": "Pop", "duration": 210},
    {"song_id": "S002", "artist": "Imagine Dragons", "genre": "Rock", "duration": 200},
    {"song_id": "S003", "artist": "Ed Sheeran", "genre": "Pop", "duration": 190},
    {"song_id": "S004", "artist": "Taylor Swift", "genre": "Pop", "duration": 180},
    {"song_id": "S005", "artist": "Adele", "genre": "Soul", "duration": 240},
    {"song_id": "S006", "artist": "Drake", "genre": "Hip-Hop", "duration": 250},
    {"song_id": "S007", "artist": "Billie Eilish", "genre": "Alternative", "duration": 220},
    {"song_id": "S008", "artist": "Bruno Mars", "genre": "Funk", "duration": 210},
    {"song_id": "S009", "artist": "The Weeknd", "genre": "R&B", "duration": 230},
    {"song_id": "S010", "artist": "Drake", "genre": "Hip-Hop", "duration": 250},
]

# Generate 100 random records
records = []
base_time = datetime(2025, 11, 5, 8, 0, 0)
for i in range(100):
    user = random.choice(users)
    song = random.choice(songs)
    timestamp = base_time + timedelta(minutes=random.randint(0, 600))  # Random time within 10 hours
    records.append({
        "user_id": user,
        "song_id": song["song_id"],
        "artist": song["artist"],
        "duration": song["duration"],
        "timestamp": timestamp.isoformat(),
        "genre": song["genre"]
    })

# Write CSV
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

print(f"CSV file '{csv_file}' with 100 records created successfully!")
