import pandas as pd
import numpy as np

# Generate users and songs
users = [f"user_{i}" for i in range(1, 100)]
songs = [f"song_{i}" for i in range(1, 50)]
timestamps = pd.date_range(start="2024-04-01", end="2024-04-30", freq="min").tolist()

# Fix: Ensure equal length for all arrays in logs_data
num_records = 1000  # Define a fixed number of records

# Force genre loyalty for two users
fixed_users = ["user_1"] * 50 + ["user_2"] * 50
fixed_songs = ["song_1"] * 50 + ["song_2"] * 50  # Songs assigned specific genres

# Fill the rest with randomized data
random_users = np.random.choice(users, num_records - 100, replace=True).tolist()
random_songs = np.random.choice(songs, num_records - 100, replace=True).tolist()
random_timestamps = np.random.choice(timestamps, num_records - 100, replace=True).tolist()
random_durations = np.random.randint(60, 300, num_records - 100).tolist()

# Combine fixed and random data
logs_data = {
    "user_id": fixed_users + random_users,
    "song_id": fixed_songs + random_songs,
    "timestamp": np.random.choice(timestamps, num_records, replace=True).tolist(),
    "duration_sec": np.random.randint(60, 300, num_records).tolist()
}

logs_df = pd.DataFrame(logs_data)
logs_df.to_csv("listening_logs.csv", index=False)

# Generate songs metadata with correct length
genres = ["Pop", "Rock", "Jazz", "Classical"]
moods = ["Happy", "Sad", "Energetic", "Chill"]

songs_data = {
    "song_id": songs,
    "title": [f"Title_{i}" for i in range(1, len(songs) + 1)],
    "artist": [f"Artist_{i}" for i in range(1, len(songs) + 1)],
    "genre": np.random.choice(genres, len(songs), replace=True),
    "mood": np.random.choice(moods, len(songs), replace=True)
}

songs_df = pd.DataFrame(songs_data)
songs_df.to_csv("songs_metadata.csv", index=False)

print("✅ Data generation completed successfully!")