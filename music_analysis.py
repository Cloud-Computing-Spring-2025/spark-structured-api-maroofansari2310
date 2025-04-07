import findspark
findspark.init()  # Initialize Spark environment

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, max, weekofyear, year, hour, row_number,
    desc, array_contains, collect_set, sum, to_timestamp
)
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("MusicStreamingAnalysis").getOrCreate()

# Read datasets
logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
logs_df = logs_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

songs_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: Find each user’s favorite genre
joined_logs_songs = logs_df.join(songs_df, "song_id")
user_genre_counts = joined_logs_songs.groupBy("user_id", "genre").agg(count("*").alias("count"))
window_spec = Window.partitionBy("user_id").orderBy(desc("count"))
user_favorite_genre = user_genre_counts.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1).drop("rank")
user_favorite_genre.write.format("csv").save("output/user_favorite_genres")

# Task 2: Calculate average listen time per song
avg_listen_time = logs_df.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration_sec"))
avg_listen_time.write.format("csv").save("output/avg_listen_time_per_song")

# Task 3: List top 10 most played songs this week
logs_with_week = logs_df.withColumn("year", year(col("timestamp"))) \
    .withColumn("week", weekofyear(col("timestamp")))
max_year_week = logs_with_week.agg(max("year").alias("max_year"), max("week").alias("max_week")).first()
current_week_logs = logs_with_week.filter(
    (col("year") == max_year_week["max_year"]) & (col("week") == max_year_week["max_week"])
)
top_songs = current_week_logs.groupBy("song_id").agg(count("*").alias("play_count")) \
    .join(songs_df, "song_id").orderBy(desc("play_count")).limit(10)
top_songs.write.format("csv").save("output/top_songs_this_week")

# Task 4: Recommend “Happy” songs to users who mostly listen to “Sad” songs
user_mood = logs_df.join(songs_df, "song_id").select("user_id", "mood", "song_id")
user_mood_counts = user_mood.groupBy("user_id", "mood").agg(count("*").alias("count"))
user_total = user_mood_counts.groupBy("user_id").agg(sum("count").alias("total"))
user_sad = user_mood.filter(col("mood") == "Sad").groupBy("user_id").agg(count("*").alias("sad_count"))
user_sad_ratio = user_total.join(user_sad, "user_id", "left") \
    .fillna(0).withColumn("ratio", col("sad_count") / col("total")) \
    .filter(col("ratio") > 0.5).select("user_id")
user_played = logs_df.groupBy("user_id").agg(collect_set("song_id").alias("played"))
happy_songs = songs_df.filter(col("mood") == "Happy").select("song_id", "title", "artist")
recommendations = user_sad_ratio.join(user_played, "user_id") \
    .crossJoin(happy_songs).filter(~array_contains(col("played"), col("song_id")))
window_spec = Window.partitionBy("user_id").orderBy("song_id")
top_recommendations = recommendations.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 3).select("user_id", "song_id", "title", "artist")
top_recommendations.write.format("csv").save("output/happy_recommendations")

# Task 5: Compute genre loyalty score
user_genre_total = joined_logs_songs.groupBy("user_id").agg(count("*").alias("total_plays"))
user_genre_max = joined_logs_songs.groupBy("user_id", "genre").agg(count("*").alias("genre_plays")) \
    .groupBy("user_id").agg(max("genre_plays").alias("max_plays"))
loyalty_scores = user_genre_total.join(user_genre_max, "user_id") \
    .withColumn("loyalty_score", col("max_plays") / col("total_plays")) \
    .filter(col("loyalty_score") > 0.8)
loyalty_scores.write.format("csv").save("output/genre_loyalty_scores")

# Task 6: Identify night owl users
night_owls = logs_df.withColumn("hour", hour(col("timestamp"))) \
    .filter((col("hour") >= 0) & (col("hour") < 5)).select("user_id").distinct()
night_owls.write.format("csv").save("output/night_owl_users")

spark.stop()