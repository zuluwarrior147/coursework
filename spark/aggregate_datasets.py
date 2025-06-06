from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr

VOTES_THRESHOLD = 10000
RATING_THRESHOLD = 7.0


def aggregate_datasets():
    spark = SparkSession.builder \
    .appName("CSV Aggregator") \
    .getOrCreate()

    crew = spark.read.csv("crew.csv", header=True, inferSchema=True)
    titles = spark.read.csv("basic_titles.csv", header=True, inferSchema=True)
    ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)


    titles = titles.filter(col("titleType") == "movie")
    titles = titles \
        .join(ratings, on="tconst", how="left")
    titles = titles.filter((col("averageRating") > RATING_THRESHOLD) & (col("numVotes") > VOTES_THRESHOLD))
    titles = titles.join(crew, on="tconst", how="left")

    mean_avg_rating = titles.select(avg("averageRating")).collect()[0][0]
    
    titles = titles.withColumn("weightedRating",
        expr(f"(numVotes / (numVotes + {VOTES_THRESHOLD}) * averageRating) + ({VOTES_THRESHOLD} / (numVotes + {VOTES_THRESHOLD}) * {mean_avg_rating})")
    )

    titles.coalesce(1).write.csv("data/processed/top_rated_weighted.csv", header=True, mode="overwrite")

    spark.stop()