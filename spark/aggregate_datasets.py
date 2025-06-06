from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr

minimum_votes = 10000


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
    titles = titles.filter((col("averageRating") > 6.0) & (col("numVotes") > minimum_votes))
    titles = titles.join(crew, on="tconst", how="left")

    mean_avg_rating = titles.select(avg("averageRating")).collect()[0][0]
    
    titles = titles.withColumn("weightedRating",
        expr(f"(numVotes / (numVotes + {minimum_votes}) * averageRating) + ({minimum_votes} / (numVotes + {minimum_votes}) * {mean_avg_rating})")
    )

    titles.coalesce(1).write.csv("data/top_rated_weighted.csv", header=True, mode="overwrite")

    spark.stop()