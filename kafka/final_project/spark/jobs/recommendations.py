from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PRODUCTS_PATH = "hdfs://hadoop-namenode:9000/marketplace/products/*.jsonl"
EVENTS_PATH = "hdfs://hadoop-namenode:9000/marketplace/client-events/*.jsonl"
OUTPUT_PATH = "hdfs://hadoop-namenode:9000/marketplace/recommendations"


spark = (
    SparkSession.builder
    .appName("marketplace-recommendations")
    .getOrCreate()
)

product_window = Window.partitionBy("product_id").orderBy(
    F.col("updated_at").desc()
)

products = (
    spark.read.json(PRODUCTS_PATH)
    .withColumn("row_number", F.row_number().over(product_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
events = spark.read.json(EVENTS_PATH)

latest_search_window = Window.partitionBy("user_id").orderBy(
    F.col("requested_at").desc()
)

latest_searches = (
    events
    .filter(F.col("request_type") == "search")
    .withColumn("row_number", F.row_number().over(latest_search_window))
    .filter(F.col("row_number") == 1)
    .select("user_id", "query")
)

users = events.select("user_id").where(F.col("user_id").isNotNull()).distinct()
users_with_query = users.join(latest_searches, "user_id", "left")

candidate_products = users_with_query.crossJoin(
    products.select(
        "product_id",
        F.col("name").alias("product_name"),
        F.col("stock.available").alias("available"),
    )
)

scored = candidate_products.withColumn(
    "query_match",
    F.when(
        F.col("query").isNotNull()
        & F.expr("instr(lower(product_name), lower(query)) > 0"),
        F.lit(1),
    ).otherwise(F.lit(0)),
)

recommendation_window = Window.partitionBy("user_id").orderBy(
    F.col("query_match").desc(),
    F.col("available").desc(),
    F.col("product_id"),
)

recommendations = (
    scored
    .withColumn("rank", F.row_number().over(recommendation_window))
    .filter(F.col("rank") <= 3)
    .select(
        "user_id",
        "product_id",
        "product_name",
        F.when(
            F.col("query_match") == 1,
            F.lit("Товар соответствует последнему поисковому запросу"),
        ).otherwise(F.lit("Популярный товар с высоким доступным остатком")).alias("reason"),
        F.date_format(
            F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ).alias("generated_at"),
    )
)

recommendations.write.mode("overwrite").json(OUTPUT_PATH)
recommendations.show(truncate=False)
spark.stop()
