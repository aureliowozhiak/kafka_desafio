from pyspark.sql import functions as F, SparkSession, types as T, Window as W

spark = (
    SparkSession.builder
    .appName("streaming-ingestion")
        .master("local[*]")
            .getOrCreate()

)

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

schema = T.StructType([
    T.StructField("user_id", T.LongType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("game_id", T.LongType()),
    T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
])

raw_data = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-server:9092")
    .option("subscribe", "app-events")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(F.from_json(F.col("json"), schema).alias("data"))
    .select("data.*")
)


# função de persistência
def write_to_mysql(df, epoch_id, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://events_storage:5432/events_storage") \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


# aggreate to event_counts table

event_counts = (
    raw_data
    .withWatermark("timestamp", "5 minutes")  # Define um watermark de 10 minutos
    .groupBy("event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .writeStream
    .outputMode("update")  # "append" não é permitido para agregações em streaming
    # .format("console")
    .foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "event_counts"))
    .trigger(processingTime="1 minute")  # Garante execução a cada 1 minuto
    .start()
)

# aggreate to user_event_counts table

user_event_counts = (
    raw_data
    .withWatermark("timestamp", "5 minutes")  # Define um watermark de 10 minutos
    .groupBy("user_id", "event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .writeStream
    .outputMode("update")  # "append" não é permitido para agregações em streaming
    # .format("console")
    .foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "user_event_counts"))
    .trigger(processingTime="1 minute")  # Garante execução a cada 1 minuto
    .start()
)

user_avg_waiting_time = (
    raw_data
    .withWatermark("timestamp", "5 minutes")
    .groupBy("user_id", F.window("timestamp", "2 minutes"))
    .agg(
        (F.max("timestamp").cast("long") - F.min("timestamp").cast("long")).alias("time_diff")
    )
    .groupBy("user_id")
    .agg(F.round(F.avg("time_diff"), 2).alias("avg_waiting_time"))
    .writeStream
    .outputMode("update")
    # .format("console")
    .foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "user_avg_waiting_time"))
    .trigger(processingTime="1 minute")
    .start()

)


event_counts.awaitTermination()
user_event_counts.awaitTermination()
user_avg_waiting_time.awaitTermination()
