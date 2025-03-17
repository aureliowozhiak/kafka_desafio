import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaSpark").master("local[*]").getOrCreate()
    import spark.implicits._

    val schema = new StructType()
      .add("user_id", StringType)
      .add("event_type", StringType)
      .add("timestamp", StringType)
      .add("game_id", StringType)
      .add("payload", MapType(StringType, StringType))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_events")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")

    val aggDf = df.groupBy("event_type").agg(
      count("user_id").alias("event_count"),
      approx_count_distinct("user_id").alias("unique_users")
    )

    val query = aggDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
