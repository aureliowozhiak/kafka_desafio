FROM bitnami/spark
COPY SparkApp.scala /app/SparkApp.scala
WORKDIR /app
CMD ["spark-submit", "--class", "SparkApp", "--master", "local", "SparkApp.jar"]
