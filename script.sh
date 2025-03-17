#!/bin/bash

# Criando diretórios
mkdir -p realtime_events/{kafka,spark,lambda,db}

# Criando docker-compose.yml para Kafka e Banco de Dados
cat <<EOF > realtime_events/docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    depends_on:
      - zookeeper
  db:
    image: postgres
    container_name: postgres_db
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: events_db
    ports:
      - "5432:5432"
EOF

# Criando script de inicialização do Kafka (criando tópico)
cat <<EOF > realtime_events/kafka/create_topic.sh
#!/bin/bash
docker exec kafka kafka-topics.sh --create --topic user_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
EOF
chmod +x realtime_events/kafka/create_topic.sh

# Criando script Python para simular Lambda
cat <<EOF > realtime_events/lambda/lambda_simulator.py
import json
import time
from kafka import KafkaProducer

events = [
    {"user_id": "123", "event_type": "click", "timestamp": "2023-10-27T10:00:00Z", "game_id": "456", "payload": {"button_id": "789"}},
    {"user_id": "124", "event_type": "login", "timestamp": "2023-10-27T10:01:00Z", "game_id": "457", "payload": {}}
]

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for event in events:
    producer.send('user_events', event)
    time.sleep(1)
EOF

# Criando aplicação Spark em Scala
cat <<EOF > realtime_events/spark/SparkApp.scala
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
EOF

# Criando Dockerfile para Spark
cat <<EOF > realtime_events/spark/Dockerfile
FROM bitnami/spark
COPY SparkApp.scala /app/SparkApp.scala
WORKDIR /app
CMD ["spark-submit", "--class", "SparkApp", "--master", "local", "SparkApp.jar"]
EOF

# Criando script SQL para criar tabelas
cat <<EOF > realtime_events/db/schema.sql
CREATE TABLE event_metrics (
    event_type VARCHAR(50),
    event_count INT,
    unique_users INT
);
EOF

# Criando README
cat <<EOF > realtime_events/README.md
# Processamento de Eventos em Tempo Real

## Passo a Passo

1. Subir os containers Kafka e Banco de Dados:
   ```sh
   docker-compose up -d
   ```
2. Criar o tópico Kafka:
   ```sh
   bash kafka/create_topic.sh
   ```
3. Rodar o simulador Lambda:
   ```sh
   python3 lambda/lambda_simulator.py
   ```
4. Rodar a aplicação Spark:
   ```sh
   docker build -t spark-app ./spark
   docker run --network host spark-app
   ```
5. Criar tabelas no banco de dados:
   ```sh
   docker exec -i postgres_db psql -U user -d events_db < db/schema.sql
   ```
EOF

# Exibir mensagem final
echo "Configuração concluída! Consulte realtime_events/README.md para instruções. "
