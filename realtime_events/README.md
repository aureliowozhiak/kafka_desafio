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