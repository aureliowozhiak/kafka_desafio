from dotenv import load_dotenv
from os import getenv


load_dotenv()


class Topic:
    BOOTSTRAP_SERVERS = "kafka-server:9092"
    TOPIC_NAME = getenv("TOPIC_NAME")
    WRITE_INTERVAL = 5
