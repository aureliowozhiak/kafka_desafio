from configs.actions import Actions
from configs.products import Products
from configs.topic import Topic
from configs.users import Users
from events import Events
from json import dumps
from kafka import KafkaProducer
from numpy.random import randint
from time import sleep


class Write:
    def __init__(self):
        self._actions = Actions
        self._current_action = None
        self._products = Products
        self._topic = Topic
        self._users = Users

    def _get_random_entity(self, entity_list, entity_length):
        return entity_list[randint(0, entity_length)]

    def _write_event(self, event):
        print(f"Writing event {event} to topic {self._topic.TOPIC_NAME}")
        producer = KafkaProducer(
            bootstrap_servers=self._topic.BOOTSTRAP_SERVERS,
            value_serializer=lambda x: dumps(x).encode("utf-8")
        )
        producer.send(topic=self._topic.TOPIC_NAME, value=event)

    def run(self):
        while True:
            current_user = self._get_random_entity(
                entity_list=self._users.USERS_IDS,
                entity_length=self._users.USERS_RANGE
            )
            current_product = self._get_random_entity(
                entity_list=self._products.PRODUCTS_IDS,
                entity_length=self._products.PRODUCTS_RANGE
            )
            current_action = self._get_random_entity(
                entity_list=self._actions.ACTIONS_NAMES,
                entity_length=self._actions.ACTIONS_RANGE
            )
            event = Events(
                action_name=current_action,
                user_id=current_user,
                product_id=current_product
            ).create_events()
            self._write_event(event)
            sleep(self._topic.WRITE_INTERVAL)


Write().run()
