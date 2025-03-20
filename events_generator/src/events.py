from datetime import datetime


class Events:
    def __init__(self, action_name, user_id, product_id):
        self.__action_name = action_name
        self.__product_id = product_id
        self.__user_id = user_id

    def create_events(self):
        return {
            "user_id": self.__user_id,
            "event_type": self.__action_name,
            "timestamp": datetime.now().isoformat(),
            "game_id": self.__product_id,
            "payload": {
                "button_id": "789"
            } if self.__action_name == "click" else {}
        }
