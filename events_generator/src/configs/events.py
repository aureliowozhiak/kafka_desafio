from dataclasses import dataclass
from typing import Dict


@dataclass
class Event:
    user_id: str
    event_type: str
    timestamp: str
    game_id: str
    payload: Dict[str, str]
