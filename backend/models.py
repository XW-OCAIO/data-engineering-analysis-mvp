from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Event(BaseModel):
    event_time: datetime
    user_id: int
    event_name: str
    category: str
    amount: float
