from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Event(BaseModel):
    event_time: datetime
    user_id: int
    event_name: str
    category: str
    amount: float


class Metrics(BaseModel):
    total_events: int
    total_users: int
    total_amount: float
