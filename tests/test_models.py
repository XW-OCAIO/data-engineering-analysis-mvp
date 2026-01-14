from backend.models import Event


def test_event_model_parses() -> None:
    event = Event(
        event_time="2026-01-01 09:00:00",
        user_id=1001,
        event_name="view",
        category="books",
        amount=0,
    )
    assert event.user_id == 1001
