from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from shared.db.models import Outbox, utcnow


PAYMENTS_QUEUE = "payments.new"
DEAD_LETTER_QUEUE = "payments.dlq"
MAX_ATTEMPTS = 3


class PaymentEvent(BaseModel):
    event_id: UUID
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPTS)


def create_payment_event(
    payment_id: UUID,
    *,
    attempt: int = 1,
    available_at: datetime | None = None,
    topic: str = PAYMENTS_QUEUE,
    details: dict[str, Any] | None = None,
) -> Outbox:
    event = PaymentEvent(event_id=uuid4(), payment_id=payment_id, attempt=attempt)
    return Outbox(
        id=event.event_id,
        payment_id=payment_id,
        topic=topic,
        payload={**event.model_dump(mode="json"), **(details or {})},
        available_at=available_at or utcnow(),
    )
