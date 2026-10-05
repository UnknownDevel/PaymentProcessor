import asyncio
import logging
import random
from datetime import timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.database import session_factory
from messaging.events import (
    DEAD_LETTER_QUEUE,
    MAX_ATTEMPTS,
    PAYMENTS_QUEUE,
    PaymentEvent,
    create_payment_event,
)
from shared.db.models import Outbox, Payment, utcnow
from shared.schemas import PaymentNotification, PaymentStatus
from shared.settings import settings


logger = logging.getLogger(__name__)


class InvalidPaymentEvent(Exception):
    pass


async def emulate_gateway() -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED


class PaymentProcessor:
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self.http_client = http_client

    async def lock_payment(
        self, session: AsyncSession, event: PaymentEvent
    ) -> tuple[Outbox, Payment] | None:
        outbox_event = await session.scalar(
            select(Outbox)
            .where(Outbox.id == event.event_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            outbox_event is None
            or outbox_event.payment_id != event.payment_id
            or outbox_event.topic != PAYMENTS_QUEUE
        ):
            raise InvalidPaymentEvent("Payment event does not match the outbox")
        if outbox_event.handled_at is not None:
            return None

        payment = await session.scalar(
            select(Payment)
            .where(Payment.id == event.payment_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if payment is None:
            raise InvalidPaymentEvent("Payment does not exist")
        if payment.webhook_sent_at is not None or payment.dead_lettered_at is not None:
            outbox_event.handled_at = utcnow()
            return None
        return outbox_event, payment

    def record_failure(
        self,
        session: AsyncSession,
        outbox_event: Outbox,
        payment: Payment,
        event: PaymentEvent,
        error: Exception,
        stage: str,
    ) -> None:
        now = utcnow()
        error_message = f"{type(error).__name__}: {error}"[:2000]
        payment.last_error = error_message
        outbox_event.handled_at = now

        if event.attempt < MAX_ATTEMPTS:
            delay = settings.retry_base_delay * 2 ** (event.attempt - 1)
            session.add(
                create_payment_event(
                    payment.id,
                    attempt=event.attempt + 1,
                    available_at=now + timedelta(seconds=delay),
                )
            )
        else:
            payment.dead_lettered_at = now
            if payment.status == PaymentStatus.PENDING:
                payment.status = PaymentStatus.FAILED
                payment.processed_at = now
            session.add(
                create_payment_event(
                    payment.id,
                    attempt=event.attempt,
                    topic=DEAD_LETTER_QUEUE,
                    details={
                        "failed_event_id": str(event.event_id),
                        "stage": stage,
                        "error": error_message,
                        "failed_at": now.isoformat(),
                    },
                )
            )

        logger.warning(
            "Payment %s failed at %s on attempt %s: %s",
            payment.id,
            stage,
            event.attempt,
            error_message,
        )

    async def process(self, event: PaymentEvent) -> None:
        async with session_factory() as session:
            async with session.begin():
                records = await self.lock_payment(session, event)
                if records is None:
                    return
                outbox_event, payment = records
                if payment.status == PaymentStatus.PENDING:
                    payment.processing_attempts += 1
                    try:
                        payment.status = await emulate_gateway()
                    except Exception as error:
                        self.record_failure(
                            session, outbox_event, payment, event, error, "gateway"
                        )
                        return
                    payment.processed_at = utcnow()

            # Commit the gateway result before attempting delivery of the notification.
            async with session.begin():
                records = await self.lock_payment(session, event)
                if records is None:
                    return
                outbox_event, payment = records
                payment.webhook_attempts += 1
                try:
                    response = await self.http_client.post(
                        payment.webhook_url,
                        json=PaymentNotification.model_validate(payment).model_dump(mode="json"),
                        headers={
                            "Idempotency-Key": str(payment.id),
                            "X-Payment-ID": str(payment.id),
                        },
                    )
                    response.raise_for_status()
                except Exception as error:
                    self.record_failure(
                        session, outbox_event, payment, event, error, "webhook"
                    )
                else:
                    payment.webhook_sent_at = utcnow()
                    payment.last_error = None
                    outbox_event.handled_at = utcnow()
