import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from shared.db.database import session_factory
from messaging.broker import create_broker, declare_topology
from shared.db.models import Outbox, utcnow
from shared.settings import settings


logger = logging.getLogger(__name__)


async def wait_for_stop(stop: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        pass


class OutboxPublisher:
    async def publish_batch(self, broker: RabbitBroker) -> int:
        published = 0
        for _ in range(settings.outbox_batch_size):
            failure: Exception | None = None
            async with session_factory() as session:
                async with session.begin():
                    event = await session.scalar(
                        select(Outbox)
                        .where(
                            Outbox.published_at.is_(None),
                            Outbox.available_at <= utcnow(),
                        )
                        .order_by(Outbox.available_at, Outbox.created_at, Outbox.id)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    if event is None:
                        return published

                    event.publish_attempts += 1
                    try:
                        await broker.publish(
                            event.payload,
                            queue=event.topic,
                            persist=True,
                            mandatory=True,
                            timeout=settings.broker_publish_timeout,
                            message_id=str(event.id),
                            correlation_id=str(event.payment_id),
                            headers={"x-attempt": event.payload.get("attempt", 1)},
                        )
                    except Exception as error:
                        failure = error
                        event.last_error = f"{type(error).__name__}: {error}"[:2000]
                        delay = min(
                            settings.retry_base_delay * 2 ** min(event.publish_attempts - 1, 16),
                            60,
                        )
                        event.available_at = utcnow() + timedelta(seconds=delay)
                    else:
                        event.published_at = utcnow()
                        event.last_error = None
                        published += 1

            if failure is not None:
                raise RuntimeError("Outbox publication failed") from failure

        return published

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                async with create_broker() as broker:
                    await declare_topology(broker)
                    while not stop.is_set():
                        if await self.publish_batch(broker) == 0:
                            await wait_for_stop(stop, settings.outbox_poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox publisher will reconnect; events remain in the database")
                await wait_for_stop(stop, settings.retry_base_delay)
