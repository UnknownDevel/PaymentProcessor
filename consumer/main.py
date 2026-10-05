import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from shared.db.database import engine
from messaging.events import MAX_ATTEMPTS, PaymentEvent
from messaging.broker import create_broker, declare_topology, payment_queue, retry_queues
from messaging.outbox import OutboxPublisher
from consumer.processor import InvalidPaymentEvent, PaymentProcessor
from shared.settings import settings


logger = logging.getLogger(__name__)
broker = create_broker()
app = FastStream(broker)
http_client: httpx.AsyncClient | None = None
processor: PaymentProcessor | None = None
outbox_stop = asyncio.Event()
outbox_task: asyncio.Task[None] | None = None


async def retry_message(
    message: RabbitMessage, event: PaymentEvent, error: Exception | str
) -> None:
    payload = event.model_dump(mode="json")
    error_message = (
        error if isinstance(error, str) else f"{type(error).__name__}: {error}"
    )[:2000]
    headers = {"x-attempt": event.attempt, "x-last-error": error_message}
    if event.attempt < MAX_ATTEMPTS:
        queue = retry_queues[event.attempt]
        payload["attempt"] = event.attempt + 1
        headers["x-attempt"] = event.attempt + 1
    else:
        try:
            if processor is None:
                raise RuntimeError("Payment processor is not initialized")
            await processor.finalize_failure(event, error_message)
        except InvalidPaymentEvent:
            logger.exception("Unknown payment event was sent to the dead letter queue")
            await message.reject(requeue=False)
            return
        except Exception:
            logger.exception("Finalization will be retried for payment %s", event.payment_id)
            queue = retry_queues[MAX_ATTEMPTS - 1]
            headers["x-finalization-error"] = error_message
        else:
            await message.ack()
            return

    try:
        await broker.publish(
            payload,
            queue=queue,
            persist=True,
            mandatory=True,
            timeout=settings.broker_publish_timeout,
            headers=headers,
            message_id=str(event.event_id),
            correlation_id=str(event.payment_id),
        )
    except Exception:
        logger.exception("Failed to publish a retry for payment %s", event.payment_id)
        await asyncio.sleep(settings.retry_base_delay)
        await message.nack(requeue=True)
    else:
        await message.ack()


@broker.subscriber(payment_queue, ack_policy=AckPolicy.MANUAL, no_reply=True)
async def process_payment(message: RabbitMessage) -> None:
    try:
        event = PaymentEvent.model_validate(await message.decode())
        attempt = int(message.headers.get("x-attempt", event.attempt))
        if not 1 <= attempt <= MAX_ATTEMPTS:
            raise ValueError("Invalid retry attempt")
        event = event.model_copy(update={"attempt": max(event.attempt, attempt)})
        finalization_error = message.headers.get("x-finalization-error")
        if finalization_error is not None and (
            not isinstance(finalization_error, str) or event.attempt != MAX_ATTEMPTS
        ):
            raise ValueError("Invalid finalization message")
    except (ValidationError, ValueError, TypeError):
        logger.exception("Invalid payment message was sent to the dead letter queue")
        await message.reject(requeue=False)
        return

    if finalization_error is not None:
        await retry_message(message, event, finalization_error)
        return

    try:
        if processor is None:
            raise RuntimeError("Payment processor is not initialized")
        await processor.process(event)
    except InvalidPaymentEvent:
        logger.exception("Unknown payment event was sent to the dead letter queue")
        await message.reject(requeue=False)
    except Exception as error:
        logger.exception("Consumer failed for payment %s", event.payment_id)
        await retry_message(message, event, error)
    else:
        await message.ack()


@app.on_startup
async def startup() -> None:
    global http_client, processor
    logging.basicConfig(level=settings.log_level.upper())
    await broker.connect()
    await declare_topology(broker)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    processor = PaymentProcessor(http_client)


@app.after_startup
async def start_outbox() -> None:
    global outbox_task
    outbox_stop.clear()
    outbox_task = asyncio.create_task(OutboxPublisher().run(outbox_stop))


@app.on_shutdown
async def stop_outbox() -> None:
    outbox_stop.set()
    if outbox_task is not None:
        outbox_task.cancel()
        with suppress(asyncio.CancelledError):
            await outbox_task


@app.after_shutdown
async def shutdown() -> None:
    if http_client is not None:
        await http_client.aclose()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(app.run())
