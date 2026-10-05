from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from messaging.events import DEAD_LETTER_QUEUE, MAX_ATTEMPTS, PAYMENTS_QUEUE
from shared.settings import settings


dead_letter_exchange = RabbitExchange(
    "payments.dlx", type=ExchangeType.DIRECT, durable=True
)
dead_letter_queue = RabbitQueue(
    DEAD_LETTER_QUEUE, durable=True, arguments={"x-queue-type": "quorum"}
)
payment_queue = RabbitQueue(
    PAYMENTS_QUEUE,
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
retry_queues = {
    attempt: RabbitQueue(
        f"payments.retry.{attempt}",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": max(1, int(settings.retry_base_delay * 2 ** (attempt - 1) * 1000)),
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": PAYMENTS_QUEUE,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )
    for attempt in range(1, MAX_ATTEMPTS)
}


def create_broker() -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        timeout=settings.broker_publish_timeout,
        graceful_timeout=settings.webhook_timeout + 10,
        default_channel=Channel(
            prefetch_count=1, publisher_confirms=True, on_return_raises=True
        ),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(dead_letter_exchange)
    queue = await broker.declare_queue(dead_letter_queue)
    await queue.bind(exchange, routing_key=DEAD_LETTER_QUEUE)
    await broker.declare_queue(payment_queue)
    for retry_queue in retry_queues.values():
        await broker.declare_queue(retry_queue)
