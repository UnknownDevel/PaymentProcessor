from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from messaging.events import create_payment_event
from shared.db.models import Payment, utcnow
from shared.schemas import Payment as PaymentRequest, PaymentStatus


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession,
    request: PaymentRequest,
    idempotency_key: str,
) -> Payment:
    async with session.begin():
        statement = (
            insert(Payment)
            .values(
                id=uuid4(),
                amount=request.amount,
                currency=request.currency,
                description=request.description,
                payment_metadata=request.metadata,
                status=PaymentStatus.PENDING,
                idempotency_key=idempotency_key,
                webhook_url=str(request.webhook_url),
                created_at=utcnow(),
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        payment = (await session.execute(statement)).scalar_one_or_none()
        if payment is not None:
            session.add(create_payment_event(payment.id))
        else:
            payment = (
                await session.execute(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).scalar_one()
            if (
                payment.amount != request.amount
                or payment.currency != request.currency
                or payment.description != request.description
                or payment.payment_metadata != request.metadata
                or payment.webhook_url != str(request.webhook_url)
            ):
                raise IdempotencyConflict

    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
