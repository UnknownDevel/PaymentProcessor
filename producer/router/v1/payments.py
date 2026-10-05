from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.database import get_session
from shared.schemas import Payment, PaymentCreated, PaymentDetails
from producer.services.payments import IdempotencyConflict, create_payment, get_payment


router = APIRouter(prefix="/payments", tags=["payments"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.post("", response_model=PaymentCreated, status_code=status.HTTP_202_ACCEPTED)
async def register_payment(
    payment: Payment,
    session: Session,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
) -> PaymentCreated:
    if not idempotency_key.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key is empty")
    try:
        result = await create_payment(session, payment, idempotency_key)
    except IdempotencyConflict as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different payment",
        ) from error
    return PaymentCreated.model_validate(result)


@router.get("/{payment_id}", response_model=PaymentDetails)
async def retrieve_payment(payment_id: UUID, session: Session) -> PaymentDetails:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetails.model_validate(payment)
