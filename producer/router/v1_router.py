from fastapi import APIRouter

from producer.router.v1.payments import router as payment_router


router = APIRouter(prefix="/api/v1")
router.include_router(payment_router)
