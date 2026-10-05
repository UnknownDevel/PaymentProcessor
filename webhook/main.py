import logging

import uvicorn
from fastapi import FastAPI

from shared.schemas import PaymentNotification


logger = logging.getLogger(__name__)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook")
async def receive_webhook(notification: PaymentNotification) -> dict[str, bool | str]:
    logger.info("Webhook received: %s", notification.model_dump_json())
    return {"received": True, "payment_id": str(notification.payment_id)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=9000)
