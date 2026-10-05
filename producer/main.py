import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import uvicorn
from fastapi import Depends, FastAPI

from producer.auth import require_api_key
from shared.db.database import engine
from messaging.outbox import OutboxPublisher
from producer.router.v1_router import router as v1_router
from shared.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=settings.log_level.upper())
    stop = asyncio.Event()
    task = asyncio.create_task(OutboxPublisher().run(stop))
    app.state.outbox_task = task
    try:
        yield
    finally:
        stop.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await engine.dispose()


def create_app() -> FastAPI:
    application = FastAPI(
        title="Payment Processor",
        description="An asynchronous payment processing app",
        lifespan=lifespan,
        dependencies=[Depends(require_api_key)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    application.include_router(v1_router)
    return application


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "producer.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        reload=settings.reload,
    )
