from hmac import compare_digest
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from shared.settings import settings


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    if api_key is None or not compare_digest(
        api_key.encode("utf-8"), settings.api_key.get_secret_value().encode("utf-8")
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
