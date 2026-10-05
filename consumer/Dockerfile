FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /service

COPY shared/requirements.txt ./shared/requirements.txt
COPY consumer/requirements.txt ./consumer/requirements.txt
RUN pip install --no-cache-dir -r consumer/requirements.txt

RUN useradd --create-home --uid 10001 app
COPY --chown=app:app shared ./shared
COPY --chown=app:app messaging ./messaging
COPY --chown=app:app consumer ./consumer

USER app

CMD ["python", "-m", "consumer.main"]
