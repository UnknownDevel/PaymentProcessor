FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    RELOAD=false

WORKDIR /service

COPY shared/requirements.txt ./shared/requirements.txt
COPY producer/requirements.txt ./producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

RUN useradd --create-home --uid 10001 app
COPY --chown=app:app shared ./shared
COPY --chown=app:app messaging ./messaging
COPY --chown=app:app producer ./producer

USER app

EXPOSE 8000

CMD ["python", "-m", "producer.main"]
