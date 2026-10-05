FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /service

COPY webhook/requirements.txt ./webhook/requirements.txt
RUN pip install --no-cache-dir -r webhook/requirements.txt

RUN useradd --create-home --uid 10001 app
COPY --chown=app:app shared ./shared
COPY --chown=app:app webhook ./webhook

USER app

EXPOSE 9000

CMD ["python", "-m", "webhook.main"]
