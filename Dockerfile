FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY golf-ui /app/golf-ui

ENV PYTHONPATH=/app

CMD ["python", "-m", "app.server"]
