# Build context: repository root (see docker-compose.yml).
FROM python:3.13-slim
WORKDIR /app
COPY workers/python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY workers/python/orders ./orders
COPY workers/python/worker.py workers/python/load.py ./
COPY audit/*.py ./audit/
ENV PYTHONUNBUFFERED=1
EXPOSE 9465
CMD ["python", "worker.py"]
