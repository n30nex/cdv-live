FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY backend /app/backend
COPY web /app/web
COPY config.txt /app/config.txt

RUN mkdir -p /app/data

ENV CONFIG_PATH=/app/config.txt
ENV DB_PATH=/app/data/mesh.db

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
