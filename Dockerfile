FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

RUN mkdir -p /app/logs

EXPOSE 7755

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7755", "--log-level", "info"]
