# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    kubernetes==29.0.0 \
    pandas==2.2.2 \
    pyarrow==17.0.0 \
    boto3==1.34.162 \
    fastapi==0.112.2 \
    uvicorn==0.30.6

WORKDIR /app
COPY app/ /app/

# default command runs the API; the CronJob overrides CMD to run graph_builder
CMD ["uvicorn", "root_cause_service:app", "--host", "0.0.0.0", "--port", "8080"]
