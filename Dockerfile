# Dockerfile for the Flow-of-Funds MCP demo

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "fof.py"]