FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# 初回にデータを投入（既にある場合はスキップ）
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000"]

