FROM python:3.11-slim

WORKDIR /app

# 依存関係インストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコピー
COPY main.py .

# gunicornで起動
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 600 main:app
