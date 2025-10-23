FROM python:3.11-slim

WORKDIR /app

# 依存関係のインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY main.py .

# ポート設定
ENV PORT 8080
EXPOSE 8080

# アプリケーション起動
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app