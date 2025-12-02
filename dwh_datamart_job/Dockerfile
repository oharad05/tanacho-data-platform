FROM python:3.11-slim

WORKDIR /app

# 依存ライブラリをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY main.py .

# 実行
CMD ["python", "main.py"]
