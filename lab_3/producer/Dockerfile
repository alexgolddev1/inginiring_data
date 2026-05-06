FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/src ./src

CMD ["python", "src/main.py"]
