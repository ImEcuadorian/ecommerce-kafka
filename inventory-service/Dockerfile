FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/app ./src/app
COPY entrypoint.sh .

RUN apt-get update && apt-get install -y dos2unix \
 && dos2unix entrypoint.sh \
 && chmod +x entrypoint.sh \
 && apt-get remove -y dos2unix \
 && rm -rf /var/lib/apt/lists/*

EXPOSE 8001

ENTRYPOINT ["./entrypoint.sh"]
