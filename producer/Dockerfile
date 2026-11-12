FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "confluent-kafka>=2.5,<3"
# El contexto de build es ./producer: se copia como paquete "producer"
COPY . /app/producer/
