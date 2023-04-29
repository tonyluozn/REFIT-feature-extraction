FROM python:3.9-slim-buster

WORKDIR /opt/flink_job

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y default-jre wget
# Download the Flink Kafka connector JAR file (version 1.14.3) and store it in /opt/flink_job/libs
RUN mkdir -p /opt/flink_job/libs && \
    wget -O /opt/flink_job/libs/flink-connector-kafka_2.12-1.14.3.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.12/1.14.3/flink-connector-kafka_2.12-1.14.3.jar && \
    wget -O /opt/flink_job/libs/kafka-clients-2.8.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar


COPY . .

CMD ["python", "batch_pipeline.py"]
