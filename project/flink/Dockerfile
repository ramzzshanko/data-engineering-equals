FROM flink:1.14.4-scala_2.12-java8

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink kafka-python influxdb-client

COPY realtime_processing.py /opt/flink/realtime_processing.py