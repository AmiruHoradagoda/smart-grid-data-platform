FROM apache/spark:3.5.1

USER root
RUN python3 -m pip install --no-cache-dir PyYAML==6.0.3 psycopg2-binary==2.9.10 \
    && mkdir -p /opt/spark-checkpoints \
    && chown spark:spark /opt/spark-checkpoints

USER spark
