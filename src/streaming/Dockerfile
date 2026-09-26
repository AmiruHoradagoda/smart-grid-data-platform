FROM apache/spark:3.5.1

USER root
RUN python3 -m pip install --no-cache-dir PyYAML==6.0.3 \
    && mkdir -p /opt/spark-checkpoints \
    && chown spark:spark /opt/spark-checkpoints

USER spark
