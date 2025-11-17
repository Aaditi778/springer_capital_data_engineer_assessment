FROM apache/spark:latest

WORKDIR /app

# Only copy script
COPY your_script.py /app/your_script.py

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/app/your_script.py"]
