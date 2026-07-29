FROM apache/airflow:2.7.2-python3.10

USER root

# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean

# Create folders for Spark Jars
RUN mkdir -p /opt/spark/jars

# Download Postgres JDBC Driver Jar from official binary repository
RUN wget https://postgresql.org -O /opt/spark/jars/postgresql-42.6.0.jar && \
    chmod 644 /opt/spark/jars/postgresql-42.6.0.jar

USER airflow

# Install Python packages without leaving bulk cache layers
RUN pip install --no-cache-dir pyspark==3.4.1 kaggle
