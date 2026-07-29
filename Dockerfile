FROM apache/airflow:2.7.2-python3.10

USER root

# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean

# Create folders for Spark Jars
RUN mkdir -p /opt/spark/jars

# Download Postgres JDBC Driver Jar
RUN wget https://postgresql.org -O /opt/spark/jars/postgresql-42.6.0.jar
RUN chmod 644 /opt/spark/jars/postgresql-42.6.0.jar

USER airflow

# Install Python packages
RUN pip install pyspark==3.4.1 kaggle
