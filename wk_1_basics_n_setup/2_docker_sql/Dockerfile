FROM python:3.9 

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_csv_data.py ingest_data.py

# additional layer for storing data
RUN apt-get install wget && mkdir data

ENTRYPOINT ["python", "ingest_data.py"]