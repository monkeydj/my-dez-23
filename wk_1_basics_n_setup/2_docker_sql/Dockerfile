FROM python:3.9 

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

# additional layer for storing data
RUN apt-get install curl && mkdir data

ENTRYPOINT ["python", "ingest_data.py"]