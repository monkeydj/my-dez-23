# load env from file
source $(dirname $0)/.env

# echo $PG_CONN_STRING 
# echo $DATA_TABLE_NAME 
# echo $DATA_URL

# run dockerize script with loaded env
docker run -it --rm --network=pg-network dez23/taxi_ingest:csv-test \
  --conn=$PG_CONN_STRING --table_name=$DATA_TABLE_NAME --csv_url=$DATA_URL