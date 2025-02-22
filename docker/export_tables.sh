#!/bin/bash
# Variables based on your docker-compose configuration.
CONTAINER_NAME="docker-db-1"        # Docker container name for Postgres.
USERNAME="postgres"        # From POSTGRES_USER.
DATABASE="postgres"        # From POSTGRES_DB.

# Retrieve a list of tables from the "public" schema in a clean format.
TABLES=$(docker exec -t $CONTAINER_NAME psql -U $USERNAME -d $DATABASE -t -A -c \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='public';")

# Loop through each table and export it to a CSV file.
for table in $TABLES; do
    echo "Exporting table: $table"
    docker exec -t $CONTAINER_NAME psql -U $USERNAME -d $DATABASE -c \
      "\COPY $table TO STDOUT WITH CSV HEADER" > "${table}.csv"
done

echo "Export completed."
