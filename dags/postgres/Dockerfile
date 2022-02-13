FROM postgres:latest

COPY init_database.sh /docker-entrypoint-initdb.d/init_database.sh

RUN chmod 777 /docker-entrypoint-initdb.d/init_database.sh
USER root
