FROM python:3.10

WORKDIR /app

COPY . .

# Requirement for python
RUN pip install discord.py
RUN pip install psycopg2

ARG DISCORD_TOKEN
ARG POSTGRESQL_DBNAME
ARG POSTGRESQL_USER
ARG POSTGRESQL_PASSWORD
ARG POSTGRESQL_HOST
ARG POSTGRESQL_PORT

CMD ["python", "sqlitetopostgre.py"]
