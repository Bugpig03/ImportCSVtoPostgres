import os
import sqlite3
import psycopg2
from psycopg2 import sql

# Dossier contenant les bases de données SQLite
# Construction du chemin relatif
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

dbName = os.environ.get('POSTGRESQL_DBNAME')
user = os.environ.get('POSTGRESQL_USER')
password = os.environ.get('POSTGRESQL_PASSWORD')
host = os.environ.get('POSTGRESQL_HOST')
port = os.environ.get('POSTGRESQL_PORT')

# Connexion à la base de données PostgreSQL
pg_conn = psycopg2.connect(
    dbname=dbName,
    user=user,
    password=password,
    host=host,
    port=port
)
pg_cursor = pg_conn.cursor()

# Création de la table PostgreSQL avec clé primaire composite
create_table_query = """
CREATE TABLE IF NOT EXISTS stats (
    user_id BIGINT NOT NULL,
    server_id BIGINT NOT NULL,
    messages INTEGER DEFAULT 0,
    seconds INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    PRIMARY KEY (server_id, user_id)
);
"""
pg_cursor.execute(create_table_query)
pg_conn.commit()

# Parcours des fichiers dans le dossier DATA
for filename in os.listdir(DATA_DIR):
    if filename.startswith("server_") and filename.endswith(".db"):
        server_id = filename.split('_')[1].split('.')[0]
        sqlite_path = os.path.join(DATA_DIR, filename)

        # Connexion à la base de données SQLite
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()

        # Récupération des données depuis SQLite
        sqlite_cursor.execute("SELECT id, messages, seconds FROM users")
        rows = sqlite_cursor.fetchall()

        # Insertion des données dans PostgreSQL avec valeurs par défaut si NULL
        for row in rows:
            user_id, messages, seconds = row
            
            # Si messages ou seconds sont None, on les remplace par 0
            messages = messages if messages is not None else 0
            seconds = seconds if seconds is not None else 0

            insert_query = sql.SQL("""
                INSERT INTO stats (user_id, server_id, messages, seconds)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (server_id, user_id) DO NOTHING;
            """)
            pg_cursor.execute(insert_query, (user_id, server_id, messages, seconds))

        sqlite_conn.close()

# Commit final et fermeture de la connexion PostgreSQL
pg_conn.commit()
pg_cursor.close()
pg_conn.close()

print("Migration terminée avec succès !")
