import sqlite3
import psycopg2

# --- SQLite ---
sqlite_conn = sqlite3.connect("data/planta.db")
sqlite_cur = sqlite_conn.cursor()

# --- PostgreSQL (Neon) ---
pg_conn = psycopg2.connect(
    host="ep-icy-frog-abynjpum-pooler.eu-west-2.aws.neon.tech",
    database="neondb",
    user="neondb_owner",
    password="TU_PASSWORD",
    port=5432,
)
pg_cur = pg_conn.cursor()

# --------------------------------------------------
# Migrar analiticas
# --------------------------------------------------
sqlite_cur.execute("""
    SELECT datetime, punto, HC, SS, DQO, Sulf
    FROM analiticas
""")

rows = sqlite_cur.fetchall()

# Limpieza previa (opcional pero recomendable)
pg_cur.execute("DELETE FROM analiticas")

for r in rows:
    ts = r[0]  # datetime de SQLite → ts en Postgres

    pg_cur.execute(
        """
        INSERT INTO analiticas (ts, punto, hc, ss, dqo, sulf)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (ts, r[1], r[2], r[3], r[4], r[5])
    )

pg_conn.commit()

sqlite_conn.close()
pg_conn.close()

print(f"✅ Migrados {len(rows)} registros a PostgreSQL")
