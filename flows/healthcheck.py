import os
import sqlite3

def main():
    db_url = os.environ["TURSO_DB_URL"]
    token = os.environ["TURSO_AUTH_TOKEN"]

    conn = sqlite3.connect(
        f"file:{db_url}?authToken={token}",
        uri=True,
        check_same_thread=False,
    )

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM articles")
    cnt = cur.fetchone()[0]

    print("✅ Connected to Turso")
    print("📊 Articles in DB:", cnt)

    conn.close()

if __name__ == "__main__":
    main()
