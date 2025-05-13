import os
import mysql.connector
import random


import time
import mysql.connector
from mysql.connector import Error
import os

MAX_RETRIES = 10
for i in range(MAX_RETRIES):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "tidb"),
            port=int(os.getenv("DB_PORT", "4000")),
            user=os.getenv("DB_USER", "root"),
            password="",
            database=os.getenv("DB_NAME", "test")
        )
        break
    except Error as e:
        print(f"[Retry {i+1}] Cannot connect to TiDB: {e}")
        time.sleep(5)
else:
    raise Exception("TiDB connection failed after retries.")

cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    age INT
)
""")

cursor.execute("SELECT COUNT(*) FROM users")
count = cursor.fetchone()[0]

if count < 1000000:
    print("Inserting 1,000,000 users...")
    for i in range(1000000):
        cursor.execute("INSERT INTO users (name, age) VALUES (%s, %s)",
                       (f"user_{i}", random.randint(18, 80)))
        if i % 10000 == 0:
            conn.commit()
            print(f"{i} users inserted...")
    conn.commit()
else:
    print("Users already inserted, skipping.")

cursor.close()
conn.close()

