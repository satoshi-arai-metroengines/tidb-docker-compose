import os
import mysql.connector
import random
import time
from mysql.connector import Error
from datetime import datetime

MAX_RETRIES = 10

# 接続リトライ
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

# usersテーブル作成
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    age INT
)
""")

# 件数チェック
cursor.execute("SELECT COUNT(*) FROM users")
count = cursor.fetchone()[0]

# 100万件バルクインサート（1,000件ずつ）
if count < 1000000:
    print("Inserting 1,000,000 users in bulk...")
    BATCH_SIZE = 1000
    total = 1_000_000
    for batch_start in range(0, total, BATCH_SIZE):
        values = [(f"user_{i}", random.randint(18, 80)) for i in range(batch_start, batch_start + BATCH_SIZE)]
        cursor.executemany("INSERT INTO users (name, age) VALUES (%s, %s)", values)
        conn.commit()
        if batch_start % 10000 == 0:
            print(f"{batch_start} inserted...")
    print("Insertion complete.")
else:
    print("Users already inserted, skipping.")

# OLAPクエリ（年齢別のユーザー数を集計）
print("\nRunning OLAP query: 集計開始")

start = time.time()
cursor.execute("""
    SELECT age, COUNT(*) AS total
    FROM users
    GROUP BY age
    ORDER BY total DESC
""")
results = cursor.fetchall()
end = time.time()

print(f"OLAP query executed in {end - start:.2f} seconds\n")

for age, total in results:
    print(f"Age {age}: {total} users")

cursor.close()
conn.close()

