import os
import mysql.connector
import random
import time
from mysql.connector import Error
from datetime import datetime, timedelta

MAX_RETRIES = 10
USER_TOTAL = 1000000
LOGS_PER_USER = 10
LOG_TOTAL = USER_TOTAL * LOGS_PER_USER

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

# ---------- usersテーブル ----------
print("Creating users table...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    age INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()
print("✅ users table created.")

# ---------- logsテーブル ----------
print("Creating logs table...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    action VARCHAR(50),
    created_at DATETIME,
    INDEX (user_id),
    INDEX (created_at)
)
""")
conn.commit()
print("✅ logs table created.")

# ---------- ユーザー件数確認 ----------
cursor.execute("SELECT COUNT(*) FROM users")
user_count = cursor.fetchone()[0]

if user_count < USER_TOTAL:
    print(f"Inserting {USER_TOTAL:,} users...")
    BATCH_SIZE = 1000
    now = datetime.now()
    for batch_start in range(0, USER_TOTAL, BATCH_SIZE):
        values = []
        for i in range(batch_start, batch_start + BATCH_SIZE):
            name = f"user_{i}"
            age = random.randint(18, 80)
            delta = timedelta(days=random.randint(0, 89), seconds=random.randint(0, 86400))
            created_at = now - delta
            values.append((name, age, created_at.strftime('%Y-%m-%d %H:%M:%S')))
        cursor.executemany("INSERT INTO users (name, age, created_at) VALUES (%s, %s, %s)", values)
        conn.commit()
        if batch_start % 10000 == 0:
            print(f"{batch_start:,} users inserted...")
    print("✅ User insertion complete.")
else:
    print("Users already exist. Skipping user insert.")

# ---------- ログ件数確認 ----------
cursor.execute("SELECT COUNT(*) FROM logs")
log_count = cursor.fetchone()[0]

if log_count < LOG_TOTAL:
    print(f"Inserting {LOG_TOTAL:,} logs...")
    BATCH_SIZE = 1000
    now = datetime.now()
    for user_id_start in range(1, USER_TOTAL + 1, BATCH_SIZE):
        values = []
        for uid in range(user_id_start, user_id_start + BATCH_SIZE):
            for _ in range(LOGS_PER_USER):
                action = random.choice(["view", "click", "purchase", "login", "logout"])
                delta = timedelta(days=random.randint(0, 89), seconds=random.randint(0, 86400))
                created_at = now - delta
                values.append((uid, action, created_at.strftime('%Y-%m-%d %H:%M:%S')))
        cursor.executemany("INSERT INTO logs (user_id, action, created_at) VALUES (%s, %s, %s)", values)
        conn.commit()
        if user_id_start % 10000 == 1:
            print(f"{user_id_start:,} users' logs inserted...")
    print("✅ Log insertion complete.")
else:
    print("Logs already exist. Skipping log insert.")

cursor.close()
conn.close()

