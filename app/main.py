from fastapi import FastAPI
import os
import mysql.connector

app = FastAPI()

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "tidb"),
        port=int(os.getenv("DB_PORT", "4000")),
        user=os.getenv("DB_USER", "root"),
        password="",
        database=os.getenv("DB_NAME", "test")
    )

@app.get("/users/{user_id}")
def get_user(user_id: int):
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user or {"error": "User not found"}

