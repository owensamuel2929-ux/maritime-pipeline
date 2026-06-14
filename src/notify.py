import os
import psycopg2
import requests


def send_alert():
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id or token.startswith("your_"):
        print("Telegram not configured, skipping alert")
        return

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT vessel_name, destination, eta
        FROM dbt_staging.stg_vessel_positions
        WHERE eta BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
        ORDER BY eta ASC
        LIMIT 5
    """)
    incoming = cursor.fetchall()

    cursor.execute("""
        SELECT congestion_level, arrivals, vessels_in_port
        FROM dbt_marts.mart_port_congestion
        WHERE event_date = CURRENT_DATE
        ORDER BY vessel_type
        LIMIT 1
    """)
    congestion = cursor.fetchone()
    conn.close()

    message = "*Rotterdam Port Update*\n\n"

    if congestion:
        level = congestion[0]
        emoji = "🔴" if level == "HIGH" else "🟡" if level == "MEDIUM" else "🟢"
        message += f"{emoji} Congestion: *{level}*\n"
        message += f"Arrivals today: {congestion[1]}\n"
        message += f"Vessels in port: {congestion[2]}\n\n"

    if incoming:
        message += "*Arriving next 24h:*\n"
        for name, dest, eta in incoming:
            message += f"- {name} -> {dest} @ {eta}\n"

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
        timeout=10
    )
