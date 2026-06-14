import os
import psycopg2
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    """Create database connection"""
    from sqlalchemy import create_engine
    engine = create_engine(DATABASE_URL)
    return engine


def fetch_port_report():
    """Fetch current port congestion and incoming vessels"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Get congestion metrics
        cursor.execute("""
            SELECT congestion_level, arrivals, departures, vessels_in_port, unique_vessels
            FROM dbt_marts.mart_port_congestion
            WHERE event_date = CURRENT_DATE
            ORDER BY event_date DESC
            LIMIT 1
        """)
        congestion = cursor.fetchone()

        # Get incoming vessels (next 24h)
        cursor.execute("""
            SELECT vessel_name, destination, eta
            FROM dbt_staging.stg_vessel_positions
            WHERE eta BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
            ORDER BY eta ASC
            LIMIT 5
        """)
        incoming = cursor.fetchall()

        # Get today's traffic pattern (hourly)
        cursor.execute("""
            SELECT event_hour, vessel_count, arrivals, departures
            FROM dbt_marts.mart_hourly_arrivals
            ORDER BY event_hour ASC
        """)
        hourly = cursor.fetchall()

        conn.close()
        return congestion, incoming, hourly

    except Exception as e:
        print(f"Error fetching report: {e}")
        return None, None, None


def format_port_report(congestion, incoming, hourly):
    """Format the port report as a readable message"""
    message = "📊 *Rotterdam Port Report*\n"
    message += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC\n\n"

    if not congestion:
        message += "⚠️ No data available yet. Run the pipeline first.\n"
        return message

    level, arrivals, departures, vessels_in_port, unique_vessels = congestion
    emoji = "🔴" if level == "HIGH" else "🟡" if level == "MEDIUM" else "🟢"

    # Congestion section
    message += f"{emoji} *Congestion Level:* {level}\n"
    message += f"📈 Arrivals today: {arrivals}\n"
    message += f"📉 Departures today: {departures}\n"
    message += f"⚓ Vessels in port: {vessels_in_port}\n"
    message += f"🚢 Unique vessels: {unique_vessels}\n\n"

    # Incoming vessels
    if incoming:
        message += "*🛢️ Arriving in next 24h:*\n"
        for idx, (name, dest, eta) in enumerate(incoming, 1):
            eta_str = eta.strftime("%H:%M") if eta else "N/A"
            message += f"{idx}. {name} → {dest} @ {eta_str}\n"
    else:
        message += "*🛢️ No arrivals expected in next 24h*\n"

    message += "\n"

    # Hourly traffic
    if hourly:
        message += "*📊 Hourly Traffic Pattern:*\n"
        message += "`Hour | Count | Arr | Dep`\n"
        for hour, count, arr, dep in hourly:
            message += f"`{int(hour):2d}:00 | {count:3d}   | {arr:3d} | {dep:3d}`\n"
    else:
        message += "*No hourly data available yet.*\n"

    return message


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message with inline buttons when /start is issued"""
    keyboard = [
        [InlineKeyboardButton("📊 Get Report", callback_data="get_report")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_report")],
        [InlineKeyboardButton("🚨 Alert Status", callback_data="alert_status")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🌊 *Maritime Port Monitor*\n\n"
        "Welcome to Rotterdam Port Intelligence!\n\n"
        "Press a button to get started:",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button presses"""
    query = update.callback_query
    if not query:
        return

    await query.answer()
    print(f"button_callback: received callback data={query.data}")

    handlers = {
        "get_report": handle_get_report,
        "refresh_report": handle_refresh_report,
        "alert_status": handle_alert_status,
        "back_to_menu": back_to_menu,
    }

    handler = handlers.get(query.data)
    if handler:
        try:
            await handler(query)
        except Exception as e:
            print(f"button_callback: exception in handler '{query.data}': {e}")
            try:
                await query.message.reply_text(
                    "⚠️ An internal error occurred while handling your request."
                )
            except Exception:
                pass
    else:
        print(f"button_callback: unknown callback data '{query.data}'")
        try:
            await query.message.reply_text(
                "⚠️ Unknown action. Please send /start to restart the menu."
            )
        except Exception:
            pass


async def handle_get_report(query):
    """Send the port report"""
    congestion, incoming, hourly = fetch_port_report()
    message = format_port_report(congestion, incoming, hourly)

    keyboard = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_report")],
        [InlineKeyboardButton("🏠 Back", callback_data="back_to_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=message, reply_markup=reply_markup, parse_mode="Markdown"
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer("Already up to date")
            return
        print(f"handle_get_report: BadRequest editing message: {e}")
        try:
            await query.message.reply_text(
                text=message, reply_markup=reply_markup, parse_mode="Markdown"
            )
        except Exception as e2:
            print(f"handle_get_report: reply_text fallback failed: {e2}")
    except Exception as e:
        print(f"handle_get_report: edit_message_text failed: {e}")
        try:
            await query.message.reply_text(
                text=message, reply_markup=reply_markup, parse_mode="Markdown"
            )
        except Exception as e2:
            print(f"handle_get_report: reply_text fallback failed: {e2}")


async def handle_refresh_report(query):
    """Refresh the port report"""
    await handle_get_report(query)


async def handle_alert_status(query):
    """Show alert configuration status"""
    message = (
        "🚨 *Alert Status*\n\n"
        "✅ Telegram notifications: *ACTIVE*\n"
        f"📞 Chat ID: `{TELEGRAM_CHAT_ID}`\n"
        "⏱️ Schedule: Every 6 hours (or on manual trigger)\n\n"
        "You will receive:\n"
        "• Port congestion level (🔴 HIGH / 🟡 MEDIUM / 🟢 LOW)\n"
        "• Daily arrivals/departures\n"
        "• Top 5 incoming vessels (24h)"
    )

    keyboard = [
        [InlineKeyboardButton("🏠 Back", callback_data="back_to_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=message, reply_markup=reply_markup, parse_mode="Markdown"
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer("Already up to date")
            return
        print(f"handle_alert_status: BadRequest editing message: {e}")
        try:
            await query.message.reply_text(
                text=message, reply_markup=reply_markup, parse_mode="Markdown"
            )
        except Exception as e2:
            print(f"handle_alert_status: reply_text fallback failed: {e2}")
    except Exception as e:
        print(f"handle_alert_status: edit_message_text failed: {e}")
        try:
            await query.message.reply_text(
                text=message, reply_markup=reply_markup, parse_mode="Markdown"
            )
        except Exception as e2:
            print(f"handle_alert_status: reply_text fallback failed: {e2}")


async def back_to_menu(query):
    """Return to main menu"""
    keyboard = [
        [InlineKeyboardButton("📊 Get Report", callback_data="get_report")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_report")],
        [InlineKeyboardButton("🚨 Alert Status", callback_data="alert_status")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=(
                "🌊 *Maritime Port Monitor*\n\n"
                "Welcome to Rotterdam Port Intelligence!\n\n"
                "Press a button to get started:"
            ),
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"back_to_menu: edit_message_text failed: {e}")
        try:
            await query.message.reply_text(
                text=(
                    "🌊 *Maritime Port Monitor*\n\n"
                    "Welcome to Rotterdam Port Intelligence!\n\n"
                    "Press a button to get started:"
                ),
                reply_markup=reply_markup,
                parse_mode="Markdown",
            )
        except Exception as e2:
            print(f"back_to_menu: reply_text fallback failed: {e2}")


def main():
    """Start the bot"""
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Command handlers
    application.add_handler(CommandHandler("start", start))

    # Button handlers
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(
        CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
    )

    # Start polling
    print("🤖 Telegram bot started (polling mode)...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
