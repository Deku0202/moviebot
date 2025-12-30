from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters
)
import psycopg2

BOT_TOKEN = ""
ADMIN_ID = 7428371039 

# DB connection
conn = psycopg2.connect(
    host="localhost",
    dbname="movies",
    user="moviebot",
    password="strongpassword"
)
conn.autocommit = True


# TEMP storage for last uploaded file
LAST_UPLOAD = {}

# 1️⃣ Capture uploads in channel
async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post = update.channel_post
    chat_id = post.chat.id
    message_id = post.message_id

    file_id = None

    if post.video:
        file_id = post.video.file_id
    elif post.document:
        file_id = post.document.file_id
    else:
        return

    LAST_UPLOAD[chat_id] = {
        "file_id": file_id,
        "channel_id": chat_id,
        "message_id": message_id
    }

    print("📥 File captured, waiting for /link")

# 2️⃣ Admin links TMDB ID
async def link_tmdb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: /link <tmdb_id>")
        return

    tmdb_id = int(context.args[0])

    if not LAST_UPLOAD:
        await update.message.reply_text("No recent upload found.")
        return

    # get last uploaded file (latest channel)
    data = list(LAST_UPLOAD.values())[-1]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO movie_files
            (tmdb_id, telegram_file_id, telegram_channel_id, telegram_message_id)
            VALUES (%s, %s, %s, %s)
        """, (
            tmdb_id,
            data["file_id"],
            data["channel_id"],
            data["message_id"]
        ))

    await update.message.reply_text(
        f"✅ Linked TMDB {tmdb_id}\nFile saved to database."
    )

# 3️⃣ Test resend (proof it works)
async def test_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /send <tmdb_id>")
        return

    tmdb_id = int(context.args[0])

    with conn.cursor() as cur:
        cur.execute("""
            SELECT telegram_file_id
            FROM movie_files
            WHERE tmdb_id = %s
            LIMIT 1
        """, (tmdb_id,))
        row = cur.fetchone()

    if not row:
        await update.message.reply_text("❌ Movie not found.")
        return

    await context.bot.send_video(
        chat_id=update.effective_chat.id,
        video=row[0],
        caption="🎬 Movie sent using file_id"
    )

# App setup
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
app.add_handler(CommandHandler("link", link_tmdb))
app.add_handler(CommandHandler("send", test_send))

print("🤖 Bot running...")
app.run_polling()
