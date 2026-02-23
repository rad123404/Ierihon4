import asyncio
import json
import logging
import re
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import aiofiles

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.error import BadRequest, RetryAfter
from telegram.constants import ParseMode

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")

DATA_DIR = Path(__file__).parent

BIRTHDAYS = []
DUTIES_TEXT = ""
SCHEDULES = {}

chat_states = defaultdict(lambda: {
    "votes": {},
    "poll_message_id": None,
    "results_message_id": None,
    "last_save": 0.0,
    "dirty": False
})

# ================== ДОБАВЛЕНО ==================
KNOWN_CHATS = set()
KNOWN_CHATS_FILE = DATA_DIR / "known_chats.json"
# ===============================================

file_write_lock = asyncio.Lock()

last_birthday_sent_date = None
last_pinned_birthday_msg_id = {}


# ================== ДОБАВЛЕНО ==================
async def save_known_chats():
    try:
        async with aiofiles.open(KNOWN_CHATS_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(list(KNOWN_CHATS)))
    except Exception as e:
        logger.error(f"Ошибка сохранения known_chats: {e}")


async def load_known_chats():
    global KNOWN_CHATS
    if not KNOWN_CHATS_FILE.exists():
        return
    try:
        async with aiofiles.open(KNOWN_CHATS_FILE, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())
            KNOWN_CHATS = set(data)
    except Exception as e:
        logger.error(f"Ошибка загрузки known_chats: {e}")
# ===============================================


def get_file(chat_id: int, chat_title: str) -> Path:
    safe = re.sub(r'[^a-zA-Z0-9_-]', '_', chat_title or f"chat_{chat_id}")[:40]
    return DATA_DIR / f"stolovaya_{safe}_{chat_id}.json"


async def save_state_periodically(chat_id: int, chat_title: str):
    now_ts = datetime.utcnow().timestamp()
    state = chat_states[chat_id]
    if not state["dirty"] or now_ts - state["last_save"] < 12:
        return

    async with file_write_lock:
        path = get_file(chat_id, chat_title)
        tmp = path.with_suffix(".tmp")
        try:
            async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                await f.write(json.dumps({
                    "date": date.today().isoformat(),
                    "votes": state["votes"],
                    "poll_message_id": state["poll_message_id"],
                    "results_message_id": state["results_message_id"],
                }, ensure_ascii=False, separators=(",", ":")))
            tmp.replace(path)
            state["last_save"] = now_ts
            state["dirty"] = False
        except Exception as e:
            logger.error(f"Ошибка сохранения {chat_id}: {e}")


async def load_state_from_file(chat_id: int, chat_title: str):
    path = get_file(chat_id, chat_title)
    if not path.exists():
        return None
    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            raw = json.loads(await f.read())
        return raw
    except Exception:
        return None


async def save_last_birthday_date(date_str: str):
    path = DATA_DIR / "last_birthday_sent.json"
    try:
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.write(json.dumps({"date": date_str}))
    except Exception as e:
        logger.error(f"Не удалось сохранить дату ДР: {e}")


async def load_last_birthday_date():
    global last_birthday_sent_date
    path = DATA_DIR / "last_birthday_sent.json"
    if not path.exists():
        return
    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())
            last_birthday_sent_date = data.get("date")
    except Exception as e:
        logger.error(f"Ошибка чтения last_birthday_sent: {e}")


def load_static_data():
    global BIRTHDAYS, DUTIES_TEXT, SCHEDULES
    try:
        with (DATA_DIR / "data_birthdays.json").open("r", encoding="utf-8") as f:
            BIRTHDAYS = json.load(f)
    except Exception as e:
        logger.error(f"birthdays: {e}")

    try:
        with (DATA_DIR / "data_duties.json").open("r", encoding="utf-8") as f:
            DUTIES_TEXT = json.load(f)["text"]
    except Exception as e:
        logger.error(f"duties: {e}")

    try:
        with (DATA_DIR / "data_schedules.json").open("r", encoding="utf-8") as f:
            SCHEDULES = json.load(f)
    except Exception as e:
        logger.error(f"schedules: {e}")


# ================== ИСПРАВЛЕНО ==================
async def check_birthdays(context: ContextTypes.DEFAULT_TYPE):
    global last_birthday_sent_date

    today = date.today()
    today_str = today.strftime("%d.%m")
    today_iso = today.isoformat()

    if last_birthday_sent_date == today_iso:
        return

    birthday_people = [b["name"] for b in BIRTHDAYS if b["date"] == today_str]

    if not birthday_people:
        return

    message = (
        "🎉 <b>С днём рождения!</b>\n\n"
        + "\n".join(f"🎂 {name}" for name in birthday_people) +
        "\n\nОт всего класса — счастья, здоровья, успехов и море позитива! "
    )

    active_chats = list(KNOWN_CHATS)

    for chat_id in active_chats:
        try:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_notification=True
            )

            await context.bot.pin_chat_message(
                chat_id=chat_id,
                message_id=sent_msg.message_id,
                disable_notification=True
            )

        except Exception as e:
            logger.error(f"[ДР] Ошибка в чате {chat_id}: {e}")

    last_birthday_sent_date = today_iso
    await save_last_birthday_date(today_iso)
# ===============================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id not in KNOWN_CHATS:
        KNOWN_CHATS.add(chat_id)
        await save_known_chats()

    await update.message.reply_text("Выбери раздел:")


async def main():
    load_static_data()
    await load_last_birthday_date()
    await load_known_chats()

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(lambda u, c: None))

    midnight_minsk = time(21, 0, tzinfo=timezone.utc)

    app.job_queue.run_daily(
        callback=check_birthdays,
        time=midnight_minsk
    )

    await app.initialize()
    await app.start()

    await app.updater.start_polling(
        drop_pending_updates=True
    )

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
