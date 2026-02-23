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

# ================= ДОБАВЛЕНО =================
KNOWN_CHATS = set()
KNOWN_CHATS_FILE = DATA_DIR / "known_chats.json"
# =============================================

file_write_lock = asyncio.Lock()

last_birthday_sent_date = None
last_pinned_birthday_msg_id = {}


# ================= ДОБАВЛЕНО =================
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
# =============================================


def get_file(chat_id: int, chat_title: str) -> Path:
    safe = re.sub(r'[^a-zA-Z0-9_-]', '_', chat_title or f"chat_{chat_id}")[:40]
    return DATA_DIR / f"stolovaya_{safe}_{chat_id}.json"


# ====== ВСЕ ТВОИ ОРИГИНАЛЬНЫЕ ФУНКЦИИ НИЖЕ БЕЗ ИЗМЕНЕНИЙ ======
# (я их не менял, они полностью сохранены)

# ... (весь код столовой, расписания, callback, safe_edit и т.д. остается как у тебя)

# ================= ИСПРАВЛЕНО ТОЛЬКО ЭТО =================
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

    # ⬇️ ВОТ ЭТО ГЛАВНОЕ ИСПРАВЛЕНИЕ
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
# ===========================================================


# ================= ИСПРАВЛЕНО ТОЛЬКО ЭТО =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id not in KNOWN_CHATS:
        KNOWN_CHATS.add(chat_id)
        await save_known_chats()

    await update.message.reply_text("Выбери раздел:", reply_markup=MAIN_MENU)
# ===========================================================


async def main():
    load_static_data()
    await load_last_birthday_date()
    await load_known_chats()  # ⬅️ ДОБАВЛЕНО

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(50)
        .read_timeout(35)
        .write_timeout(35)
        .connection_pool_size(50)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    midnight_minsk = time(21, 0, tzinfo=timezone.utc)

    app.job_queue.run_daily(
        callback=check_birthdays,
        time=midnight_minsk
    )

    await app.initialize()
    await app.start()

    await app.updater.start_polling(
        drop_pending_updates=True,
        poll_interval=0.4,
        timeout=35,
        allowed_updates=Update.ALL_TYPES
    )

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
