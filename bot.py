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
# Нет необходимости в mkdir, так как директория скрипта уже существует

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

file_write_lock = asyncio.Lock()

last_birthday_sent_date = None
last_pinned_birthday_msg_id = {}


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
        logger.info(f"Сохранена дата последнего поздравления: {date_str}")
    except Exception as e:
        logger.error(f"Не удалось сохранить дату ДР: {e}")


async def load_last_birthday_date():
    global last_birthday_sent_date
    path = DATA_DIR / "last_birthday_sent.json"
    if not path.exists():
        logger.info("Файл last_birthday_sent.json не найден → первая проверка ДР")
        return
    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())
            last_birthday_sent_date = data.get("date")
        logger.info(f"Загружена дата последнего поздравления: {last_birthday_sent_date}")
    except Exception as e:
        logger.error(f"Ошибка чтения last_birthday_sent: {e}")


def load_static_data():
    global BIRTHDAYS, DUTIES_TEXT, SCHEDULES
    try:
        with (DATA_DIR / "data_birthdays.json").open("r", encoding="utf-8") as f:
            BIRTHDAYS = json.load(f)
        logger.info(f"Загружено {len(BIRTHDAYS)} дней рождения")
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


# ────────────────────────────────────────────── МЕНЮ ──────────────────────────────────────────────

MAIN_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("📅 Расписание",     callback_data="menu_schedule")],
    [InlineKeyboardButton("🍽 Столовая",      callback_data="menu_stolovaya")],
    [InlineKeyboardButton("🧹 Дежурства",     callback_data="duties")],
    [InlineKeyboardButton("🎂 Дни рождения",   callback_data="menu_birthdays")],
])

PROFILE_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("📐 Математика (профиль)", callback_data="profile_math")],
    [InlineKeyboardButton("🧪 Химия (профиль)",     callback_data="profile_chem")],
    [InlineKeyboardButton("📘 База",                callback_data="profile_base")],
    [InlineKeyboardButton("↩️ Назад",               callback_data="back_main")],
])


def days_menu(profile):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Пн", callback_data=f"day*{profile}*pn"),
            InlineKeyboardButton("Вт", callback_data=f"day*{profile}*vt"),
            InlineKeyboardButton("Ср", callback_data=f"day*{profile}*sr"),
        ],
        [
            InlineKeyboardButton("Чт", callback_data=f"day*{profile}*cht"),
            InlineKeyboardButton("Пт", callback_data=f"day*{profile}*pt"),
        ],
        [InlineKeyboardButton("↩️ Назад", callback_data="menu_schedule")],
    ])


STOL_MAIN_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("📊 Создать опрос", callback_data="stol_create_poll")],
    [InlineKeyboardButton("📈 Показать итоги", callback_data="stol_show_results")],
    [InlineKeyboardButton("↩️ Назад", callback_data="back_main")],
])

STOL_POLL_MARKUP = InlineKeyboardMarkup([
    [InlineKeyboardButton("🍽 Буду есть", callback_data="stol_eat")],
    [InlineKeyboardButton("🙅 Не буду есть", callback_data="stol_no_eat")],
    [InlineKeyboardButton("🏫 Не буду в школе", callback_data="stol_absent")],
])

DUTIES_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("↩️ Назад", callback_data="back_main")],
])

BIRTHDAYS_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("↩️ Назад", callback_data="back_main")],
])


async def safe_edit(query, text, reply_markup=None, parse_mode=None):
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.warning(f"Ошибка редактирования: {e}")


async def fast_edit(bot, chat_id, message_id, text):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
        return True
    except RetryAfter as ra:
        logger.warning(f"Флуд-контроль: жду {ra.retry_after} сек")
        await asyncio.sleep(ra.retry_after)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            return True
        except Exception:
            return False
    except BadRequest as br:
        if "not modified" in str(br).lower():
            return True
        logger.warning(f"BadRequest: {br}")
        return False
    except Exception as e:
        logger.error(f"Ошибка редактирования: {e}")
        return False


def get_results_text(votes: dict) -> str:
    eat = []
    no_eat = []
    absent = []

    for user_data in votes.values():
        name = user_data["name"]
        username = user_data.get("username")
        if username:
            disp = f"@{username}"
        else:
            disp = name

        if user_data["status"] == "eat":
            eat.append(disp)
        elif user_data["status"] == "no_eat":
            no_eat.append(disp)
        elif user_data["status"] == "absent":
            absent.append(disp)

    text = "📊 Итоги опроса\n\n"
    text += f"🍽 Будут есть ({len(eat)}): {', '.join(eat) or '-'}\n\n"
    text += f"🙅 Не будут есть ({len(no_eat)}): {', '.join(no_eat) or '-'}\n\n"
    text += f"🏫 Не будут в школе ({len(absent)}): {', '.join(absent) or '-'}\n"

    return text


async def check_birthdays(context: ContextTypes.DEFAULT_TYPE):
    global last_birthday_sent_date

    minsk_tz = timezone(timedelta(hours=3))
    today = datetime.now(minsk_tz).date()
    today_iso = today.isoformat()
    today_md = f"{today.day:02d}.{today.month:02d}"

    if last_birthday_sent_date == today_iso:
        logger.info("[ДР] Уже поздравляли сегодня, пропуск")
        return

    birthdays_today = [p["name"] for p in BIRTHDAYS if p["date"] == today_md]
    if not birthdays_today:
        logger.info("[ДР] Сегодня нет ДР")
        return

    names = " и ".join(birthdays_today) if len(birthdays_today) > 1 else birthdays_today[0]
    text = f"🎉 Поздравляем с Днём Рождения: {names}! 🥳\nЖелаем счастья, здоровья и успехов! 🎂"

    logger.info(f"[ДР] Поздравляем: {names}")

    for chat_id in list(chat_states.keys()):
        try:
            sent_msg = await context.bot.send_message(chat_id=chat_id, text=text)

            prev_msg_id = last_pinned_birthday_msg_id.get(chat_id)
            if prev_msg_id:
                try:
                    await context.bot.unpin_chat_message(chat_id=chat_id, message_id=prev_msg_id)
                except Exception:
                    pass

            try:
                await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent_msg.message_id, disable_notification=True)
            except Exception:
                logger.warning(f"[ДР] Не удалось закрепить в {chat_id}")

            last_pinned_birthday_msg_id[chat_id] = sent_msg.message_id

        except Exception as e:
            logger.error(f"[ДР] Ошибка в чате {chat_id}: {e}")

    last_birthday_sent_date = today_iso
    await save_last_birthday_date(today_iso)
    logger.info("[ДР] Поздравление завершено успешно")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери раздел:", reply_markup=MAIN_MENU)


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message:
        return

    await q.answer()

    data = q.data
    chat_id = q.message.chat.id
    chat_title = q.message.chat.title or f"chat_{chat_id}"
    user = q.from_user
    uid = str(user.id)

    state = chat_states[chat_id]

    if not state["votes"]:
        loaded = await load_state_from_file(chat_id, chat_title)
        if loaded:
            state.update(loaded)
            state["last_save"] = datetime.utcnow().timestamp() - 25
            state["dirty"] = False

    if data == "menu_birthdays":
        text = "🎂 <b>Дни рождения класса</b>\n\n"
        months = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
                  7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
        emojis = {1:"❄️",2:"💕",3:"🌸",4:"🐰",5:"🌷",6:"☀️",7:"🏖️",8:"🌻",9:"🍁",10:"🎃",11:"🍂",12:"🎄"}

        by_month = {i:[] for i in range(1,13)}
        for p in BIRTHDAYS:
            d, m = map(int, p["date"].split("."))
            by_month[m].append((d, p["name"]))

        for m in range(1,13):
            if by_month[m]:
                text += f"<b>{emojis.get(m,'⭐')} {months[m]}</b>\n"
                for d, name in sorted(by_month[m]):
                    text += f"  • {d:02d} → {name}\n"
                text += "\n"

        await safe_edit(q, text, BIRTHDAYS_MENU, ParseMode.HTML)
        return

    if data == "menu_schedule":
        await safe_edit(q, "Выбери профиль:", PROFILE_MENU)
        return

    if data.startswith("profile_"):
        prof = data.split("_")[1]
        await safe_edit(q, SCHEDULES[prof]["title"], days_menu(prof), ParseMode.MARKDOWN)
        return

    if data.startswith("day*"):
        _, prof, day = data.split("*")
        await safe_edit(q, SCHEDULES[prof][day], days_menu(prof), ParseMode.MARKDOWN)
        return

    if data in ("back_main", "back_main_from_profile"):
        await safe_edit(q, "Выбери раздел:", MAIN_MENU)
        return

    if data == "duties":
        await safe_edit(q, DUTIES_TEXT, DUTIES_MENU)
        return

    if data == "menu_stolovaya":
        await safe_edit(q, "Выбери действие:", STOL_MAIN_MENU)
        return

    if data == "stol_create_poll":
        try:
            await q.message.delete()
        except Exception:
            pass

        state["votes"].clear()
        state["dirty"] = True

        poll_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="🍽 Опрос на завтра",
            reply_markup=STOL_POLL_MARKUP
        )
        state["poll_message_id"] = poll_msg.message_id

        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=poll_msg.message_id, disable_notification=True)
        except Exception:
            pass

        res_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=get_results_text(state["votes"])
        )
        state["results_message_id"] = res_msg.message_id

        await save_state_periodically(chat_id, chat_title)
        return

    if data in ("stol_eat", "stol_no_eat", "stol_absent"):
        status_map = {"stol_eat": "eat", "stol_no_eat": "no_eat", "stol_absent": "absent"}
        new_status = status_map[data]

        state["votes"][uid] = {
            "name": user.first_name or "Без имени",
            "username": user.username or None,
            "status": new_status
        }
        state["dirty"] = True

        if state.get("results_message_id"):
            new_text = get_results_text(state["votes"])
            success = await fast_edit(context.bot, chat_id, state["results_message_id"], new_text)
            if success:
                await q.answer("Голос изменён ✓")
            else:
                await q.answer("Результаты скоро обновятся", show_alert=True)
        else:
            await q.answer("Голос принят")

        await save_state_periodically(chat_id, chat_title)
        return

    if data == "stol_show_results":
        if state["votes"]:
            await context.bot.send_message(chat_id=chat_id, text=get_results_text(state["votes"]))
        else:
            await context.bot.send_message(chat_id=chat_id, text="Пока никто не проголосовал.")
        return


async def main():
    load_static_data()
    await load_last_birthday_date()

    logger.info("Сканирую сохранённые чаты по именам файлов...")
    for file_path in DATA_DIR.glob("stolovaya_*.json"):
        try:
            filename = file_path.name
            if not filename.startswith("stolovaya_") or not filename.endswith(".json"):
                continue

            chat_id_str = filename.rsplit("_", 1)[-1].removesuffix(".json")
            chat_id = int(chat_id_str)

            chat_states[chat_id]
            logger.info(f"Обнаружен и добавлен чат {chat_id} из файла {filename}")

        except ValueError as ve:
            logger.warning(f"Невалидный chat_id в имени файла {filename}: {ve}")
        except Exception as e:
            logger.error(f"Ошибка обработки имени файла {filename}: {e}")

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

    app.job_queue.run_once(
        callback=check_birthdays,
        when=5
    )

    # ИСПРАВЛЕННЫЙ БЛОК — только это изменилось
    app.job_queue.run_daily(
        callback=check_birthdays,
        time=time(21, 0, 0)  # 21:00 UTC = 00:00 Минск
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
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
    except Exception as e:
        logger.critical(f"Критическая ошибка запуска: {e}", exc_info=True)
