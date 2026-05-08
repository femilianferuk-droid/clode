"""
Telegram Business Bot
─────────────────────
Функции:
  • Перехват удалённых сообщений (через raw update хук)
  • Автоответчик с HTML-триггерами
  • Статистика чатов
  • Черный список (не отвечать определённым пользователям)
  • Расписание (автоответ только в рабочие часы)
  • Приветственное сообщение для новых чатов
База: SQLite (bot.db)
"""

import asyncio
import logging
import os
import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime, time as dtime
from typing import Any, Callable, Awaitable, Dict, List, Optional

from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Update,
    User,
    Chat,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.i18n import gettext as _

# ══════════════════════════════════════════════════════════
#  КОНФИГ
# ══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_PATH   = "bot.db"

# ══════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════
def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS owners (
                user_id  INTEGER PRIMARY KEY,
                added_at TEXT NOT NULL
            );

            -- кеш всех бизнес-сообщений
            CREATE TABLE IF NOT EXISTS msg_cache (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                bc_id   TEXT    NOT NULL,
                chat_id INTEGER NOT NULL,
                msg_id  INTEGER NOT NULL,
                from_id INTEGER,
                from_name TEXT,
                text    TEXT,
                caption TEXT,
                media_type TEXT,
                file_id    TEXT,
                date       TEXT NOT NULL,
                UNIQUE(bc_id, chat_id, msg_id)
            );

            -- триггеры автоответчика
            CREATE TABLE IF NOT EXISTS triggers (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id  INTEGER NOT NULL,
                keyword   TEXT    NOT NULL COLLATE NOCASE,
                response  TEXT    NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                match_exact INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT  NOT NULL,
                UNIQUE(owner_id, keyword)
            );

            -- чёрный список
            CREATE TABLE IF NOT EXISTS blacklist (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                chat_id  INTEGER NOT NULL,
                note     TEXT,
                added_at TEXT NOT NULL,
                UNIQUE(owner_id, chat_id)
            );

            -- настройки владельца
            CREATE TABLE IF NOT EXISTS settings (
                owner_id        INTEGER PRIMARY KEY,
                schedule_on     INTEGER NOT NULL DEFAULT 0,
                schedule_from   TEXT    NOT NULL DEFAULT '09:00',
                schedule_to     TEXT    NOT NULL DEFAULT '18:00',
                welcome_on      INTEGER NOT NULL DEFAULT 0,
                welcome_text    TEXT    NOT NULL DEFAULT 'Привет! Чем могу помочь?',
                welcome_seen    TEXT    NOT NULL DEFAULT ''
            );

            -- статистика
            CREATE TABLE IF NOT EXISTS stats (
                owner_id      INTEGER NOT NULL,
                chat_id       INTEGER NOT NULL,
                chat_name     TEXT,
                msg_in        INTEGER NOT NULL DEFAULT 0,
                msg_out       INTEGER NOT NULL DEFAULT 0,
                auto_replies  INTEGER NOT NULL DEFAULT 0,
                deleted_count INTEGER NOT NULL DEFAULT 0,
                last_activity TEXT,
                PRIMARY KEY(owner_id, chat_id)
            );
        """)


@contextmanager
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

# ── helpers ───────────────────────────────────────────────
def ensure_owner(user_id: int):
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO owners(user_id,added_at) VALUES(?,?)",
            (user_id, datetime.utcnow().isoformat()),
        )
        con.execute(
            "INSERT OR IGNORE INTO settings(owner_id) VALUES(?)",
            (user_id,),
        )

def get_settings(owner_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM settings WHERE owner_id=?", (owner_id,)
        ).fetchone()

def set_setting(owner_id: int, key: str, value):
    with db() as con:
        con.execute(f"UPDATE settings SET {key}=? WHERE owner_id=?", (value, owner_id))

def stat_inc(owner_id: int, chat_id: int, chat_name: str, field: str):
    now = datetime.utcnow().isoformat()
    with db() as con:
        con.execute(
            """INSERT INTO stats(owner_id,chat_id,chat_name,last_activity)
               VALUES(?,?,?,?)
               ON CONFLICT(owner_id,chat_id) DO UPDATE SET
               chat_name=excluded.chat_name,
               last_activity=excluded.last_activity""",
            (owner_id, chat_id, chat_name, now),
        )
        con.execute(
            f"UPDATE stats SET {field}={field}+1 WHERE owner_id=? AND chat_id=?",
            (owner_id, chat_id),
        )

def cache_msg_raw(bc_id, chat_id, msg_id, from_id, from_name,
                  text, caption, media_type, file_id, ts):
    with db() as con:
        con.execute(
            """INSERT OR REPLACE INTO msg_cache
               (bc_id,chat_id,msg_id,from_id,from_name,text,caption,media_type,file_id,date)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (bc_id, chat_id, msg_id, from_id, from_name,
             text, caption, media_type, file_id, ts),
        )

def get_cached(bc_id, chat_id, msg_ids: list):
    with db() as con:
        ph = ",".join("?" * len(msg_ids))
        return con.execute(
            f"SELECT * FROM msg_cache WHERE bc_id=? AND chat_id=? AND msg_id IN ({ph})",
            [bc_id, chat_id] + msg_ids,
        ).fetchall()

def in_blacklist(owner_id: int, chat_id: int) -> bool:
    with db() as con:
        return con.execute(
            "SELECT 1 FROM blacklist WHERE owner_id=? AND chat_id=?",
            (owner_id, chat_id),
        ).fetchone() is not None

def get_active_triggers(owner_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM triggers WHERE owner_id=? AND is_active=1",
            (owner_id,),
        ).fetchall()

def get_all_triggers(owner_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM triggers WHERE owner_id=? ORDER BY id",
            (owner_id,),
        ).fetchall()

def in_schedule(s) -> bool:
    """Проверяет, входит ли текущее UTC-время в расписание."""
    if not s or not s["schedule_on"]:
        return True
    try:
        now = datetime.utcnow().time()
        t_from = dtime(*map(int, s["schedule_from"].split(":")))
        t_to   = dtime(*map(int, s["schedule_to"].split(":")))
        return t_from <= now <= t_to
    except Exception:
        return True

def mark_welcome_seen(owner_id: int, chat_id: int):
    s = get_settings(owner_id)
    seen = set((s["welcome_seen"] or "").split(",")) if s["welcome_seen"] else set()
    seen.add(str(chat_id))
    set_setting(owner_id, "welcome_seen", ",".join(seen))

def is_welcome_seen(owner_id: int, chat_id: int) -> bool:
    s = get_settings(owner_id)
    seen = set((s["welcome_seen"] or "").split(",")) if s["welcome_seen"] else set()
    return str(chat_id) in seen

# ══════════════════════════════════════════════════════════
#  RAW UPDATE MIDDLEWARE — перехват business_messages_deleted
# ══════════════════════════════════════════════════════════
class RawUpdateMiddleware(BaseMiddleware):
    """
    Получает сырой dict апдейта ДО того, как aiogram попытается
    его распарсить. Обрабатываем business_messages_deleted здесь.
    """
    async def __call__(
        self,
        handler: Callable[[Update, dict], Awaitable[Any]],
        event: Update,
        data: dict,
    ) -> Any:
        bot: Bot = data["bot"]
        
        # Логируем raw update для отладки
        raw_update = data.get("event_update", {})
        if isinstance(raw_update, Update):
            raw_update = raw_update.model_dump(exclude_none=True)
        
        # Проверяем наличие business_messages_deleted
        deleted = raw_update.get("business_messages_deleted") if isinstance(raw_update, dict) else None
        
        if deleted:
            logger.info(f"Detected business_messages_deleted: {deleted}")
            await _process_deleted(deleted, bot)
            # Возвращаем None чтобы предотвратить дальнейшую обработку
            return None

        return await handler(event, data)


async def _process_deleted(deleted: dict, bot: Bot):
    bc_id      = deleted.get("business_connection_id", "")
    chat       = deleted.get("chat", {})
    chat_id    = chat.get("id")
    msg_ids    = deleted.get("message_ids", [])
    chat_title = chat.get("title") or chat.get("first_name") or str(chat_id)

    logger.info(f"Processing deleted messages: bc_id={bc_id}, chat_id={chat_id}, msg_ids={msg_ids}")

    if not bc_id or not chat_id or not msg_ids:
        logger.warning("Missing required fields in deleted message update")
        return

    try:
        bc       = await bot.get_business_connection(bc_id)
        owner_id = bc.user.id
        logger.info(f"Business connection owner: {owner_id}")
    except Exception as e:
        logger.warning("_process_deleted: get_business_connection failed: %s", e)
        return

    cached       = get_cached(bc_id, chat_id, msg_ids)
    found_ids    = {r["msg_id"] for r in cached}
    missing_ids  = set(msg_ids) - found_ids
    
    logger.info(f"Cached messages found: {len(cached)}, missing: {len(missing_ids)}")

    # Обновляем статистику
    stat_inc(owner_id, chat_id, chat_title, "deleted_count")

    header = (
        f"🗑 <b>Удалено {len(msg_ids)} сообщ.</b>\n"
        f"💬 Чат: <b>{chat_title}</b> (<code>{chat_id}</code>)\n"
        f"🕐 {datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC"
    )
    try:
        await bot.send_message(owner_id, header)
    except Exception as e:
        logger.error("Cannot DM owner %d: %s", owner_id, e)
        return

    for row in cached:
        from_name = row["from_name"] or "Неизвестный"
        from_id   = row["from_id"]   or "?"
        lines = [
            f"👤 <b>{from_name}</b> (<code>{from_id}</code>)",
            f"🕐 {row['date']}",
            f"🆔 msg_id: <code>{row['msg_id']}</code>",
        ]
        if row["text"]:
            text_content = row["text"][:1000]  # Обрезаем длинные сообщения
            lines.append(f"\n📝 {text_content}")
        elif row["caption"]:
            caption_content = row["caption"][:1000]
            lines.append(f"\n📝 {caption_content}")
        if row["media_type"]:
            lines.append(f"📎 [{row['media_type']}]")

        body = "\n".join(lines)
        mt   = row["media_type"]
        fid  = row["file_id"]
        try:
            if fid and mt == "photo":
                await bot.send_photo(owner_id, fid, caption=body)
            elif fid and mt == "video":
                await bot.send_video(owner_id, fid, caption=body)
            elif fid and mt == "document":
                await bot.send_document(owner_id, fid, caption=body)
            elif fid and mt == "audio":
                await bot.send_audio(owner_id, fid, caption=body)
            elif fid and mt == "voice":
                await bot.send_voice(owner_id, fid, caption=body)
            elif fid and mt == "sticker":
                await bot.send_sticker(owner_id, fid)
                await bot.send_message(owner_id, body)
            elif fid and mt == "animation":
                await bot.send_animation(owner_id, fid, caption=body)
            else:
                await bot.send_message(owner_id, body)
        except Exception as e:
            logger.error("send deleted msg to owner: %s", e)

    if missing_ids:
        await bot.send_message(
            owner_id,
            f"⚠️ Не закешировано (бот не видел): "
            f"<code>{', '.join(map(str, missing_ids))}</code>"
        )

# ══════════════════════════════════════════════════════════
#  FSM
# ══════════════════════════════════════════════════════════
class AddTrigger(StatesGroup):
    keyword  = State()
    response = State()

class EditWelcome(StatesGroup):
    text = State()

class EditSchedule(StatesGroup):
    times = State()

class AddBlacklist(StatesGroup):
    chat_id = State()

# ══════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════
def kb(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=c) for t, c in row]
        for row in rows
    ])

def main_kb():
    return kb(
        [("📋 Триггеры",    "triggers_list"), ("➕ Добавить триггер", "trigger_add")],
        [("🚫 Чёрный список","bl_list"),       ("📊 Статистика",       "stats_view")],
        [("⚙️ Настройки",   "settings_menu"),  ("ℹ️ Помощь",           "help")],
    )

def back_kb(to="main_menu"):
    return kb([(("« Назад"), to)])

def triggers_kb(trigs):
    rows = []
    for t in trigs:
        icon = "✅" if t["is_active"] else "❌"
        rows.append([(f"{icon} {t['keyword'][:22]}", f"t_view_{t['id']}")])
    rows.append([("« Назад", "main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def trigger_view_kb(tid: int, active: bool):
    toggle = "❌ Выкл" if active else "✅ Вкл"
    return kb(
        [(toggle, f"t_toggle_{tid}"), ("🗑 Удалить", f"t_del_{tid}")],
        [("« К списку", "triggers_list")],
    )

def settings_kb(s):
    sched = "✅ Расписание" if s["schedule_on"] else "❌ Расписание"
    welc  = "✅ Приветствие" if s["welcome_on"] else "❌ Приветствие"
    return kb(
        [(sched, "sched_toggle"), ("🕐 Часы работы", "sched_edit")],
        [(welc,  "welc_toggle"),  ("✏️ Текст привет.", "welc_edit")],
        [("« Назад", "main_menu")],
    )

# ══════════════════════════════════════════════════════════
#  ROUTER
# ══════════════════════════════════════════════════════════
router = Router()

# ── /start ────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(msg: Message):
    ensure_owner(msg.from_user.id)
    await msg.answer(
        "<b>👋 Добро пожаловать!</b>\n\n"
        "<b>Возможности бота:</b>\n"
        "🗑 Перехват удалённых сообщений из бизнес-чатов\n"
        "🤖 Автоответчик с HTML-триггерами\n"
        "👋 Приветствие новых диалогов\n"
        "🕐 Расписание автоответов\n"
        "🚫 Чёрный список чатов\n"
        "📊 Статистика по чатам\n\n"
        "<i>Подключи бизнес-аккаунт:\n"
        "Настройки → Telegram Business → Чат-боты</i>"
    )

@router.message(Command("menu"))
async def cmd_menu(msg: Message):
    ensure_owner(msg.from_user.id)
    await msg.answer("🏠 <b>Главное меню</b>", reply_markup=main_kb())

# ── main_menu callback ─────────────────────────────────────
@router.callback_query(F.data == "main_menu")
async def cb_main(call: CallbackQuery):
    await call.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=main_kb())

# ── help ──────────────────────────────────────────────────
@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    await call.message.edit_text(
        "<b>ℹ️ Справка</b>\n\n"
        "<b>Удалённые сообщения</b>\n"
        "Бот кеширует все входящие бизнес-сообщения. При удалении — отправляет их тебе.\n\n"
        "<b>Автоответчик</b>\n"
        "Добавляй триггеры: ключевое слово + HTML-ответ.\n"
        "Поддерживаемые теги:\n"
        "<code>&lt;b&gt;</code> <code>&lt;i&gt;</code> <code>&lt;u&gt;</code> "
        "<code>&lt;s&gt;</code> <code>&lt;code&gt;</code> <code>&lt;a href=''&gt;</code>\n\n"
        "<b>Расписание</b>\n"
        "Автоответы только в заданные часы (UTC).\n\n"
        "<b>Приветствие</b>\n"
        "Отправляется один раз при первом сообщении в новом чате.\n\n"
        "<b>Чёрный список</b>\n"
        "Чаты из ЧС — без автоответов.",
        reply_markup=back_kb(),
    )

# ══════════════════════════════════════════════════════════
#  ТРИГГЕРЫ
# ══════════════════════════════════════════════════════════
@router.callback_query(F.data == "triggers_list")
async def cb_trig_list(call: CallbackQuery):
    trigs = get_all_triggers(call.from_user.id)
    if not trigs:
        await call.message.edit_text(
            "📋 <b>Триггеры</b>\n\nСписок пуст.",
            reply_markup=kb([("➕ Добавить", "trigger_add")], [("« Назад", "main_menu")]),
        )
        return
    await call.message.edit_text(
        f"📋 <b>Триггеры</b> ({len(trigs)} шт.)",
        reply_markup=triggers_kb(trigs),
    )

@router.callback_query(F.data.startswith("t_view_"))
async def cb_trig_view(call: CallbackQuery):
    tid = int(call.data.split("_")[2])
    with db() as con:
        t = con.execute("SELECT * FROM triggers WHERE id=? AND owner_id=?",
                        (tid, call.from_user.id)).fetchone()
    if not t:
        await call.answer("Не найден", show_alert=True); return
    mode = "точное совпадение" if t["match_exact"] else "подстрока"
    status = "✅ Активен" if t["is_active"] else "❌ Отключён"
    await call.message.edit_text(
        f"<b>Триггер #{t['id']}</b>\n\n"
        f"<b>Слово:</b> <code>{t['keyword']}</code>\n"
        f"<b>Режим:</b> {mode}\n"
        f"<b>Статус:</b> {status}\n\n"
        f"<b>Ответ (HTML):</b>\n{t['response']}",
        reply_markup=trigger_view_kb(tid, bool(t["is_active"])),
    )

@router.callback_query(F.data.startswith("t_toggle_"))
async def cb_trig_toggle(call: CallbackQuery):
    tid = int(call.data.split("_")[2])
    with db() as con:
        row = con.execute("SELECT is_active FROM triggers WHERE id=? AND owner_id=?",
                          (tid, call.from_user.id)).fetchone()
        if not row: await call.answer("Не найден", show_alert=True); return
        new = 0 if row["is_active"] else 1
        con.execute("UPDATE triggers SET is_active=? WHERE id=?", (new, tid))
    await call.answer("✅ Включён" if new else "❌ Отключён")
    with db() as con:
        t = con.execute("SELECT * FROM triggers WHERE id=?", (tid,)).fetchone()
    mode = "точное совпадение" if t["match_exact"] else "подстрока"
    status = "✅ Активен" if t["is_active"] else "❌ Отключён"
    await call.message.edit_text(
        f"<b>Триггер #{t['id']}</b>\n\n"
        f"<b>Слово:</b> <code>{t['keyword']}</code>\n"
        f"<b>Режим:</b> {mode}\n"
        f"<b>Статус:</b> {status}\n\n"
        f"<b>Ответ (HTML):</b>\n{t['response']}",
        reply_markup=trigger_view_kb(tid, bool(t["is_active"])),
    )

@router.callback_query(F.data.startswith("t_del_"))
async def cb_trig_del(call: CallbackQuery):
    tid = int(call.data.split("_")[2])
    with db() as con:
        con.execute("DELETE FROM triggers WHERE id=? AND owner_id=?",
                    (tid, call.from_user.id))
    await call.answer("🗑 Удалён")
    await cb_trig_list(call)

# FSM — добавить триггер
@router.callback_query(F.data == "trigger_add")
async def cb_trig_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddTrigger.keyword)
    await call.message.edit_text(
        "➕ <b>Новый триггер — шаг 1/2</b>\n\n"
        "Введи <b>ключевое слово</b> или фразу.\n\n"
        "<i>Примеры: цена, доставка, график работы</i>\n\n"
        "/cancel — отмена"
    )

@router.message(AddTrigger.keyword)
async def fsm_trig_kw(msg: Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=main_kb()); return
    kw = (msg.text or "").strip()
    if not kw:
        await msg.answer("Пустое слово, попробуй ещё:"); return
    await state.update_data(keyword=kw)
    await state.set_state(AddTrigger.response)
    await msg.answer(
        f"➕ <b>Шаг 2/2</b>\n\n"
        f"Ключевое слово: <code>{kw}</code>\n\n"
        f"Теперь введи <b>текст ответа</b> с HTML-разметкой.\n\n"
        f"<i>Пример:</i>\n"
        f"<code>&lt;b&gt;Доставка&lt;/b&gt; — 1-3 дня.\n"
        f"Подробнее: &lt;a href='https://example.com'&gt;сайт&lt;/a&gt;</code>\n\n"
        f"/cancel — отмена"
    )

@router.message(AddTrigger.response)
async def fsm_trig_resp(msg: Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=main_kb()); return
    resp = msg.text or msg.caption or ""
    if not resp.strip():
        await msg.answer("Пустой ответ, попробуй ещё:"); return
    data = await state.get_data()
    kw = data["keyword"]
    try:
        with db() as con:
            con.execute(
                "INSERT INTO triggers(owner_id,keyword,response,created_at) VALUES(?,?,?,?)",
                (msg.from_user.id, kw, resp, datetime.utcnow().isoformat()),
            )
        await state.clear()
        await msg.answer(
            f"✅ <b>Триггер добавлен!</b>\n\n"
            f"<b>Слово:</b> <code>{kw}</code>\n\n"
            f"<b>Ответ (предпросмотр):</b>\n{resp}",
            reply_markup=main_kb(),
        )
    except sqlite3.IntegrityError:
        await state.clear()
        await msg.answer(
            f"⚠️ Триггер <code>{kw}</code> уже существует.",
            reply_markup=main_kb(),
        )

# ══════════════════════════════════════════════════════════
#  ЧЁРНЫЙ СПИСОК
# ══════════════════════════════════════════════════════════
@router.callback_query(F.data == "bl_list")
async def cb_bl_list(call: CallbackQuery):
    with db() as con:
        rows = con.execute(
            "SELECT * FROM blacklist WHERE owner_id=? ORDER BY id",
            (call.from_user.id,),
        ).fetchall()
    if not rows:
        await call.message.edit_text(
            "🚫 <b>Чёрный список</b>\n\nПуст.",
            reply_markup=kb([("➕ Добавить чат", "bl_add")], [("« Назад", "main_menu")]),
        )
        return
    lines = ["🚫 <b>Чёрный список</b>\n"]
    brows = []
    for r in rows:
        note = f" — {r['note']}" if r["note"] else ""
        lines.append(f"• <code>{r['chat_id']}</code>{note}")
        brows.append([(f"❌ {r['chat_id']}", f"bl_del_{r['id']}")])
    brows.append([("➕ Добавить", "bl_add"), ("« Назад", "main_menu")])
    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=brows),
    )

@router.callback_query(F.data == "bl_add")
async def cb_bl_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddBlacklist.chat_id)
    await call.message.edit_text(
        "🚫 <b>Добавить в чёрный список</b>\n\n"
        "Введи <b>chat_id</b> пользователя/чата.\n"
        "(Можно узнать из статистики или логов)\n\n"
        "/cancel — отмена"
    )

@router.message(AddBlacklist.chat_id)
async def fsm_bl_cid(msg: Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear(); await msg.answer("Отменено.", reply_markup=main_kb()); return
    try:
        cid = int(msg.text.strip())
    except ValueError:
        await msg.answer("Нужно число. Попробуй ещё:"); return
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO blacklist(owner_id,chat_id,added_at) VALUES(?,?,?)",
            (msg.from_user.id, cid, datetime.utcnow().isoformat()),
        )
    await state.clear()
    await msg.answer(
        f"✅ <code>{cid}</code> добавлен в чёрный список.",
        reply_markup=main_kb(),
    )

@router.callback_query(F.data.startswith("bl_del_"))
async def cb_bl_del(call: CallbackQuery):
    rid = int(call.data.split("_")[2])
    with db() as con:
        con.execute("DELETE FROM blacklist WHERE id=? AND owner_id=?",
                    (rid, call.from_user.id))
    await call.answer("Удалён из ЧС")
    await cb_bl_list(call)

# ══════════════════════════════════════════════════════════
#  СТАТИСТИКА
# ══════════════════════════════════════════════════════════
@router.callback_query(F.data == "stats_view")
async def cb_stats(call: CallbackQuery):
    with db() as con:
        rows = con.execute(
            "SELECT * FROM stats WHERE owner_id=? ORDER BY last_activity DESC LIMIT 20",
            (call.from_user.id,),
        ).fetchall()
    if not rows:
        await call.message.edit_text(
            "📊 <b>Статистика</b>\n\nПока нет данных.",
            reply_markup=back_kb(),
        ); return
    lines = ["📊 <b>Статистика чатов</b>\n"]
    for r in rows:
        name = r["chat_name"] or str(r["chat_id"])
        lines.append(
            f"💬 <b>{name}</b>\n"
            f"   📥 вх: {r['msg_in']}  📤 исх: {r['msg_out']}\n"
            f"   🤖 автоответов: {r['auto_replies']}  "
            f"🗑 удалено: {r['deleted_count']}"
        )
    await call.message.edit_text(
        "\n\n".join(lines), reply_markup=back_kb(),
    )

# ══════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════
@router.callback_query(F.data == "settings_menu")
async def cb_settings(call: CallbackQuery):
    s = get_settings(call.from_user.id)
    await call.message.edit_text(
        f"⚙️ <b>Настройки</b>\n\n"
        f"Расписание: {'вкл ✅' if s['schedule_on'] else 'выкл ❌'} "
        f"({s['schedule_from']}–{s['schedule_to']} UTC)\n"
        f"Приветствие: {'вкл ✅' if s['welcome_on'] else 'выкл ❌'}",
        reply_markup=settings_kb(s),
    )

@router.callback_query(F.data == "sched_toggle")
async def cb_sched_toggle(call: CallbackQuery):
    s = get_settings(call.from_user.id)
    new = 0 if s["schedule_on"] else 1
    set_setting(call.from_user.id, "schedule_on", new)
    await call.answer("Расписание " + ("включено ✅" if new else "выключено ❌"))
    await cb_settings(call)

@router.callback_query(F.data == "sched_edit")
async def cb_sched_edit(call: CallbackQuery, state: FSMContext):
    await state.set_state(EditSchedule.times)
    s = get_settings(call.from_user.id)
    await call.message.edit_text(
        f"🕐 <b>Часы работы</b> (UTC)\n\n"
        f"Текущие: <code>{s['schedule_from']}–{s['schedule_to']}</code>\n\n"
        f"Введи новые в формате <code>ЧЧ:ММ-ЧЧ:ММ</code>\n"
        f"Пример: <code>09:00-18:00</code>\n\n"
        f"/cancel — отмена"
    )

@router.message(EditSchedule.times)
async def fsm_sched(msg: Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear(); await msg.answer("Отменено.", reply_markup=main_kb()); return
    try:
        fr, to = msg.text.strip().split("-")
        dtime(*map(int, fr.split(":"))); dtime(*map(int, to.split(":")))
    except Exception:
        await msg.answer("Неверный формат. Пример: <code>09:00-18:00</code>"); return
    set_setting(msg.from_user.id, "schedule_from", fr.strip())
    set_setting(msg.from_user.id, "schedule_to", to.strip())
    await state.clear()
    await msg.answer(
        f"✅ Расписание: <code>{fr.strip()}–{to.strip()}</code> UTC",
        reply_markup=main_kb(),
    )

@router.callback_query(F.data == "welc_toggle")
async def cb_welc_toggle(call: CallbackQuery):
    s = get_settings(call.from_user.id)
    new = 0 if s["welcome_on"] else 1
    set_setting(call.from_user.id, "welcome_on", new)
    await call.answer("Приветствие " + ("включено ✅" if new else "выключено ❌"))
    await cb_settings(call)

@router.callback_query(F.data == "welc_edit")
async def cb_welc_edit(call: CallbackQuery, state: FSMContext):
    await state.set_state(EditWelcome.text)
    s = get_settings(call.from_user.id)
    await call.message.edit_text(
        f"👋 <b>Текст приветствия</b>\n\n"
        f"Текущий:\n{s['welcome_text']}\n\n"
        f"Введи новый текст (HTML поддерживается).\n\n"
        f"/cancel — отмена"
    )

@router.message(EditWelcome.text)
async def fsm_welc(msg: Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear(); await msg.answer("Отменено.", reply_markup=main_kb()); return
    text = msg.text or msg.caption or ""
    if not text.strip():
        await msg.answer("Пустой текст, попробуй ещё:"); return
    set_setting(msg.from_user.id, "welcome_text", text)
    await state.clear()
    await msg.answer(
        f"✅ <b>Приветствие обновлено!</b>\n\n{text}",
        reply_markup=main_kb(),
    )

# ══════════════════════════════════════════════════════════
#  БИЗНЕС: входящие сообщения
# ══════════════════════════════════════════════════════════
def _extract_media(msg: Message) -> tuple:
    if msg.photo:      return "photo",     msg.photo[-1].file_id
    if msg.video:      return "video",     msg.video.file_id
    if msg.document:   return "document",  msg.document.file_id
    if msg.audio:      return "audio",     msg.audio.file_id
    if msg.voice:      return "voice",     msg.voice.file_id
    if msg.sticker:    return "sticker",   msg.sticker.file_id
    if msg.animation:  return "animation", msg.animation.file_id
    return None, None

@router.business_message()
async def on_biz_msg(msg: Message, bot: Bot):
    bc_id = msg.business_connection_id
    if not bc_id:
        return

    # кешируем сообщение
    mt, fid = _extract_media(msg)
    ts = datetime.utcfromtimestamp(msg.date.timestamp()).isoformat()
    fn = msg.from_user.full_name if msg.from_user else "Неизвестный"
    fi = msg.from_user.id if msg.from_user else None
    cache_msg_raw(bc_id, msg.chat.id, msg.message_id, fi, fn,
                  msg.text, msg.caption, mt, fid, ts)

    logger.info(f"Cached message {msg.message_id} from {fn} in chat {msg.chat.id}")

    try:
        bc = await bot.get_business_connection(bc_id)
        owner_id = bc.user.id
    except Exception as e:
        logger.warning("get_business_connection: %s", e)
        return

    chat_name = msg.chat.title or msg.chat.full_name or str(msg.chat.id)
    is_outgoing = msg.from_user and msg.from_user.id == owner_id

    if is_outgoing:
        stat_inc(owner_id, msg.chat.id, chat_name, "msg_out")
        return

    stat_inc(owner_id, msg.chat.id, chat_name, "msg_in")

    # Проверки перед автоответом
    if in_blacklist(owner_id, msg.chat.id):
        logger.info(f"Chat {msg.chat.id} is in blacklist, skipping")
        return

    s = get_settings(owner_id)
    if not s:
        ensure_owner(owner_id)
        s = get_settings(owner_id)

    if not in_schedule(s):
        logger.info(f"Outside schedule hours, skipping auto-reply")
        return

    # Приветствие
    if s["welcome_on"] and not is_welcome_seen(owner_id, msg.chat.id):
        mark_welcome_seen(owner_id, msg.chat.id)
        try:
            await bot.send_message(
                chat_id=msg.chat.id,
                text=s["welcome_text"],
                parse_mode=ParseMode.HTML,
                business_connection_id=bc_id,
            )
            stat_inc(owner_id, msg.chat.id, chat_name, "auto_replies")
            logger.info(f"Welcome message sent to chat {chat_name}")
        except Exception as e:
            logger.error("welcome send: %s", e)
        return  # приветствие вместо триггеров

    # Триггеры
    text = (msg.text or msg.caption or "").lower()
    if not text:
        return

    for t in get_active_triggers(owner_id):
        kw = t["keyword"].lower()
        matched = (text == kw) if t["match_exact"] else (kw in text)
        if matched:
            try:
                await bot.send_message(
                    chat_id=msg.chat.id,
                    text=t["response"],
                    parse_mode=ParseMode.HTML,
                    business_connection_id=bc_id,
                )
                stat_inc(owner_id, msg.chat.id, chat_name, "auto_replies")
                logger.info("trigger #%d fired → chat %d", t["id"], msg.chat.id)
            except Exception as e:
                logger.error("trigger send: %s", e)
            break

@router.edited_business_message()
async def on_biz_edit(msg: Message):
    bc_id = msg.business_connection_id
    if not bc_id:
        return
    mt, fid = _extract_media(msg)
    ts = datetime.utcfromtimestamp(msg.date.timestamp()).isoformat()
    fn = msg.from_user.full_name if msg.from_user else "Неизвестный"
    fi = msg.from_user.id if msg.from_user else None
    cache_msg_raw(bc_id, msg.chat.id, msg.message_id, fi, fn,
                  msg.text, msg.caption, mt, fid, ts)

# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
async def main():
    init_db()
    
    # Не используем DefaultBotProperties с parse_mode=HTML
    bot = Bot(token=BOT_TOKEN)
    
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware для перехвата business_messages_deleted
    dp.update.outer_middleware(RawUpdateMiddleware())

    dp.include_router(router)

    # Важно: получаем raw updates
    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "edited_message",
            "callback_query",
            "business_message",
            "edited_business_message",
            "business_connection",
            # Самое важное:
            "deleted_business_messages",  # это правильное название обновления!
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
