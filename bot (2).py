import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Callable, Awaitable

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
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_PATH = "bot.db"

# ─── Database ─────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS owners (
                user_id   INTEGER PRIMARY KEY,
                added_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_cache (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                business_connection_id TEXT NOT NULL,
                chat_id                INTEGER NOT NULL,
                message_id             INTEGER NOT NULL,
                from_id                INTEGER,
                from_name              TEXT,
                text                   TEXT,
                caption                TEXT,
                media_type             TEXT,
                file_id                TEXT,
                date                   TEXT NOT NULL,
                UNIQUE(business_connection_id, chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS triggers (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id    INTEGER NOT NULL,
                keyword     TEXT NOT NULL COLLATE NOCASE,
                response    TEXT NOT NULL,
                is_active   INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT NOT NULL,
                UNIQUE(owner_id, keyword)
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


def is_owner(user_id: int) -> bool:
    with db() as con:
        row = con.execute("SELECT 1 FROM owners WHERE user_id=?", (user_id,)).fetchone()
        return row is not None


def add_owner(user_id: int):
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO owners(user_id, added_at) VALUES(?,?)",
            (user_id, datetime.utcnow().isoformat()),
        )


def cache_message(msg: Message, business_connection_id: str):
    text = msg.text or None
    caption = msg.caption or None
    from_id = msg.from_user.id if msg.from_user else None
    from_name = (msg.from_user.full_name or "") if msg.from_user else ""
    media_type = None
    file_id = None
    if msg.photo:
        media_type = "photo"
        file_id = msg.photo[-1].file_id
    elif msg.video:
        media_type = "video"
        file_id = msg.video.file_id
    elif msg.document:
        media_type = "document"
        file_id = msg.document.file_id
    elif msg.audio:
        media_type = "audio"
        file_id = msg.audio.file_id
    elif msg.voice:
        media_type = "voice"
        file_id = msg.voice.file_id
    elif msg.sticker:
        media_type = "sticker"
        file_id = msg.sticker.file_id
    elif msg.animation:
        media_type = "animation"
        file_id = msg.animation.file_id

    with db() as con:
        con.execute(
            """INSERT OR REPLACE INTO message_cache
               (business_connection_id, chat_id, message_id, from_id, from_name,
                text, caption, media_type, file_id, date)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                business_connection_id,
                msg.chat.id,
                msg.message_id,
                from_id,
                from_name,
                text,
                caption,
                media_type,
                file_id,
                datetime.utcfromtimestamp(msg.date.timestamp()).isoformat(),
            ),
        )


def get_cached_messages(business_connection_id: str, chat_id: int, message_ids: list):
    with db() as con:
        placeholders = ",".join("?" * len(message_ids))
        rows = con.execute(
            f"""SELECT * FROM message_cache
                WHERE business_connection_id=? AND chat_id=? AND message_id IN ({placeholders})""",
            [business_connection_id, chat_id] + message_ids,
        ).fetchall()
    return rows


def get_triggers(owner_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM triggers WHERE owner_id=? ORDER BY id",
            (owner_id,),
        ).fetchall()


def get_active_triggers(owner_id: int):
    with db() as con:
        return con.execute(
            "SELECT * FROM triggers WHERE owner_id=? AND is_active=1",
            (owner_id,),
        ).fetchall()


def add_trigger(owner_id: int, keyword: str, response: str) -> bool:
    try:
        with db() as con:
            con.execute(
                "INSERT INTO triggers(owner_id,keyword,response,created_at) VALUES(?,?,?,?)",
                (owner_id, keyword.strip(), response, datetime.utcnow().isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def delete_trigger(trigger_id: int, owner_id: int) -> bool:
    with db() as con:
        cur = con.execute(
            "DELETE FROM triggers WHERE id=? AND owner_id=?",
            (trigger_id, owner_id),
        )
    return cur.rowcount > 0


def toggle_trigger(trigger_id: int, owner_id: int):
    with db() as con:
        row = con.execute(
            "SELECT is_active FROM triggers WHERE id=? AND owner_id=?",
            (trigger_id, owner_id),
        ).fetchone()
        if not row:
            return None
        new_state = 0 if row["is_active"] else 1
        con.execute(
            "UPDATE triggers SET is_active=? WHERE id=? AND owner_id=?",
            (new_state, trigger_id, owner_id),
        )
    return bool(new_state)


# ─── FSM States ───────────────────────────────────────────────────────────────
class AddTrigger(StatesGroup):
    waiting_keyword = State()
    waiting_response = State()


# ─── Keyboards ────────────────────────────────────────────────────────────────
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список триггеров", callback_data="triggers_list")],
        [InlineKeyboardButton(text="➕ Добавить триггер", callback_data="trigger_add")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")],
    ])


def triggers_kb(triggers) -> InlineKeyboardMarkup:
    rows = []
    for t in triggers:
        status = "✅" if t["is_active"] else "❌"
        rows.append([
            InlineKeyboardButton(
                text=f"{status} {t['keyword'][:20]}",
                callback_data=f"t_info_{t['id']}",
            )
        ])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def trigger_action_kb(trigger_id: int, is_active: bool) -> InlineKeyboardMarkup:
    toggle_text = "❌ Отключить" if is_active else "✅ Включить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=toggle_text, callback_data=f"t_toggle_{trigger_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"t_delete_{trigger_id}"),
        ],
        [InlineKeyboardButton(text="« К списку", callback_data="triggers_list")],
    ])


# ─── Middleware для business_messages_deleted ─────────────────────────────────
class BusinessDeletedMiddleware(BaseMiddleware):
    """
    Перехватывает апдейты типа business_messages_deleted через outer middleware,
    совместимо со всеми версиями aiogram 3.x
    """
    async def __call__(
        self,
        handler: Callable[[Update, dict], Awaitable[Any]],
        event: Update,
        data: dict,
    ) -> Any:
        deleted = getattr(event, "business_messages_deleted", None)
        if deleted is not None:
            bot: Bot = data["bot"]
            await handle_deleted(deleted, bot)
            return  # не передаём дальше в цепочку
        return await handler(event, data)


async def handle_deleted(deleted, bot: Bot):
    """Логика обработки удалённых бизнес-сообщений."""
    bc_id = deleted.business_connection_id
    chat_id = deleted.chat.id
    message_ids = deleted.message_ids

    try:
        bc = await bot.get_business_connection(bc_id)
        owner_id = bc.user.id
    except Exception as e:
        logger.warning("Cannot get business connection for deleted event: %s", e)
        return

    cached = get_cached_messages(bc_id, chat_id, message_ids)
    chat_title = (
        getattr(deleted.chat, "title", None)
        or getattr(deleted.chat, "full_name", None)
        or str(chat_id)
    )
    not_found_ids = set(message_ids) - {r["message_id"] for r in cached}

    header = (
        f"🗑 <b>Удалено сообщений: {len(message_ids)}</b>\n"
        f"📌 Чат: <b>{chat_title}</b> (<code>{chat_id}</code>)\n"
        f"🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n"
        "─────────────────────"
    )

    try:
        await bot.send_message(owner_id, header, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error("Cannot notify owner %d: %s", owner_id, e)
        return

    if not cached and not_found_ids:
        await bot.send_message(
            owner_id,
            f"⚠️ Сообщения <code>{', '.join(map(str, not_found_ids))}</code> не были закешированы "
            "(бот не видел их до удаления).",
            parse_mode=ParseMode.HTML,
        )
        return

    for row in cached:
        from_name = row["from_name"] or "Неизвестно"
        from_id = row["from_id"] or "?"
        date_str = row["date"]
        lines = [
            f"👤 <b>От:</b> {from_name} (<code>{from_id}</code>)",
            f"🕐 <b>Дата:</b> {date_str}",
            f"🆔 <b>ID:</b> <code>{row['message_id']}</code>",
        ]
        if row["text"]:
            lines.append(f"\n📝 <b>Текст:</b>\n{row['text']}")
        elif row["caption"]:
            lines.append(f"\n📝 <b>Подпись:</b>\n{row['caption']}")
        if row["media_type"]:
            lines.append(f"📎 <b>Медиа:</b> {row['media_type']}")

        msg_text = "\n".join(lines)
        mt = row["media_type"]
        fid = row["file_id"]

        try:
            if fid and mt:
                if mt == "photo":
                    await bot.send_photo(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                elif mt == "video":
                    await bot.send_video(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                elif mt == "document":
                    await bot.send_document(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                elif mt == "audio":
                    await bot.send_audio(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                elif mt == "voice":
                    await bot.send_voice(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                elif mt == "sticker":
                    await bot.send_sticker(owner_id, fid)
                    await bot.send_message(owner_id, msg_text, parse_mode=ParseMode.HTML)
                elif mt == "animation":
                    await bot.send_animation(owner_id, fid, caption=msg_text, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(owner_id, msg_text, parse_mode=ParseMode.HTML)
            else:
                await bot.send_message(owner_id, msg_text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error("Error forwarding deleted message to owner: %s", e)

    if not_found_ids:
        await bot.send_message(
            owner_id,
            f"⚠️ Не закешировано (ID): <code>{', '.join(map(str, not_found_ids))}</code>",
            parse_mode=ParseMode.HTML,
        )


# ─── Router ───────────────────────────────────────────────────────────────────
router = Router()


# ─── /start ───────────────────────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(message: Message):
    add_owner(message.from_user.id)
    await message.answer(
        "<b>👋 Привет!</b>\n\n"
        "Я бизнес-бот с функциями:\n"
        "• 🔍 <b>Перехват удалённых сообщений</b> — пришлю тебе всё, что удалили в бизнес-чатах\n"
        "• 🤖 <b>Автоответчик</b> — отвечаю на ключевые слова в бизнес-чатах\n\n"
        "Подключи бизнес-аккаунт через настройки Telegram:\n"
        "<i>Настройки → Telegram Business → Чат-боты → выбери этого бота</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(),
    )


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    if not is_owner(message.from_user.id):
        add_owner(message.from_user.id)
    await message.answer(
        "🏠 <b>Главное меню</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(),
    )


# ─── Callbacks ────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery):
    await call.message.edit_text(
        "🏠 <b>Главное меню</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(),
    )


@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    text = (
        "<b>ℹ️ Справка</b>\n\n"
        "<b>Удалённые сообщения</b>\n"
        "Когда кто-то удаляет сообщение в бизнес-чате — я пришлю его тебе сюда.\n\n"
        "<b>Автоответчик</b>\n"
        "Добавь триггеры — ключевые слова и HTML-ответы на них.\n"
        "Когда клиент напишет слово-триггер, я автоматически отвечу в чат.\n\n"
        "<b>HTML-теги в ответах:</b>\n"
        "<code>&lt;b&gt;жирный&lt;/b&gt;</code>\n"
        "<code>&lt;i&gt;курсив&lt;/i&gt;</code>\n"
        "<code>&lt;a href='...'&gt;ссылка&lt;/a&gt;</code>\n"
        "<code>&lt;code&gt;код&lt;/code&gt;</code>\n"
    )
    await call.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад", callback_data="main_menu")]
        ]),
    )


@router.callback_query(F.data == "triggers_list")
async def cb_triggers_list(call: CallbackQuery):
    triggers = get_triggers(call.from_user.id)
    if not triggers:
        await call.message.edit_text(
            "📋 <b>Триггеры</b>\n\nСписок пуст. Добавь первый триггер!",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить", callback_data="trigger_add")],
                [InlineKeyboardButton(text="« Назад", callback_data="main_menu")],
            ]),
        )
        return
    await call.message.edit_text(
        f"📋 <b>Триггеры</b> ({len(triggers)} шт.)\n\nВыбери триггер для управления:",
        parse_mode=ParseMode.HTML,
        reply_markup=triggers_kb(triggers),
    )


@router.callback_query(F.data.startswith("t_info_"))
async def cb_trigger_info(call: CallbackQuery):
    trigger_id = int(call.data.split("_")[2])
    with db() as con:
        t = con.execute(
            "SELECT * FROM triggers WHERE id=? AND owner_id=?",
            (trigger_id, call.from_user.id),
        ).fetchone()
    if not t:
        await call.answer("Триггер не найден", show_alert=True)
        return
    status = "✅ Активен" if t["is_active"] else "❌ Отключён"
    await call.message.edit_text(
        f"<b>Триггер #{t['id']}</b>\n\n"
        f"<b>Ключевое слово:</b> <code>{t['keyword']}</code>\n"
        f"<b>Статус:</b> {status}\n\n"
        f"<b>Ответ:</b>\n{t['response']}",
        parse_mode=ParseMode.HTML,
        reply_markup=trigger_action_kb(trigger_id, bool(t["is_active"])),
    )


@router.callback_query(F.data.startswith("t_toggle_"))
async def cb_trigger_toggle(call: CallbackQuery):
    trigger_id = int(call.data.split("_")[2])
    result = toggle_trigger(trigger_id, call.from_user.id)
    if result is None:
        await call.answer("Триггер не найден", show_alert=True)
        return
    await call.answer("включён ✅" if result else "отключён ❌")
    with db() as con:
        t = con.execute(
            "SELECT * FROM triggers WHERE id=? AND owner_id=?",
            (trigger_id, call.from_user.id),
        ).fetchone()
    if t:
        status = "✅ Активен" if t["is_active"] else "❌ Отключён"
        await call.message.edit_text(
            f"<b>Триггер #{t['id']}</b>\n\n"
            f"<b>Ключевое слово:</b> <code>{t['keyword']}</code>\n"
            f"<b>Статус:</b> {status}\n\n"
            f"<b>Ответ:</b>\n{t['response']}",
            parse_mode=ParseMode.HTML,
            reply_markup=trigger_action_kb(trigger_id, bool(t["is_active"])),
        )


@router.callback_query(F.data.startswith("t_delete_"))
async def cb_trigger_delete(call: CallbackQuery):
    trigger_id = int(call.data.split("_")[2])
    ok = delete_trigger(trigger_id, call.from_user.id)
    if ok:
        await call.answer("Триггер удалён 🗑")
        triggers = get_triggers(call.from_user.id)
        if not triggers:
            await call.message.edit_text(
                "📋 <b>Триггеры</b>\n\nСписок пуст.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Добавить", callback_data="trigger_add")],
                    [InlineKeyboardButton(text="« Назад", callback_data="main_menu")],
                ]),
            )
        else:
            await call.message.edit_text(
                f"📋 <b>Триггеры</b> ({len(triggers)} шт.)\n\nВыбери триггер:",
                parse_mode=ParseMode.HTML,
                reply_markup=triggers_kb(triggers),
            )
    else:
        await call.answer("Ошибка удаления", show_alert=True)


@router.callback_query(F.data == "trigger_add")
async def cb_trigger_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddTrigger.waiting_keyword)
    await call.message.edit_text(
        "➕ <b>Новый триггер</b>\n\n"
        "Введи <b>ключевое слово</b> (или фразу), на которое будет реагировать автоответчик.\n\n"
        "<i>Например: цена, прайс, доставка, привет</i>\n\n"
        "Напиши /cancel для отмены.",
        parse_mode=ParseMode.HTML,
    )


@router.message(AddTrigger.waiting_keyword)
async def fsm_got_keyword(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=main_menu_kb())
        return
    kw = (message.text or "").strip()
    if not kw:
        await message.answer("Ключевое слово не может быть пустым. Попробуй ещё раз:")
        return
    await state.update_data(keyword=kw)
    await state.set_state(AddTrigger.waiting_response)
    await message.answer(
        f"✅ Ключевое слово: <code>{kw}</code>\n\n"
        "Теперь введи <b>текст ответа</b>. Поддерживается HTML-разметка.\n\n"
        "<i>Например:</i> <code>&lt;b&gt;Наш прайс&lt;/b&gt;: example.com/price</code>\n\n"
        "Напиши /cancel для отмены.",
        parse_mode=ParseMode.HTML,
    )


@router.message(AddTrigger.waiting_response)
async def fsm_got_response(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=main_menu_kb())
        return
    response_text = message.text or message.caption or ""
    if not response_text.strip():
        await message.answer("Ответ не может быть пустым. Попробуй ещё раз:")
        return
    data = await state.get_data()
    keyword = data["keyword"]
    ok = add_trigger(message.from_user.id, keyword, response_text)
    await state.clear()
    if ok:
        await message.answer(
            f"✅ <b>Триггер добавлен!</b>\n\n"
            f"<b>Слово:</b> <code>{keyword}</code>\n"
            f"<b>Ответ:</b>\n{response_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(),
        )
    else:
        await message.answer(
            f"⚠️ Триггер с ключом <code>{keyword}</code> уже существует. "
            "Удали его и создай заново.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(),
        )


# ─── Business: входящие / редактирование ──────────────────────────────────────
@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    bc_id = message.business_connection_id
    if not bc_id:
        return

    cache_message(message, bc_id)

    try:
        bc = await bot.get_business_connection(bc_id)
        owner_id = bc.user.id
    except Exception as e:
        logger.warning("Cannot get business connection %s: %s", bc_id, e)
        return

    if message.from_user and message.from_user.id == owner_id:
        return

    text = (message.text or message.caption or "").lower()
    if not text:
        return

    for t in get_active_triggers(owner_id):
        if t["keyword"].lower() in text:
            try:
                await bot.send_message(
                    chat_id=message.chat.id,
                    text=t["response"],
                    parse_mode=ParseMode.HTML,
                    business_connection_id=bc_id,
                    reply_to_message_id=message.message_id,
                )
                logger.info("Auto-replied trigger #%d → chat %d", t["id"], message.chat.id)
            except Exception as e:
                logger.error("Auto-reply failed: %s", e)
            break


@router.edited_business_message()
async def on_business_edit(message: Message):
    bc_id = message.business_connection_id
    if bc_id:
        cache_message(message, bc_id)


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware перехватывает business_messages_deleted совместимо со всеми 3.x
    dp.update.outer_middleware(BusinessDeletedMiddleware())

    dp.include_router(router)

    allowed_updates = [
        "message",
        "edited_message",
        "callback_query",
        "business_message",
        "edited_business_message",
        "business_messages_deleted",
        "business_connection",
    ]

    logger.info("Bot starting...")
    await dp.start_polling(bot, allowed_updates=allowed_updates)


if __name__ == "__main__":
    asyncio.run(main())
