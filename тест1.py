import asyncio
import logging
import os
import json
import random
import math
from datetime import datetime, timezone, timedelta
import tempfile
from io import BytesIO
import qrcode
from base64 import urlsafe_b64encode
import aiohttp
import html
import sys
import time
from sqlalchemy import Column, Integer, String, Boolean, ARRAY, BigInteger, JSON, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import cairo
from PIL import Image
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, Dice, BufferedInputFile, FSInputFile, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from pytonconnect import TonConnect
from pytonconnect.storage import IStorage
from pytoniq_core import Address, begin_cell
from collections import defaultdict

# Константы
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7767867120:AAHwguMDVYSATqcL-DHPoP5BiH_RjAEOLUI")
XROCKET_DEV_API_KEY = os.getenv("XROCKET_DEV_API_KEY")
MANIFEST_URL = "https://raw.githubusercontent.com/XaBbl4/pytonconnect/main/pytonconnect-manifest.json"
XROCKET_API_URL = "https://pay.xrocket.tg/"
WEBHOOK_URL = "https://your-domain.com/webhook"  # Замените на ваш URL
WEBHOOK_PATH = "/webhook"
WEB_SERVER_HOST = "0.0.0.0"
WEB_SERVER_PORT = 8080
XROCKET_FEE = 0.015
DEFAULT_BOT_FEE = 0.05
STATUS_FEE = 1.0
WHEEL_FEE = 0.05
TICKETS_FEE = 0.05
CUSTOM_PRIZE_FEE = 0.05
GAME_TIMEOUT = 300
THROW_TIMEOUT = 900
AD_LIMIT_PER_DAY = 3
MOSCOW_TZ = timezone(timedelta(hours=3))
MIN_BANK_AMOUNT_TON = 0.2
MIN_BANK_AMOUNT_USDT = 1.0
MAX_AMOUNT = 1000.0
LOG_DIR = "logs"
DEV_USERS = {1, 6400281763}
RATE_LIMIT_ATTEMPTS = 5
RATE_LIMIT_PERIOD = 60
DATABASE_URL = "postgresql+asyncpg://postgres:your_password@localhost/bot_db"  # Замените пароль

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - %(funcName)s:%(lineno)d',
    handlers=[logging.StreamHandler(sys.stdout)]
)
for handler in logging.getLogger().handlers:
    handler.stream.reconfigure(encoding='utf-8')
    handler.addFilter(lambda record: not record.name.startswith('aiogram'))

logging.getLogger('aiogram').propagate = False

# Переводы
TRANSLATIONS = {
    "ru": {
        "welcome_group": "Добро пожаловать! Используйте команды /start в ЛС для меню, +bank для общего банка или +pvp для дуэли.",
        "welcome_user": "Меню пользователя:",
        "admin_groups": "Выберите группу для администрирования:",
        "language_select": "Выберите язык / Select language:",
        "game_already_running": "В группе уже идет игра. Дождитесь окончания.",
        "game_settings_missing": "Перед запуском игры установите xRocket токен и настройте приз в меню настроек.",
        "no_send_permission": "Бот не может отправлять сообщения в эту группу. Дайте разрешение перед запуском игр.",
        "bank_format": "Формат: +bank [тип игры] [сумма] [валюта]\nПример: +bank slot 0.2 TON",
        "bank_wrong_type": "Неверный тип игры. Доступны: slot, dice, basketball, football, bowling, darts",
        "bank_wrong_currency": "Валюта должна быть TON или USDT.",
        "bank_amount_invalid": "Сумма должна быть числом.",
        "bank_amount_too_low": "Сумма должна быть не менее {min_amount} {currency}.",
        "bank_amount_too_high": "Сумма не должна превышать {max_amount} {currency}.",
        "bank_invoice_error": "Ошибка создания инвойса. Попробуйте позже.",
        "bank_started": "Сбор общего банка открыт! Сумма: {amount} {currency}. Тип игры: {game_type}. Оплатите через ссылку и нажмите 'Участвую'. Время: 5 минут.",
        "bank_participate": "Вы участвуете в общем банке! Кидайте emoji после сбора.",
        "bank_not_collected": "Общий банк не собран. Игра отменена.",
        "bank_collected": "Сбор завершен! Сумма: {total} {currency}. Кидайте {emoji} в течение 15 минут!",
        "bank_no_participants": "Никто не участвовал. Возврат: {refund:.3f} {currency} каждому участнику.",
        "bank_winner": "Победил {winner} с результатом {value}! Полная сумма: {total} {currency}, выигрыш после комиссий xRocket ({xrocket_fee:.3f}) и бота ({bot_fee:.3f}): {prize:.3f} {currency}.",
        "bank_transfer_error": "Ошибка: недостаточно средств на балансе бота! Обратитесь к администратору.",
        "attempts_exceeded": "@{username}, вы превысили лимит попыток ({max_attempts}). Мут на 1 час.",
        "pvp_format_error": "Формат: +pvp @username [сумма] [валюта]\nПример: +pvp @Ivan 1 TON",
        "pvp_self_challenge": "Нельзя вызвать себя на PVP.",
        "pvp_user_not_found": "Пользователь не найден.",
        "pvp_accepted": "PVP принято! Оплатите {amount} {currency} каждый. Время: 5 минут.",
        "pvp_not_paid": "PVP отменено: не все оплатили.",
        "pvp_started": "PVP началось! Кидайте 🎲 3 раза каждый!",
        "pvp_incomplete": "PVP отменено: не все броски сделаны.",
        "pvp_draw": "Ничья ({score1} vs {score2})! Повторяем броски.",
        "ad_limit_exceeded": "Лимит рекламы на сегодня исчерпан ({limit}/{limit}).",
        "group_blocked": "Бот заблокирован в этой группе. Свяжитесь с @f_row для уточнения причин."
    },
    "en": {
        "welcome_group": "Welcome! Use /start in PM for the menu, +bank for a common bank, or +pvp for a duel.",
        "welcome_user": "User menu:",
        "admin_groups": "Select a group to administer:",
        "language_select": "Choose language / Select language:",
        "game_already_running": "A game is already running in the group. Wait for it to finish.",
        "game_settings_missing": "Before starting the game, set the xRocket token and configure the prize in the settings menu.",
        "no_send_permission": "The bot cannot send messages to this group. Grant permission before starting games.",
        "bank_format": "Format: +bank [game type] [amount] [currency]\nExample: +bank slot 0.2 TON",
        "bank_wrong_type": "Invalid game type. Available: slot, dice, basketball, football, bowling, darts",
        "bank_wrong_currency": "Currency must be TON or USDT.",
        "bank_amount_invalid": "Amount must be a number.",
        "bank_amount_too_low": "Amount must be at least {min_amount} {currency}.",
        "bank_amount_too_high": "Amount must not exceed {max_amount} {currency}.",
        "bank_invoice_error": "Error creating invoice. Try again later.",
        "bank_started": "Common bank collection started! Amount: {amount} {currency}. Game type: {game_type}. Pay via the link and click 'Participate'. Time: 5 minutes.",
        "bank_participate": "You are participating in the common bank! Throw emoji after collection.",
        "bank_not_collected": "Common bank not collected. Game canceled.",
        "bank_collected": "Collection completed! Amount: {total} {currency}. Throw {emoji} within 15 minutes!",
        "bank_no_participants": "No one participated. Refund: {refund:.3f} {currency} to each participant.",
        "bank_winner": "Winner: {winner} with result {value}! Full amount: {total} {currency}, prize after xRocket ({xrocket_fee:.3f}) and bot ({bot_fee:.3f}) fees: {prize:.3f} {currency}.",
        "bank_transfer_error": "Error: insufficient funds on the bot's balance! Contact the administrator.",
        "attempts_exceeded": "@{username}, you exceeded the attempt limit ({max_attempts}). Muted for 1 hour.",
        "pvp_format_error": "Format: +pvp @username [amount] [currency]\nExample: +pvp @Ivan 1 TON",
        "pvp_self_challenge": "You cannot challenge yourself to PVP.",
        "pvp_user_not_found": "User not found.",
        "pvp_accepted": "PVP accepted! Pay {amount} {currency} each. Time: 5 minutes.",
        "pvp_not_paid": "PVP canceled: not everyone paid.",
        "pvp_started": "PVP started! Throw 🎲 3 times each!",
        "pvp_incomplete": "PVP canceled: not all throws completed.",
        "pvp_draw": "Draw ({score1} vs {score2})! Repeat throws.",
        "ad_limit_exceeded": "Ad limit for today exceeded ({limit}/{limit}).",
        "group_blocked": "Bot is blocked in this group. Contact @f_row for details."
    }
}

# Настройка SQLAlchemy
Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True)
    username = Column(String)
    chat_id = Column(BigInteger)
    wallet_connected = Column(Boolean, default=False)
    wallet_address = Column(String)
    tickets = Column(Integer, default=0)
    status = Column(String, default='')
    language = Column(String, default='ru')
    tonconnect_storage = Column(JSON, default={})

class Group(Base):
    __tablename__ = "groups"
    group_id = Column(BigInteger, primary_key=True)
    group_title = Column(String)
    group_username = Column(String)
    primary_admins = Column(ARRAY(BigInteger), default=[])
    extra_admins = Column(ARRAY(BigInteger), default=[])
    admin_settings = Column(JSON, default={
        "xrocket_token": None, "holders_only": False, "ca": "", "min_coins": 0.0, "coin_selection": "TON",
        "prize_amount": 0.0, "custom_prize": None, "infinite_game": False, "tickets_enabled": False,
        "tickets_start_time": 0, "semi_win": False, "attempts_limit": 3, "bot_fee": DEFAULT_BOT_FEE
    })
    game_status = Column(Boolean, default=False)
    emoji_selection = Column(String, default='🎲')
    active_game = Column(String)
    game_data = Column(JSON, default={})
    blocked = Column(Boolean, default=False)
    ad_enabled = Column(Boolean, default=False)
    ad_message = Column(JSON)
    ad_count_today = Column(Integer, default=0)
    ad_last_reset = Column(DateTime)

async def init_db():
    """Инициализация таблиц в базе данных."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logging.info("База данных инициализирована с SQLAlchemy.")

# Middleware
class PermissionCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Update, data):
        if event.message and event.chat.type in ["group", "supergroup"]:
            try:
                bot_permissions = await bot.get_chat_member(event.chat.id, (await bot.get_me()).id)
                if not bot_permissions.can_send_messages:
                    await send_message_safe(event.chat.id, TRANSLATIONS[data.get("language", "ru")]["no_send_permission"])
                    return
            except Exception as e:
                logging.error(f"Ошибка проверки прав бота в чате {event.chat.id}: {e}")
                return
        return await handler(event, data)

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self):
        self.rate_limits = defaultdict(lambda: {"count": 0, "last_time": 0, "lock": asyncio.Lock()})
        self.limit = RATE_LIMIT_ATTEMPTS
        self.window = RATE_LIMIT_PERIOD

    async def __call__(self, handler, event, data):
        user_id = event.from_user.id if event.from_user else None
        if not user_id or not await is_bot_active():
            return await handler(event, data)
        
        current_time = time.time()
        user_data = self.rate_limits[user_id]
        
        async with user_data["lock"]:
            if current_time - user_data["last_time"] > self.window:
                user_data["count"] = 1
                user_data["last_time"] = current_time
            else:
                user_data["count"] += 1
                if user_data["count"] > self.limit:
                    language = await get_user_language(user_id)
                    await send_message_safe(event.chat.id if isinstance(event, Message) else event.message.chat.id,
                                           "Слишком много запросов. Подождите минуту." if language == "ru" else "Too many requests. Wait a minute.")
                    return
        return await handler(event, data)

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)
dp.update.middleware(RateLimitMiddleware())
dp.update.middleware(PermissionCheckMiddleware())

# Состояния для FSM
class Form(StatesGroup):
    waiting_language = State()
    waiting_xrocket_token = State()
    waiting_ca = State()  # Новое
    waiting_min_coins = State()  # Новое
    waiting_prize_amount = State()  # Новое
    waiting_custom_prize_text = State()  # Новое
    waiting_custom_prize_image = State()  # Новое
    waiting_add_admin = State()
    waiting_remove_admin = State()
    waiting_status_self = State()
    waiting_status_other_username = State()
    waiting_status_other_text = State()
    waiting_dev_ad_text = State()
    waiting_dev_ad_media = State()
    waiting_dev_ad_confirm = State()
    waiting_group_fee = State()

# Глобальные переменные
bot_active = True
url_storage = {}

# Функции для работы с базой данных
async def get_user_data(user_id: int) -> dict:
    async with async_session() as session:
        try:
            user = await session.get(User, user_id)
            if user:
                return {
                    "user_id": user.user_id,
                    "username": user.username,
                    "chat_id": user.chat_id,
                    "wallet_connected": user.wallet_connected,
                    "wallet_address": user.wallet_address,
                    "tickets": user.tickets,
                    "status": user.status,
                    "language": user.language,
                    "tonconnect_storage": user.tonconnect_storage
                }
            return {}
        except Exception as e:
            logging.error(f"Ошибка получения данных пользователя {user_id}: {e}")
            return {}

async def save_user_data(user_id: int, data: dict):
    async with async_session() as session:
        try:
            user = await session.get(User, user_id)
            if not user:
                user = User(user_id=user_id)
            user.username = data.get("username")
            user.chat_id = data.get("chat_id")
            user.wallet_connected = data.get("wallet_connected")
            user.wallet_address = data.get("wallet_address")
            user.tickets = data.get("tickets", 0)
            user.status = data.get("status", "")
            user.language = data.get("language", "ru")
            user.tonconnect_storage = data.get("tonconnect_storage", {})
            session.add(user)
            await session.commit()
        except Exception as e:
            logging.error(f"Ошибка сохранения данных пользователя {user_id}: {e}")

async def get_group_data(group_id: int) -> dict:
    async with async_session() as session:
        try:
            group = await session.get(Group, group_id)
            if group:
                return {
                    "group_id": group.group_id,
                    "group_title": group.group_title,
                    "group_username": group.group_username,
                    "primary_admins": group.primary_admins,
                    "extra_admins": group.extra_admins,
                    "admin_settings": group.admin_settings,
                    "game_status": group.game_status,
                    "emoji_selection": group.emoji_selection,
                    "active_game": group.active_game,
                    "game_data": group.game_data,
                    "blocked": group.blocked,
                    "ad_enabled": group.ad_enabled,
                    "ad_message": group.ad_message,
                    "ad_count_today": group.ad_count_today,
                    "ad_last_reset": group.ad_last_reset
                }
            return {}
        except Exception as e:
            logging.error(f"Ошибка получения данных группы {group_id}: {e}")
            return {}

async def save_group_data(group_id: int, data: dict):
    async with async_session() as session:
        try:
            group = await session.get(Group, group_id)
            if not group:
                group = Group(group_id=group_id)
            group.group_title = data.get("group_title")
            group.group_username = data.get("group_username")
            group.primary_admins = data.get("primary_admins", [])
            group.extra_admins = data.get("extra_admins", [])
            group.admin_settings = data.get("admin_settings", {
                "xrocket_token": None, "holders_only": False, "ca": "", "min_coins": 0.0, "coin_selection": "TON",
                "prize_amount": 0.0, "custom_prize": None, "infinite_game": False, "tickets_enabled": False,
                "tickets_start_time": 0, "semi_win": False, "attempts_limit": 3, "bot_fee": DEFAULT_BOT_FEE
            })
            group.game_status = data.get("game_status", False)
            group.emoji_selection = data.get("emoji_selection", "🎲")
            group.active_game = data.get("active_game")
            group.game_data = data.get("game_data", {})
            group.blocked = data.get("blocked", False)
            group.ad_enabled = data.get("ad_enabled", False)
            group.ad_message = data.get("ad_message")
            group.ad_count_today = data.get("ad_count_today", 0)
            group.ad_last_reset = data.get("ad_last_reset")
            session.add(group)
            await session.commit()
        except Exception as e:
            logging.error(f"Ошибка сохранения данных группы {group_id}: {e}")

async def get_user_language(user_id: int) -> str:
    user_data = await get_user_data(user_id)
    return user_data.get("language", "ru")

async def is_bot_active() -> bool:
    current_task = asyncio.current_task()
    user_id = getattr(current_task, "user_id", None) if current_task else None
    return bot_active or (user_id in DEV_USERS)

# Логирование событий группы
def log_event(group_id: int, event_type: str, details: str):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, f"group_{group_id}_log.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {event_type} | {details}\n")
    logging.info(f"Группа {group_id} - {event_type}: {details}", extra={"group_id": group_id})

# TonConnect Storage для подключения кошельков
class TcStorage(IStorage):
    def __init__(self, chat_id: int):
        self.chat_id = chat_id

    async def set_item(self, key: str, value: str):
        user_data = await get_user_data(self.chat_id)
        user_data.setdefault("tonconnect_storage", {})[key] = value
        await save_user_data(self.chat_id, user_data)

    async def get_item(self, key: str, default_value: str = None):
        user_data = await get_user_data(self.chat_id)
        return user_data.get("tonconnect_storage", {}).get(key, default_value)

    async def remove_item(self, key: str):
        user_data = await get_user_data(self.chat_id)
        if "tonconnect_storage" in user_data and key in user_data["tonconnect_storage"]:
            del user_data["tonconnect_storage"][key]
            await save_user_data(self.chat_id, user_data)

def get_connector(chat_id: int):
    return TonConnect(MANIFEST_URL, storage=TcStorage(chat_id))

def get_comment_message(destination_address: str, amount: int, comment: str) -> dict:
    boc = begin_cell().store_uint(0, 32).store_string(comment).end_cell().to_boc()
    return {
        "address": destination_address,
        "amount": str(amount),
        "payload": urlsafe_b64encode(boc).decode()
    }

# Асинхронные функции для работы с API
async def get_jetton_balance(wallet_address: str, jetton_address: str) -> float:
    try:
        raw_wallet = Address(wallet_address).to_str(is_user_friendly=False)
        raw_jetton = Address(jetton_address).to_str(is_user_friendly=False)
        url = f"https://tonapi.io/v2/accounts/{raw_wallet}/jettons"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    logging.warning(f"Не удалось получить баланс Jetton: статус {resp.status}", extra={"wallet": wallet_address})
                    return 0.0
                data = await resp.json()
                for item in data.get("balances", []):
                    if item.get("jetton", {}).get("address") == raw_jetton:
                        raw_bal = item.get("balance", "0")
                        decimals = item.get("jetton", {}).get("decimals", 9)
                        balance = float(raw_bal) / (10 ** decimals)
                        logging.info(f"Баланс Jetton для {wallet_address}: {balance}", extra={"wallet": wallet_address})
                        return balance
                return 0.0
    except Exception as e:
        logging.error(f"Ошибка при получении баланса Jetton для {wallet_address}: {e}", extra={"wallet": wallet_address})
        return 0.0

async def check_xrocket_balance(group_id: int, amount: float, currency: str) -> bool:
    group_data = await get_group_data(group_id)
    token = group_data["admin_settings"].get("xrocket_token")
    if not token:
        log_event(group_id, "Ошибка", "Токен xRocket не указан")
        return False
    headers = {"Rocket-Pay-Key": token}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{XROCKET_API_URL}app/info", headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    log_event(group_id, "Ошибка", f"Не удалось проверить баланс: статус {resp.status}")
                    return False
                data = await resp.json()
                for bal in data["data"]["balances"]:
                    if bal["currency"] == currency:
                        balance = float(bal["balance"])
                        log_event(group_id, "Информация", f"Проверен баланс: {balance} {currency}")
                        return balance >= amount
                log_event(group_id, "Ошибка", f"Валюта {currency} не найдена на балансе")
                return False
        except Exception as e:
            log_event(group_id, "Ошибка", f"Ошибка при проверке баланса xRocket: {e}")
            return False

async def send_xrocket_transfer(group_id: int, tg_user_id: int, amount: float, currency: str, description: str) -> bool:
    group_data = await get_group_data(group_id)
    token = group_data["admin_settings"].get("xrocket_token")
    if not token:
        log_event(group_id, "Ошибка", "Токен xRocket не указан")
        return False
    headers = {"Rocket-Pay-Key": token, "Content-Type": "application/json"}
    transfer_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "tgUserId": tg_user_id,
        "currency": currency,
        "amount": amount,
        "transferId": transfer_id,
        "description": description
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{XROCKET_API_URL}app/transfer", headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                response_text = await resp.text()
                if resp.status == 201:
                    log_event(group_id, "Приз", f"Переведено {amount} {currency} пользователю {tg_user_id}, transferId: {transfer_id}")
                    return True
                else:
                    log_event(group_id, "Ошибка", f"Ошибка перевода: статус {resp.status}, ответ: {response_text}")
                    return False
        except Exception as e:
            log_event(group_id, "Ошибка", f"Ошибка сети при переводе: {e}")
            return False

async def create_xrocket_invoice(entity_id: int, amount: float, currency: str, description: str, num_payments: int = 0, is_group: bool = True) -> dict:
    token = (await get_group_data(entity_id))["admin_settings"].get("xrocket_token") if is_group else XROCKET_DEV_API_KEY
    if not token:
        error_msg = f"Нет токена для {'группы' if is_group else 'пользователя'} {entity_id}"
        if is_group:
            log_event(entity_id, "Ошибка", error_msg)
        else:
            logging.error(error_msg)
        return {}
    headers = {"Rocket-Pay-Key": token, "Content-Type": "application/json"}
    payload = {
        "amount": float(amount),
        "currency": currency.upper(),
        "description": description,
        "numPayments": int(num_payments),
        "expiredIn": GAME_TIMEOUT,
        "callbackUrl": WEBHOOK_URL
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{XROCKET_API_URL}tg-invoices", headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                response_text = await resp.text()
                if resp.status == 201:
                    data = await resp.json()
                    invoice_data = data["data"]
                    if "url" not in invoice_data or "id" not in invoice_data:
                        error_msg = f"Неверный формат ответа: {response_text}"
                        if is_group:
                            log_event(entity_id, "Ошибка", error_msg)
                        else:
                            logging.error(error_msg)
                        return {}
                    if is_group:
                        log_event(entity_id, "Платеж", f"Создан инвойс: {amount} {currency}, ID: {invoice_data['id']}")
                    else:
                        logging.info(f"Создан инвойс для пользователя {entity_id}: {amount} {currency}, ID: {invoice_data['id']}")
                    return {"url": invoice_data["url"], "id": invoice_data["id"]}
                else:
                    error_msg = f"Не удалось создать инвойс: статус {resp.status}, ответ: {response_text}"
                    if is_group:
                        log_event(entity_id, "Ошибка", error_msg)
                    else:
                        logging.error(error_msg)
                    return {}
        except Exception as e:
            error_msg = f"Ошибка сети при создании инвойса: {e}"
            if is_group:
                log_event(entity_id, "Ошибка", error_msg)
            else:
                logging.error(error_msg)
            return {}

async def check_invoice_status(invoice_id: str) -> dict:
    payload = {"invoice_id": invoice_id}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(WEBHOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Проверен инвойс {invoice_id}: статус={data.get('status')}, сумма={data.get('amount', 0)}")
                    return {"paid": data.get("status") == "paid", "amount": data.get("amount", 0)}
                logging.warning(f"Не удалось проверить инвойс {invoice_id}: статус {resp.status}")
                return {"paid": False, "amount": 0}
        except Exception as e:
            logging.error(f"Ошибка при проверке инвойса {invoice_id}: {e}")
            return {"paid": False, "amount": 0}

# Класс для колеса фортуны
class FortuneWheel:
    def __init__(self, sectors):
        self.sectors = sectors
        self.sector_count = len(sectors)
        self.total_cells = sum(sectors.values())
        self.start_angle = random.uniform(0, 2 * math.pi)
        self.size = 800
        self.outer_radius = (self.size / 2) - 60
        self.inner_radius = self.outer_radius - 180
        self.sector_colors = self._generate_sector_colors()
        self.sector_angles = self._calculate_angles()
        self.logo_surface = cairo.ImageSurface.create_from_png("logo.png")
        self.background_image = cairo.ImageSurface.create_from_png("background.png")

    def _hsv_to_rgb(self, h, s, v):
        h = float(h) % 360
        s = float(s)
        v = float(v)
        h60 = h / 60.0
        h60f = math.floor(h60)
        hi = int(h60f) % 6
        f = h60 - h60f
        p = v * (1 - s)
        q = v * (1 - f * s)
        t = v * (1 - (1 - f) * s)
        if hi == 0: return v, t, p
        elif hi == 1: return q, v, p
        elif hi == 2: return p, v, t
        elif hi == 3: return p, q, v
        elif hi == 4: return t, p, v
        elif hi == 5: return v, p, q
        return 0, 0, 0

    def _generate_sector_colors(self):
        colors = {}
        hue_step = 360 / len(self.sectors)
        for i, name in enumerate(self.sectors):
            hue = (i * hue_step) % 360
            colors[name] = self._hsv_to_rgb(hue, 0.8, 0.9)
        return colors

    def _calculate_angles(self):
        angles = {}
        current_angle = 0
        angle_multiplier = 2 * math.pi / self.total_cells
        for name, weight in self.sectors.items():
            sector_angle = weight * angle_multiplier
            angles[name] = (current_angle, current_angle + sector_angle)
            current_angle += sector_angle
        return angles

    def _draw_sector(self, ctx, name, start_angle, end_angle):
        ctx.save()
        ctx.move_to(0, 0)
        ctx.arc(0, 0, self.outer_radius, start_angle, end_angle)
        ctx.close_path()
        ctx.set_source_rgb(*self.sector_colors[name])
        ctx.fill_preserve()
        ctx.set_source_rgb(1, 1, 1)
        ctx.set_line_width(2)
        ctx.stroke()
        text_angle = (start_angle + end_angle) / 2
        ctx.rotate(text_angle)
        ctx.set_font_size(28)
        ctx.select_font_face("", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
        ctx.move_to(self.inner_radius + 1, 15)
        ctx.set_source_rgb(1, 1, 1)
        ctx.show_text(name)
        ctx.restore()

    def draw_frame(self, rotation_angle):
        surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, self.size, self.size)
        ctx = cairo.Context(surface)
        scale_x = self.size / self.background_image.get_width()
        scale_y = self.size / self.background_image.get_height()
        ctx.scale(scale_x, scale_y)
        ctx.set_source_surface(self.background_image, 0, 0)
        ctx.paint()
        ctx.scale(1 / scale_x, 1 / scale_y)
        ctx.translate(self.size / 2, self.size / 2)
        ctx.rotate(rotation_angle + self.start_angle)
        ctx.arc(0, 0, self.outer_radius + 15, 0, 2 * math.pi)
        ctx.set_source_rgb(0.8, 0.7, 0.2)
        ctx.fill()
        for name, (start_angle, end_angle) in self.sector_angles.items():
            self._draw_sector(ctx, name, start_angle, end_angle)
        ctx.rotate(-(rotation_angle + self.start_angle))
        logo_w, logo_h = self.logo_surface.get_width(), self.logo_surface.get_height()
        max_logo_diam = (self.inner_radius - 15) * 2
        scale_factor = min(max_logo_diam / logo_w, max_logo_diam / logo_h)
        ctx.scale(scale_factor, scale_factor)
        ctx.set_source_surface(self.logo_surface, -logo_w / 2, -logo_h / 2)
        ctx.paint()
        ctx.scale(1 / scale_factor, 1 / scale_factor)
        pointer_size = 60
        ctx.move_to(self.outer_radius - 5, 0)
        ctx.line_to(self.outer_radius + pointer_size, pointer_size / 2)
        ctx.line_to(self.outer_radius + pointer_size, -pointer_size / 2)
        ctx.close_path()
        ctx.set_source_rgb(0.9, 0.2, 0.2)
        ctx.fill_preserve()
        ctx.set_source_rgb(1, 1, 1)
        ctx.set_line_width(2)
        ctx.stroke()
        buf = BytesIO()
        surface.write_to_png(buf)
        buf.seek(0)
        return Image.open(buf).convert("RGBA")

    def spin(self, frames=72):
        final_angle = random.uniform(20 * math.pi, 25 * math.pi)
        frames_list = []
        for i in range(frames):
            progress = i / frames
            eased_progress = 1 - (1 - progress) ** 4
            current_angle = final_angle * eased_progress
            frames_list.append(self.draw_frame(current_angle))
        final_position = (-final_angle - self.start_angle) % (2 * math.pi)
        winner = None
        epsilon = 1e-6
        for name, (start, end) in self.sector_angles.items():
            if (start - epsilon <= final_position <= end + epsilon) or (start - epsilon <= final_position + 2 * math.pi <= end + epsilon):
                winner = name
                break
        with tempfile.NamedTemporaryFile(suffix=".gif", delete=False) as temp:
            frames_list[0].save(temp.name, format='GIF', save_all=True, append_images=frames_list[1:], duration=50, loop=0)
            temp_path = temp.name
        return winner, temp_path

# Проверка настроек игры
def check_game_settings(group_data: dict) -> bool:
    settings = group_data["admin_settings"]
    result = settings["xrocket_token"] is not None and (settings["prize_amount"] > 0 or settings["custom_prize"] is not None)
    if not result:
        logging.warning(f"Проверка настроек игры не пройдена для группы {group_data['group_id']}")
    return result

# Клавиатуры для интерфейса
async def user_menu_keyboard(language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Подключить кошелек" if language == "ru" else "Connect wallet", callback_data="user_connect_wallet")
    kb.button(text="Отключить кошелек" if language == "ru" else "Disconnect wallet", callback_data="user_disconnect_wallet")
    kb.button(text="Статус" if language == "ru" else "Status", callback_data="user_status_menu")
    kb.adjust(1)
    return kb.as_markup()

def language_select_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang:ru")
    kb.button(text="English", callback_data="lang:en")
    kb.adjust(2)
    return kb.as_markup()

async def admin_main_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Купить рекламу" if language == "ru" else "Buy advertisement", url="https://t.me/f_row")
    kb.button(text="Игры" if language == "ru" else "Games", callback_data=f"admin_games:{group_id}")
    kb.button(text="Настройки" if language == "ru" else "Settings", callback_data=f"admin_settings:{group_id}")
    kb.button(text="Добавить админа" if language == "ru" else "Add admin", callback_data=f"admin_add_admin:{group_id}")
    kb.button(text="Удалить админа" if language == "ru" else "Remove admin", callback_data=f"admin_remove_admin:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data="admin_groups_back")
    kb.adjust(1)
    return kb.as_markup()

async def admin_games_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Emoji игра" if language == "ru" else "Emoji game", callback_data=f"admin_games_emoji:{group_id}")
    kb.button(text="Колесо фортуны" if language == "ru" else "Fortune wheel", callback_data=f"admin_games_wheel:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_main_menu:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

async def admin_settings_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="xRocket Pay", callback_data=f"admin_settings_xrocket:{group_id}")
    kb.button(text="Текущие настройки" if language == "ru" else "Current settings", callback_data=f"admin_settings_current:{group_id}")
    kb.button(text="Только холдеры" if language == "ru" else "Holders only", callback_data=f"admin_settings_holders:{group_id}")
    kb.button(text="Приз" if language == "ru" else "Prize", callback_data=f"admin_settings_prize:{group_id}")
    kb.button(text="Билетики" if language == "ru" else "Tickets", callback_data=f"admin_settings_tickets:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_main_menu:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def xrocket_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Установить токен" if language == "ru" else "Set token", callback_data=f"xrocket_set_token:{group_id}")
    kb.button(text="Удалить токен" if language == "ru" else "Delete token", callback_data=f"xrocket_delete_token:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_settings:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def holders_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Установить CA" if language == "ru" else "Set CA", callback_data=f"holders_set_ca:{group_id}")
    kb.button(text="Установить мин. монеты" if language == "ru" else "Set min coins", callback_data=f"holders_set_min_coins:{group_id}")
    kb.button(text="Включить" if language == "ru" else "Enable", callback_data=f"holders_on:{group_id}")
    kb.button(text="Выключить" if language == "ru" else "Disable", callback_data=f"holders_off:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_settings:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def prize_menu_keyboard(group_id: int, language: str = "ru"):
    group_data = await get_group_data(group_id)
    current_coin = group_data["admin_settings"]["coin_selection"]
    kb = InlineKeyboardBuilder()
    kb.button(text="Установить сумму" if language == "ru" else "Set amount", callback_data=f"prize_set_amount:{group_id}")
    kb.button(text="Установить кастомный приз" if language == "ru" else "Set custom prize", callback_data=f"prize_set_custom:{group_id}")
    kb.button(text=f"TON{'✅' if current_coin == 'TON' else ''}", callback_data=f"prize_set_ton:{group_id}")
    kb.button(text=f"USDT{'✅' if current_coin == 'USDT' else ''}", callback_data=f"prize_set_usdt:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_settings:{group_id}")
    kb.adjust(1, 1, 2, 1)
    return kb.as_markup()

def custom_prize_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Установить текст" if language == "ru" else "Set text", callback_data=f"custom_prize_text:{group_id}")
    kb.button(text="Установить изображение" if language == "ru" else "Set image", callback_data=f"custom_prize_image:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"prize_menu:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def tickets_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Включить" if language == "ru" else "Enable", callback_data=f"tickets_on:{group_id}")
    kb.button(text="Выключить" if language == "ru" else "Disable", callback_data=f"tickets_off:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_settings:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def emoji_game_menu_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Начать игру" if language == "ru" else "Start game", callback_data=f"emoji_game_start:{group_id}")
    kb.button(text="Остановить игру" if language == "ru" else "Stop game", callback_data=f"emoji_game_stop:{group_id}")
    kb.button(text="Смена Emoji" if language == "ru" else "Change Emoji", callback_data=f"emoji_game_change_emoji:{group_id}")
    kb.button(text="Бесконечная игра" if language == "ru" else "Infinite game", callback_data=f"emoji_game_infinite:{group_id}")
    kb.button(text="Полувыигрыши" if language == "ru" else "Semi-wins", callback_data=f"emoji_game_semiwin:{group_id}")
    kb.button(text="Кол-во попыток" if language == "ru" else "Attempts count", callback_data=f"emoji_game_attempts:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"admin_games:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def attempts_menu_keyboard(group_id: int, language: str = "ru"):
    group_data = await get_group_data(group_id)
    current_attempts = group_data["admin_settings"]["attempts_limit"]
    kb = InlineKeyboardBuilder()
    for i in range(1, 7):
        text = f"{i}{'✅' if i == current_attempts else ''}"
        kb.button(text=text, callback_data=f"set_attempts:{group_id}:{i}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"emoji_game_menu:{group_id}")
    kb.adjust(3, 1)
    return kb.as_markup()

def change_emoji_keyboard(group_id: int, selected_emoji: str, language: str = "ru"):
    emojis = ["🎰", "🎲", "🎯", "🏀", "⚽", "🎳"]
    kb = InlineKeyboardBuilder()
    for e in emojis:
        text = f"{e}✅" if e == selected_emoji else e
        kb.button(text=text, callback_data=f"set_emoji:{group_id}:{e}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"emoji_game_menu:{group_id}")
    kb.adjust(3)
    return kb.as_markup()

def infinite_game_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Включить" if language == "ru" else "Enable", callback_data=f"infinite_on:{group_id}")
    kb.button(text="Выключить" if language == "ru" else "Disable", callback_data=f"infinite_off:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"emoji_game_menu:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

def semiwin_keyboard(group_id: int, language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Включить" if language == "ru" else "Enable", callback_data=f"semiwin_on:{group_id}")
    kb.button(text="Выключить" if language == "ru" else "Disable", callback_data=f"semiwin_off:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"emoji_game_menu:{group_id}")
    kb.adjust(1)
    return kb.as_markup()

async def dev_menu_keyboard(language: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Выключить бот" if bot_active else "Включить бот", callback_data="dev_toggle_bot")
    kb.button(text="Реклама" if language == "ru" else "Ads", callback_data="dev_ads:1")
    kb.button(text="Группы" if language == "ru" else "Groups", callback_data="dev_groups:1")
    kb.adjust(1)
    return kb.as_markup()

async def dev_ads_keyboard(page: int = 1, language: str = "ru"):
    async with async_session() as session:
        try:
            groups = await session.execute("SELECT group_id, group_title, ad_enabled FROM groups ORDER BY group_id LIMIT 10 OFFSET :offset", {"offset": (page - 1) * 10})
            groups = groups.fetchall()
            total = await session.execute("SELECT COUNT(*) FROM groups")
            total = total.scalar()
        except Exception as e:
            logging.error(f"Ошибка получения групп для dev_ads_keyboard: {e}")
            groups = []
            total = 0
    
    kb = InlineKeyboardBuilder()
    for group_id, title, ad_enabled in groups:
        kb.button(text=title, callback_data=f"dev_ad_group:{group_id}")
        kb.button(text="➕", callback_data=f"dev_ad_create:{group_id}")
        kb.button(text="➖", callback_data=f"dev_ad_delete:{group_id}")
        kb.button(text="Включить рекламу" if not ad_enabled else "Выключить рекламу", callback_data=f"dev_ad_toggle:{group_id}")
        kb.adjust(3, 1)
    
    total_pages = (total + 9) // 10
    if total_pages > 1:
        pagination = []
        if page > 1:
            pagination.append(("◄", f"dev_ads:{page-1}"))
        for p in range(max(1, page-2), min(total_pages+1, page+3)):
            pagination.append((str(p), f"dev_ads:{p}" if p != page else "noop"))
        if page < total_pages:
            pagination.append(("►", f"dev_ads:{page+1}"))
        for text, cb in pagination[:5]:
            kb.button(text=text, callback_data=cb)
        kb.adjust(5)
    return kb.as_markup()

async def dev_groups_keyboard(page: int = 1, language: str = "ru"):
    async with async_session() as session:
        try:
            groups = await session.execute("SELECT group_id, group_title, blocked FROM groups ORDER BY group_id LIMIT 10 OFFSET :offset", {"offset": (page - 1) * 10})
            groups = groups.fetchall()
            total = await session.execute("SELECT COUNT(*) FROM groups")
            total = total.scalar()
        except Exception as e:
            logging.error(f"Ошибка получения групп для dev_groups_keyboard: {e}")
            groups = []
            total = 0
    
    kb = InlineKeyboardBuilder()
    for group_id, title, blocked in groups:
        kb.button(text=title, callback_data=f"dev_group:{group_id}")
        kb.button(text="Заблокировать" if not blocked else "Разблокировать", callback_data=f"dev_group_toggle:{group_id}")
        kb.button(text="Изменить комиссию", callback_data=f"dev_group_fee:{group_id}")
        kb.adjust(3)
    
    total_pages = (total + 9) // 10
    if total_pages > 1:
        pagination = []
        if page > 1:
            pagination.append(("◄", f"dev_groups:{page-1}"))
        for p in range(max(1, page-2), min(total_pages+1, page+3)):
            pagination.append((str(p), f"dev_groups:{p}" if p != page else "noop"))
        if page < total_pages:
            pagination.append(("►", f"dev_groups:{page+1}"))
        for text, cb in pagination[:5]:
            kb.button(text=text, callback_data=cb)
        kb.adjust(5)
    kb.button(text="Назад" if language == "ru" else "Back", callback_data="dev_menu")
    return kb.as_markup()

async def dev_group_menu_keyboard(group_id: int, language: str = "ru"):
    group_data = await get_group_data(group_id)
    kb = InlineKeyboardBuilder()
    kb.button(text=group_data["group_title"], callback_data=f"dev_group:{group_id}")
    kb.button(text="Заблокировать" if not group_data["blocked"] else "Разблокировать", callback_data=f"dev_group_toggle:{group_id}")
    kb.button(text="Изменить комиссию", callback_data=f"dev_group_fee:{group_id}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data="dev_groups:1")
    kb.adjust(3, 1)
    return kb.as_markup()

# Утилитные функции
async def send_message_safe(chat_id: int, text: str, reply_markup=None):
    try:
        await bot.send_message(chat_id, text, reply_markup=reply_markup)
    except TelegramForbiddenError:
        logging.error(f"Не удалось отправить сообщение в чат {chat_id}: бот заблокирован или удален")
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в чат {chat_id}: {e}")

async def delayed_task(delay: int, coro, group_id: int):
    await asyncio.sleep(delay)
    await coro(group_id)

def schedule_game_task(group_id: int, delay: int, coro):
    task = asyncio.create_task(delayed_task(delay, coro, group_id))
    task.group_id = group_id
    return task

# Обработчики
@dp.message(lambda message: message.text == "/start")
async def start_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return

    language = await get_user_language(user_id)
    if not language:
        await send_message_safe(message.chat.id, TRANSLATIONS["ru"]["language_select"], reply_markup=language_select_keyboard())
        await state.set_state(Form.waiting_language)
        logging.info(f"Пользователь {user_id} запрошен выбор языка")
        return

    user_data = await get_user_data(user_id)
    if not user_data:
        user_data = {
            "user_id": user_id,
            "username": message.from_user.username or "",
            "chat_id": message.chat.id,
            "language": language
        }
        await save_user_data(user_id, user_data)

    if message.chat.type in ["group", "supergroup"]:
        group_data = await get_group_data(message.chat.id)
        if group_data.get("blocked", False):
            await send_message_safe(message.chat.id, TRANSLATIONS[language]["group_blocked"])
            await bot.leave_chat(message.chat.id)
            return
        await send_message_safe(message.chat.id, TRANSLATIONS[language]["welcome_group"])
    else:
        kb = await user_menu_keyboard(language)
        if user_id in DEV_USERS:
            kb = InlineKeyboardBuilder.from_markup(kb)
            kb.button(text="Dev меню" if language == "ru" else "Dev menu", callback_data="dev_menu")
            kb.adjust(1)
        await send_message_safe(message.chat.id, TRANSLATIONS[language]["welcome_user"], reply_markup=kb.as_markup())
        async with async_session() as session:
            try:
                result = await session.execute("SELECT group_id, group_title FROM groups WHERE :user_id = ANY(primary_admins) OR :user_id = ANY(extra_admins)", {"user_id": user_id})
                admin_groups = result.fetchall()
            except Exception as e:
                logging.error(f"Ошибка получения админских групп для {user_id}: {e}")
                admin_groups = []
        if admin_groups:
            admin_kb = InlineKeyboardBuilder()
            for g_id, g_title in admin_groups:
                admin_kb.button(text=g_title, callback_data=f"admin_group:{g_id}")
            admin_kb.adjust(1)
            await send_message_safe(message.chat.id, TRANSLATIONS[language]["admin_groups"], reply_markup=admin_kb.as_markup())
    logging.info(f"Пользователь {user_id} запустил бота, язык: {language}")

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def language_handler(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    language = call.data.split(":")[1]
    user_data = await get_user_data(user_id)
    user_data["language"] = language
    await save_user_data(user_id, user_data)
    await call.message.edit_text(TRANSLATIONS[language]["welcome_user"], reply_markup=await user_menu_keyboard(language))
    await state.clear()
    logging.info(f"Пользователь {user_id} выбрал язык: {language}")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_group:"))
async def admin_group_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    language = await get_user_language(user_id)
    group_data = await get_group_data(group_id)
    if user_id in group_data["primary_admins"] or user_id in group_data["extra_admins"]:
        await call.message.edit_text(f"Управление группой «{group_data['group_title']}»:" if language == "ru" else f"Managing group «{group_data['group_title']}»:", reply_markup=await admin_main_menu_keyboard(group_id, language))
    else:
        await send_message_safe(call.message.chat.id, "Вы не администратор этой группы." if language == "ru" else "You are not an admin of this group.")
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл управление группой {group_id}")

@dp.callback_query(lambda c: c.data == "admin_groups_back")
async def admin_groups_back(call: CallbackQuery):
    await start_handler(call.message, None)
    await call.answer()
    logging.info(f"Пользователь {call.from_user.id} вернулся к списку групп")

@dp.callback_query(lambda c: c.data == "dev_menu")
async def dev_menu_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    language = await get_user_language(user_id)
    await call.message.edit_text("Dev меню:" if language == "ru" else "Dev menu:", reply_markup=await dev_menu_keyboard(language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл dev-меню")

@dp.callback_query(lambda c: c.data.startswith("dev_ads:"))
async def dev_ads_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    page = int(call.data.split(":")[1])
    language = await get_user_language(user_id)
    await call.message.edit_text("Управление рекламой:" if language == "ru" else "Ad management:", reply_markup=await dev_ads_keyboard(page, language))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("dev_groups:"))
async def dev_groups_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    page = int(call.data.split(":")[1])
    language = await get_user_language(user_id)
    await call.message.edit_text("Управление группами:" if language == "ru" else "Group management:", reply_markup=await dev_groups_keyboard(page, language))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("dev_group:"))
async def dev_group_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    group_id = int(call.data.split(":")[1])
    language = await get_user_language(user_id)
    await call.message.edit_text(f"Управление группой:" if language == "ru" else "Group management:", reply_markup=await dev_group_menu_keyboard(group_id, language))
    await call.answer()

@dp.callback_query(lambda c: c.data == "dev_toggle_bot")
async def dev_toggle_bot_handler(call: CallbackQuery):
    global bot_active
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    bot_active = not bot_active
    language = await get_user_language(user_id)
    await call.message.edit_text("Бот переключен." if language == "ru" else "Bot toggled.", reply_markup=await dev_menu_keyboard(language))
    await call.answer()
    logging.info(f"Бот переключен в состояние {bot_active} пользователем {user_id}")

@dp.callback_query(lambda c: c.data.startswith("dev_ad_toggle:"))
async def dev_ad_toggle_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    group_id = int(call.data.split(":")[1])
    group_data = await get_group_data(group_id)
    group_data["ad_enabled"] = not group_data["ad_enabled"]
    await save_group_data(group_id, group_data)
    language = await get_user_language(user_id)
    await call.message.edit_text(f"Реклама для группы изменена." if language == "ru" else "Ads for group toggled.", reply_markup=await dev_ads_keyboard(1, language))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("dev_group_toggle:"))
async def dev_group_toggle_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id not in DEV_USERS:
        await call.answer("У вас нет доступа к dev-меню.")
        return
    group_id = int(call.data.split(":")[1])
    group_data = await get_group_data(group_id)
    group_data["blocked"] = not group_data["blocked"]
    await save_group_data(group_id, group_data)
    language = await get_user_language(user_id)
    await call.message.edit_text(f"Статус группы изменен." if language == "ru" else "Group status toggled.", reply_markup=await dev_groups_keyboard(1, language))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_games_emoji:"))
async def admin_games_emoji_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Emoji игра:" if language == "ru" else "Emoji game:", reply_markup=emoji_game_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню Emoji игры для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_settings_holders:"))
async def holders_settings_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы." if language == "ru" else "You are not an admin of this group.")
        return
    await call.message.edit_text("Только холдеры:" if language == "ru" else "Holders only:", reply_markup=holders_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню холдеров для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("holders_set_ca:"))
async def holders_set_ca_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Введите контрактный адрес (CA):" if language == "ru" else "Enter contract address (CA):")
    await state.set_state(Form.waiting_ca)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_ca)
async def process_ca(message: Message, state: FSMContext):
    ca = message.text.strip()
    data = await state.get_data()
    group_id = data["group_id"]
    user_id = message.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы.")
        await state.clear()
        return
    group_data["admin_settings"]["ca"] = ca
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "CA установлен." if language == "ru" else "CA set.", reply_markup=holders_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {user_id} установил CA для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("holders_set_min_coins:"))
async def holders_set_min_coins_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Введите минимальное количество монет:" if language == "ru" else "Enter minimum number of coins:")
    await state.set_state(Form.waiting_min_coins)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_min_coins)
async def process_min_coins(message: Message, state: FSMContext):
    try:
        min_coins = float(message.text.strip())
        if min_coins < 0:
            raise ValueError
    except ValueError:
        language = await get_user_language(message.from_user.id)
        await send_message_safe(message.chat.id, "Введите корректное положительное число." if language == "ru" else "Enter a valid positive number.")
        return
    data = await state.get_data()
    group_id = data["group_id"]
    user_id = message.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы.")
        await state.clear()
        return
    group_data["admin_settings"]["min_coins"] = min_coins
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "Минимальное количество монет установлено." if language == "ru" else "Minimum coins set.", reply_markup=holders_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {user_id} установил минимальное количество монет для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("holders_on:"))
async def holders_on_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["holders_only"] = True
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Только холдеры включены." if language == "ru" else "Holders only enabled.", reply_markup=holders_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} включил режим только для холдеров в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("holders_off:"))
async def holders_off_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["holders_only"] = False
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Только холдеры отключены." if language == "ru" else "Holders only disabled.", reply_markup=holders_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} отключил режим только для холдеров в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_settings_prize:"))
async def prize_settings_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Приз:" if language == "ru" else "Prize:", reply_markup=await prize_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню призов для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("prize_set_amount:"))
async def prize_set_amount_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Введите сумму приза:" if language == "ru" else "Enter prize amount:")
    await state.set_state(Form.waiting_prize_amount)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_prize_amount)
async def process_prize_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount < 0:
            raise ValueError
    except ValueError:
        language = await get_user_language(message.from_user.id)
        await send_message_safe(message.chat.id, "Введите корректное положительное число." if language == "ru" else "Enter a valid positive number.")
        return
    data = await state.get_data()
    group_id = data["group_id"]
    user_id = message.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы.")
        await state.clear()
        return
    group_data["admin_settings"]["prize_amount"] = amount
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "Сумма приза установлена." if language == "ru" else "Prize amount set.", reply_markup=await prize_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {user_id} установил сумму приза для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("prize_set_custom:"))
async def prize_set_custom_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Кастомный приз:" if language == "ru" else "Custom prize:", reply_markup=custom_prize_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню кастомного приза для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("custom_prize_text:"))
async def custom_prize_text_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Введите текст кастомного приза:" if language == "ru" else "Enter custom prize text:")
    await state.set_state(Form.waiting_custom_prize_text)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_custom_prize_text)
async def process_custom_prize_text(message: Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    group_id = data["group_id"]
    user_id = message.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы.")
        await state.clear()
        return
    group_data["admin_settings"]["custom_prize"] = {"type": "text", "value": text}
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "Текст кастомного приза установлен." if language == "ru" else "Custom prize text set.", reply_markup=custom_prize_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {user_id} установил текст кастомного приза для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("custom_prize_image:"))
async def custom_prize_image_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Отправьте изображение для кастомного приза:" if language == "ru" else "Send an image for the custom prize:")
    await state.set_state(Form.waiting_custom_prize_image)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_custom_prize_image)
async def process_custom_prize_image(message: Message, state: FSMContext):
    if not message.photo:
        language = await get_user_language(message.from_user.id)
        await send_message_safe(message.chat.id, "Отправьте изображение." if language == "ru" else "Send an image.")
        return
    data = await state.get_data()
    group_id = data["group_id"]
    user_id = message.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы.")
        await state.clear()
        return
    group_data["admin_settings"]["custom_prize"] = {"type": "photo", "file_id": message.photo[-1].file_id}
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "Изображение кастомного приза установлено." if language == "ru" else "Custom prize image set.", reply_markup=custom_prize_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {user_id} установил изображение кастомного приза для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("prize_set_ton:"))
async def prize_set_ton_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["coin_selection"] = "TON"
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Приз:" if language == "ru" else "Prize:", reply_markup=await prize_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} выбрал TON для приза в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("prize_set_usdt:"))
async def prize_set_usdt_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["coin_selection"] = "USDT"
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Приз:" if language == "ru" else "Prize:", reply_markup=await prize_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} выбрал USDT для приза в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_settings_tickets:"))
async def tickets_settings_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Билетики:" if language == "ru" else "Tickets:", reply_markup=tickets_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню билетиков для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("tickets_on:"))
async def tickets_on_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["tickets_enabled"] = True
    group_data["admin_settings"]["tickets_start_time"] = int(time.time())
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Билетики включены." if language == "ru" else "Tickets enabled.", reply_markup=tickets_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} включил билетики в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("tickets_off:"))
async def tickets_off_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["tickets_enabled"] = False
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Билетики отключены." if language == "ru" else "Tickets disabled.", reply_markup=tickets_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} отключил билетики в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_start:"))
async def emoji_game_start_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    if group_data["game_status"]:
        await send_message_safe(group_id, TRANSLATIONS[language]["game_already_running"])
        await call.answer()
        return
    if not check_game_settings(group_data):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_settings_missing"])
        await call.answer()
        return
    group_data["game_status"] = True
    group_data["active_game"] = "emoji"
    group_data["game_data"] = {
        "start_time": int(time.time()),
        "participants": {},
        "best_result": {"value": 0, "user_id": None, "timestamp": 0},
        "attempts": {}
    }
    await save_group_data(group_id, group_data)
    await send_message_safe(group_id, f"Emoji игра началась! Кидайте {group_data['emoji_selection']}!" if language == "ru" else f"Emoji game started! Throw {group_data['emoji_selection']}!")
    await call.message.edit_text("Emoji игра:" if language == "ru" else "Emoji game:", reply_markup=emoji_game_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} запустил Emoji игру в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_stop:"))
async def emoji_game_stop_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    if group_data["active_game"] != "emoji" or not group_data["game_status"]:
        await call.answer("Нет активной Emoji игры." if language == "ru" else "No active Emoji game.")
        return
    group_data["game_status"] = False
    group_data["active_game"] = None
    group_data["game_data"] = {}
    await save_group_data(group_id, group_data)
    await send_message_safe(group_id, "Emoji игра остановлена." if language == "ru" else "Emoji game stopped.")
    await call.message.edit_text("Emoji игра:" if language == "ru" else "Emoji game:", reply_markup=emoji_game_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} остановил Emoji игру в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_change_emoji:"))
async def emoji_game_change_emoji_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Выберите Emoji:" if language == "ru" else "Select Emoji:", reply_markup=change_emoji_keyboard(group_id, group_data["emoji_selection"], language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню смены Emoji для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("set_emoji:"))
async def set_emoji_handler(call: CallbackQuery):
    _, group_id, emoji = call.data.split(":")
    user_id = call.from_user.id
    group_data = await get_group_data(int(group_id))
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["emoji_selection"] = emoji
    await save_group_data(int(group_id), group_data)
    await call.message.edit_text("Emoji игра:" if language == "ru" else "Emoji game:", reply_markup=emoji_game_menu_keyboard(int(group_id), language))
    await call.answer(f"Emoji изменено на {emoji}")
    logging.info(f"Пользователь {user_id} сменил Emoji на {emoji} для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_infinite:"))
async def emoji_game_infinite_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Бесконечная игра:" if language == "ru" else "Infinite game:", reply_markup=infinite_game_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню бесконечной игры для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("infinite_on:"))
async def infinite_on_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["infinite_game"] = True
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Бесконечная игра включена." if language == "ru" else "Infinite game enabled.", reply_markup=infinite_game_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} включил бесконечную игру в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("infinite_off:"))
async def infinite_off_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["infinite_game"] = False
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Бесконечная игра отключена." if language == "ru" else "Infinite game disabled.", reply_markup=infinite_game_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} отключил бесконечную игру в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_semiwin:"))
async def emoji_game_semiwin_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Полувыигрыши:" if language == "ru" else "Semi-wins:", reply_markup=semiwin_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню полувыигрышей для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("semiwin_on:"))
async def semiwin_on_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["semi_win"] = True
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Полувыигрыши включены." if language == "ru" else "Semi-wins enabled.", reply_markup=semiwin_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} включил полувыигрыши в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("semiwin_off:"))
async def semiwin_off_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["semi_win"] = False
    await save_group_data(group_id, group_data)
    await call.message.edit_text("Полувыигрыши отключены." if language == "ru" else "Semi-wins disabled.", reply_markup=semiwin_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} отключил полувыигрыши в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_attempts:"))
async def emoji_game_attempts_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Количество попыток:" if language == "ru" else "Attempts count:", reply_markup=await attempts_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню количества попыток для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("set_attempts:"))
async def set_attempts_handler(call: CallbackQuery):
    _, group_id, attempts = call.data.split(":")
    user_id = call.from_user.id
    group_data = await get_group_data(int(group_id))
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["attempts_limit"] = int(attempts)
    await save_group_data(int(group_id), group_data)
    await call.message.edit_text("Количество попыток:" if language == "ru" else "Attempts count:", reply_markup=await attempts_menu_keyboard(int(group_id), language))
    await call.answer(f"Установлено: {attempts} попыток" if language == "ru" else f"Set: {attempts} attempts")
    logging.info(f"Пользователь {user_id} установил {attempts} попыток для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("emoji_game_menu:"))
async def emoji_game_menu_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("Emoji игра:" if language == "ru" else "Emoji game:", reply_markup=emoji_game_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} вернулся в меню Emoji игры для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_settings_xrocket:"))
async def admin_settings_xrocket_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await call.message.edit_text("xRocket Pay:", reply_markup=xrocket_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} открыл меню xRocket для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("xrocket_set_token:"))
async def xrocket_set_token_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    await send_message_safe(call.message.chat.id, "Введите токен xRocket:" if language == "ru" else "Enter xRocket token:")
    await state.set_state(Form.waiting_xrocket_token)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("xrocket_delete_token:"))
async def xrocket_delete_token_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы.")
        return
    group_data["admin_settings"]["xrocket_token"] = None
    await save_group_data(group_id, group_data)
    await send_message_safe(call.message.chat.id, "Токен удален." if language == "ru" else "Token deleted.", reply_markup=xrocket_menu_keyboard(group_id, language))
    await call.answer()
    logging.info(f"Пользователь {user_id} удалил токен xRocket для группы {group_id}")

@dp.message(Form.waiting_xrocket_token)
async def process_xrocket_token(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    group_id = data["group_id"]
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Вы не администратор этой группы." if language == "ru" else "You are not an admin of this group.")
        await state.clear()
        return
    
    token = message.text.strip()
    group_data["admin_settings"]["xrocket_token"] = token
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, "Токен xRocket установлен." if language == "ru" else "xRocket token set.", reply_markup=await admin_settings_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Токен xRocket установлен для группы {group_id} пользователем {user_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_settings_current:"))
async def admin_settings_current_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы." if language == "ru" else "You are not an admin of this group.")
        return
    settings = group_data["admin_settings"]
    text = (
        f"xRocket Token: {settings['xrocket_token']}\n"
        f"Holders Only: {settings['holders_only']}\n"
        f"CA: {settings['ca']}\n"
        f"Min Coins: {settings['min_coins']}\n"
        f"Prize Amount: {settings['prize_amount']}\n"
        f"Custom Prize: {settings['custom_prize']}"
    )
    await call.message.edit_text(text, reply_markup=await admin_settings_menu_keyboard(group_id, language))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_add_admin:"))
async def admin_add_admin_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"]:
        await call.answer("Только основной администратор может добавлять админов." if language == "ru" else "Only primary admin can add admins.")
        return
    await send_message_safe(call.message.chat.id, "Введите @username нового администратора:" if language == "ru" else "Enter @username of the new admin:")
    await state.set_state(Form.waiting_add_admin)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_add_admin)
async def process_add_admin(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    group_id = data["group_id"]
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"]:
        await send_message_safe(message.chat.id, "Только основной администратор может добавлять админов." if language == "ru" else "Only primary admin can add admins.")
        await state.clear()
        return
    
    username = message.text.strip().lstrip("@")
    async with async_session() as session:
        try:
            result = await session.execute("SELECT user_id FROM users WHERE username = :username", {"username": username})
            new_admin_id = result.scalar()
        except Exception as e:
            logging.error(f"Ошибка поиска нового администратора: {e}")
            new_admin_id = None
    
    if not new_admin_id:
        await send_message_safe(message.chat.id, "Пользователь не найден." if language == "ru" else "User not found.")
        await state.clear()
        return
    
    if new_admin_id in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Этот пользователь уже администратор." if language == "ru" else "This user is already an admin.")
        await state.clear()
        return
    
    group_data["extra_admins"].append(new_admin_id)
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, f"@{username} добавлен как администратор." if language == "ru" else f"@{username} added as admin.", reply_markup=await admin_main_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {new_admin_id} добавлен как админ в группу {group_id} пользователем {user_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_remove_admin:"))
async def admin_remove_admin_handler(call: CallbackQuery, state: FSMContext):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"]:
        await call.answer("Только основной администратор может удалять админов." if language == "ru" else "Only primary admin can remove admins.")
        return
    await send_message_safe(call.message.chat.id, "Введите @username администратора для удаления:" if language == "ru" else "Enter @username of the admin to remove:")
    await state.set_state(Form.waiting_remove_admin)
    await state.update_data(group_id=group_id)
    await call.answer()

@dp.message(Form.waiting_remove_admin)
async def process_remove_admin(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    group_id = data["group_id"]
    group_data = await get_group_data(group_id)
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"]:
        await send_message_safe(message.chat.id, "Только основной администратор может удалять админов." if language == "ru" else "Only primary admin can remove admins.")
        await state.clear()
        return
    
    username = message.text.strip().lstrip("@")
    async with async_session() as session:
        try:
            result = await session.execute("SELECT user_id FROM users WHERE username = :username", {"username": username})
            admin_id = result.scalar()
        except Exception as e:
            logging.error(f"Ошибка поиска администратора для удаления: {e}")
            admin_id = None
    
    if not admin_id or admin_id not in group_data["extra_admins"]:
        await send_message_safe(message.chat.id, "Этот пользователь не администратор." if language == "ru" else "This user is not an admin.")
        await state.clear()
        return
    
    group_data["extra_admins"] = [aid for aid in group_data["extra_admins"] if aid != admin_id]
    await save_group_data(group_id, group_data)
    await send_message_safe(message.chat.id, f"@{username} удален из администраторов." if language == "ru" else f"@{username} removed from admins.", reply_markup=await admin_main_menu_keyboard(group_id, language))
    await state.clear()
    logging.info(f"Пользователь {admin_id} удален из админов группы {group_id} пользователем {user_id}")

@dp.callback_query(lambda c: c.data.startswith("+bank"))
async def bank_handler(message: Message):
    if message.chat.type not in ["group", "supergroup"]:
        return
    
    group_id = message.chat.id
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await send_message_safe(group_id, TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    
    if group_data.get("game_status", False):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_already_running"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) попытался запустить банк, но другая игра уже идет в группе {group_id}")
        return
    
    if not check_game_settings(group_data):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_settings_missing"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) попытался запустить банк без настроек в группе {group_id}")
        return
    
    parts = message.text.split()
    if len(parts) != 4 or parts[0] != "+bank":
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_format"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел неверный формат команды: {message.text}")
        return
    
    game_type, amount_str, currency = parts[1:]
    game_types = {"slot": "🎰", "dice": "🎲", "basketball": "🏀", "football": "⚽", "bowling": "🎳", "darts": "🎯"}
    if game_type not in game_types:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_wrong_type"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал неверный тип игры: {game_type}")
        return
    
    if currency not in ["TON", "USDT"]:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_wrong_currency"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал неверную валюту: {currency}")
        return
    
    try:
        amount = float(amount_str.replace(",", "."))
        if amount <= 0:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_invalid"])
            logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал отрицательную или нулевую сумму: {amount_str}")
            return
        min_amount = MIN_BANK_AMOUNT_TON if currency == "TON" else MIN_BANK_AMOUNT_USDT
        if amount < min_amount:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_too_low"].format(min_amount=min_amount, currency=currency))
            logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал сумму ниже минимума: {amount} {currency}")
            return
        if amount > MAX_AMOUNT:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_too_high"].format(max_amount=MAX_AMOUNT, currency=currency))
            logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал сумму выше максимума: {amount} {currency}")
            return
    except ValueError:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_invalid"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел некорректную сумму: {amount_str}")
        return
    
    group_data["game_status"] = True
    group_data["active_game"] = "bank"
    invoice = await create_xrocket_invoice(group_id, amount, currency, f"Общий банк для {game_type}" if language == "ru" else f"Common bank for {game_type}")
    if not invoice:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_invoice_error"])
        group_data["game_status"] = False
        group_data["active_game"] = None
        await save_group_data(group_id, group_data)
        logging.error(f"Не удалось создать инвойс для банка в группе {group_id} пользователем {user_id}")
        return
    
    group_data["game_data"] = {
        "start_time": int(time.time()),
        "amount": amount,
        "currency": currency,
        "game_type": game_type,
        "emoji": game_types[game_type],
        "participants": {},
        "best_result": {"value": 0, "user_id": None, "timestamp": 0},
        "attempts": {},
        "invoice_id": invoice["id"]
    }
    await save_group_data(group_id, group_data)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Оплатить" if language == "ru" else "Pay", url=invoice["url"])
    kb.button(text="Участвую" if language == "ru" else "Participate", callback_data=f"bank_participate:{group_id}")
    await send_message_safe(group_id, TRANSLATIONS[language]["bank_started"].format(amount=amount, currency=currency, game_type=game_type), reply_markup=kb.as_markup())
    
    await send_ad_if_enabled(group_id)
    
    logging.info(f"Пользователь {user_id} (username: {message.from_user.username}) запустил общий банк в группе {group_id} с суммой {amount} {currency}, invoice_id={invoice['id']}")
    asyncio.create_task(schedule_game_task(group_id, GAME_TIMEOUT, process_bank_game))

@dp.callback_query(lambda c: c.data.startswith("bank_participate:"))
async def bank_participate_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await call.answer(TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    if group_data.get("active_game") != "bank" or not group_data.get("game_status"):
        await call.answer("Игра недоступна." if language == "ru" else "Game unavailable.")
        logging.warning(f"Пользователь {user_id} попытался участвовать в недоступном банке в группе {group_id}")
        return
    
    game_data = group_data["game_data"]
    game_data["participants"][str(user_id)] = True
    game_data["attempts"][str(user_id)] = 0
    group_data["game_data"] = game_data
    await save_group_data(group_id, group_data)
    
    await send_message_safe(call.message.chat.id, TRANSLATIONS[language]["bank_participate"])
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) присоединился к общему банку в группе {group_id}")

async def process_bank_game(group_id: int):
    group_data = await get_group_data(group_id)
    if group_data.get("active_game") != "bank" or not group_data.get("game_status"):
        logging.info(f"Процесс банка пропущен для группы {group_id} - игра не активна или неверный тип")
        return
    
    game_data = group_data["game_data"]
    language = await get_user_language(int(game_data.get("best_result", {}).get("user_id", user_id)) if game_data.get("best_result", {}).get("user_id") else user_id)
    
    invoice_status = await check_invoice_status(game_data["invoice_id"])
    if not invoice_status["paid"]:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_not_collected"])
        group_data["game_status"] = False
        group_data["active_game"] = None
        await save_group_data(group_id, group_data)
        logging.info(f"Банк в группе {group_id} отменен - инвойс не оплачен")
        return
    
    total_collected = invoice_status["amount"]
    game_data["amount"] = total_collected
    await send_message_safe(group_id, TRANSLATIONS[language]["bank_collected"].format(total=total_collected, currency=game_data["currency"], emoji=game_data["emoji"]))
    await save_group_data(group_id, group_data)
    logging.info(f"Сбор банка завершен в группе {group_id} с суммой {total_collected} {game_data['currency']}")
    
    await asyncio.sleep(THROW_TIMEOUT)
    
    if not game_data["participants"]:
        xrocket_fee = total_collected * XROCKET_FEE
        bot_fee = max(0.1 if game_data["currency"] == "TON" else 0.5, (total_collected - xrocket_fee) * group_data["admin_settings"]["bot_fee"])
        refund = total_collected - xrocket_fee - bot_fee if total_collected >= xrocket_fee + bot_fee else total_collected - xrocket_fee
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_no_participants"].format(refund=refund))
        log_event(group_id, "Возврат", f"Возвращено {refund:.3f} {game_data['currency']} участникам")
    elif game_data["best_result"]["user_id"]:
        winner_id = int(game_data["best_result"]["user_id"])
        winner_data = await get_user_data(winner_id)
        winner_username = winner_data.get("username", "Пользователь" if language == "ru" else "User")
        xrocket_fee = total_collected * XROCKET_FEE
        bot_fee = max(0.1 if game_data["currency"] == "TON" else 0.5, (total_collected - xrocket_fee) * group_data["admin_settings"]["bot_fee"])
        prize = total_collected - xrocket_fee - bot_fee
        success = await send_xrocket_transfer(group_id, winner_id, prize, game_data["currency"], "Bank game prize")
        if success:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_winner"].format(
                winner=await format_user_mention(winner_id, winner_username),
                value=game_data["best_result"]["value"],
                total=total_collected,
                xrocket_fee=xrocket_fee,
                bot_fee=bot_fee,
                prize=prize,
                currency=game_data["currency"]
            ))
            if group_data["admin_settings"]["tickets_enabled"]:
                winner_data["tickets"] = winner_data.get("tickets", 0) + 1
                await save_user_data(winner_id, winner_data)
                logging.info(f"Пользователь {winner_id} получил билет за победу в банке в группе {group_id}")
            logging.info(f"Банк в группе {group_id} выиграл {winner_id} с призом {prize} {game_data['currency']}")
        else:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_transfer_error"])
            for admin_id in group_data["primary_admins"] + group_data["extra_admins"]:
                await send_message_safe(admin_id, "Ошибка при выдаче приза в общем банке." if language == "ru" else "Error issuing Bank game prize.")
            logging.error(f"Не удалось перевести приз для банка в группе {group_id}")
    
    for uid, attempts in game_data["attempts"].items():
        if attempts > group_data["admin_settings"]["attempts_limit"]:
            try:
                await bot.restrict_chat_member(group_id, int(uid), types.ChatPermissions(can_send_messages=True))
                logging.info(f"Снят мут с пользователя {uid} в группе {group_id} после банка")
            except Exception as e:
                logging.error(f"Не удалось снять мут с пользователя {uid} в группе {group_id}: {e}")
    
    group_data["game_status"] = False
    group_data["active_game"] = None
    group_data["game_data"] = {}
    await save_group_data(group_id, group_data)
    logging.info(f"Процесс банка завершен для группы {group_id}")

@dp.message(Dice)
async def handle_dice(message: Message):
    if message.chat.type not in ["group", "supergroup"] or message.forward_from:
        return
    
    group_id = message.chat.id
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False) or not group_data.get("game_status"):
        return
    
    language = await get_user_language(user_id)
    game_type = group_data.get("active_game")
    game_data = group_data.get("game_data", {})
    
    DICE_VALUES = {
        '🎰': {'win': [64], 'semi': [1, 22, 43]},
        '🎲': {'win': [6], 'semi': []},
        '🏀': {'win': [5], 'semi': [4]},
        '⚽': {'win': [5], 'semi': [4]},
        '🎳': {'win': [6], 'semi': []},
        '🎯': {'win': [6], 'semi': []}
    }
    
    if game_type == "emoji":
        if str(user_id) not in game_data.get("participants", {}):
            return
        
        emoji = message.dice.emoji
        if emoji != group_data["emoji_selection"]:
            return
        
        attempts = game_data["attempts"].get(str(user_id), 0)
        max_attempts = group_data["admin_settings"]["attempts_limit"]
        if attempts >= max_attempts:
            try:
                await bot.restrict_chat_member(group_id, user_id, types.ChatPermissions(), until_date=int(time.time()) + 3600)
                await send_message_safe(group_id, TRANSLATIONS[language]["attempts_exceeded"].format(username=message.from_user.username, max_attempts=max_attempts))
                logging.info(f"Пользователь {user_id} замучен за превышение попыток ({max_attempts}) в Emoji игре в группе {group_id}")
            except Exception as e:
                logging.error(f"Не удалось замутить пользователя {user_id} в группе {group_id}: {e}")
            return
        
        game_data["attempts"][str(user_id)] = attempts + 1
        if "message_ids" not in game_data:
            game_data["message_ids"] = []
        game_data["message_ids"].append(message.message_id)
        value = message.dice.value
        
        win_values = DICE_VALUES[emoji]["win"]
        if group_data["admin_settings"]["semi_win"]:
            win_values += DICE_VALUES[emoji]["semi"]
        
        if value in win_values:
            current_best = game_data["best_result"]
            if value > current_best["value"] or (value == current_best["value"] and message.date.timestamp() < current_best["timestamp"]):
                if group_data["admin_settings"]["holders_only"]:
                    user_data = await get_user_data(user_id)
                    wallet = user_data.get("wallet_address")
                    if not wallet:
                        return
                    balance = await get_jetton_balance(wallet, group_data["admin_settings"]["ca"])
                    if balance < group_data["admin_settings"]["min_coins"]:
                        return
                game_data["best_result"] = {"value": value, "user_id": str(user_id), "timestamp": message.date.timestamp()}
                group_data["game_data"] = game_data
                await save_group_data(group_id, group_data)
                logging.info(f"Пользователь {user_id} бросил {emoji} со значением {value} в Emoji игре в группе {group_id}, обновлен лучший результат")
    
    elif game_type == "bank":
        if str(user_id) not in game_data["participants"]:
            return
        
        if message.dice.emoji != game_data["emoji"]:
            return
        
        attempts = game_data["attempts"].get(str(user_id), 0)
        max_attempts = group_data["admin_settings"]["attempts_limit"]
        if attempts >= max_attempts:
            try:
                await bot.restrict_chat_member(group_id, user_id, types.ChatPermissions(), until_date=int(time.time()) + 3600)
                await send_message_safe(group_id, TRANSLATIONS[language]["attempts_exceeded"].format(username=message.from_user.username, max_attempts=max_attempts))
                logging.info(f"Пользователь {user_id} замучен за превышение попыток ({max_attempts}) в банке в группе {group_id}")
            except Exception as e:
                logging.error(f"Не удалось замутить пользователя {user_id} в группе {group_id}: {e}")
            return
        
        game_data["attempts"][str(user_id)] = attempts + 1
        value = message.dice.value
        
        win_values = DICE_VALUES[game_data["emoji"]]["win"]
        if value in win_values:
            current_best = game_data["best_result"]
            if value > current_best["value"] or (value == current_best["value"] and message.date.timestamp() < current_best["timestamp"]):
                game_data["best_result"] = {"value": value, "user_id": str(user_id), "timestamp": message.date.timestamp()}
                group_data["game_data"] = game_data
                await save_group_data(group_id, group_data)
                logging.info(f"Пользователь {user_id} бросил {game_data['emoji']} со значением {value} в банке в группе {group_id}, обновлен лучший результат")
    
    elif game_type == "pvp":
        if str(user_id) not in [game_data["initiator"], game_data["opponent"]]:
            return
        
        if not (game_data["initiator_paid"] and game_data["opponent_paid"]):
            return
        
        if message.dice.emoji != "🎲":
            return
        
        score_list = game_data["initiator_score"] if str(user_id) == game_data["initiator"] else game_data["opponent_score"]
        if len(score_list) < 3:
            score_list.append(message.dice.value)
            group_data["game_data"] = game_data
            await save_group_data(group_id, group_data)
            logging.info(f"Пользователь {user_id} добавил значение {message.dice.value} в PVP в группе {group_id}")
        elif len(score_list) >= 3:
            try:
                await bot.restrict_chat_member(group_id, user_id, types.ChatPermissions(), until_date=int(time.time()) + 3600)
                await send_message_safe(group_id, TRANSLATIONS[language]["attempts_exceeded"].format(username=message.from_user.username, max_attempts=3))
                logging.info(f"Пользователь {user_id} замучен за превышение 3 попыток в PVP в группе {group_id}")
            except Exception as e:
                logging.error(f"Не удалось замутить пользователя {user_id} в группе {group_id}: {e}")


@dp.message(lambda message: message.text.startswith("+pvp"))
async def pvp_handler(message: Message):
    if message.chat.type not in ["group", "supergroup"]:
        return
    
    group_id = message.chat.id
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await send_message_safe(group_id, TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    
    if group_data.get("game_status", False):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_already_running"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) попытался запустить PVP, но другая игра уже идет в группе {group_id}")
        return
    
    if not check_game_settings(group_data):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_settings_missing"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) попытался запустить PVP без настроек в группе {group_id}")
        return
    
    parts = message.text.split()
    if len(parts) != 4 or parts[0] != "+pvp" or not parts[1].startswith("@"):
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_format_error"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел неверный формат команды PVP: {message.text}")
        return
    
    opponent_username, amount_str, currency = parts[1:]
    if currency not in ["TON", "USDT"]:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_wrong_currency"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал неверную валюту для PVP: {currency}")
        return
    
    try:
        amount = float(amount_str.replace(",", "."))
        min_amount = MIN_BANK_AMOUNT_TON if currency == "TON" else MIN_BANK_AMOUNT_USDT
        if amount < min_amount:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_too_low"].format(min_amount=min_amount, currency=currency))
            logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал сумму ниже минимума для PVP: {amount} {currency}")
            return
        if amount > MAX_AMOUNT:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_too_high"].format(max_amount=MAX_AMOUNT, currency=currency))
            logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал сумму выше максимума для PVP: {amount} {currency}")
            return
    except ValueError:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_amount_invalid"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел некорректную сумму для PVP: {amount_str}")
        return
    
    opponent_id = None
    async with async_session() as session:
        try:
            result = await session.execute("SELECT user_id FROM users WHERE username = :username", {"username": opponent_username.lstrip("@")})
            opponent_id = result.scalar()
        except Exception as e:
            logging.error(f"Ошибка поиска оппонента для PVP: {e}")
    
    if not opponent_id:
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_user_not_found"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал несуществующего оппонента для PVP: {opponent_username}")
        return
    
    if str(user_id) == str(opponent_id):
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_self_challenge"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) попытался вызвать себя на PVP в группе {group_id}")
        return
    
    group_data["game_status"] = True
    group_data["active_game"] = "pvp"
    group_data["game_data"] = {
        "initiator": str(user_id),
        "opponent": str(opponent_id),
        "amount": amount,
        "currency": currency,
        "start_time": int(time.time()),
        "initiator_paid": False,
        "opponent_paid": False,
        "initiator_score": [],
        "opponent_score": []
    }
    await save_group_data(group_id, group_data)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Принять вызов" if language == "ru" else "Accept challenge", callback_data=f"pvp_accept:{group_id}")
    kb.button(text="Отменить PVP" if language == "ru" else "Cancel PVP", callback_data=f"pvp_cancel:{group_id}")
    await send_message_safe(group_id, f"@{opponent_username}, вас вызвали на PVP! Ставка: {amount} {currency}. У каждого по 3 броска кубика. Принять?" if language == "ru" else f"@{opponent_username}, you’ve been challenged to PVP! Stake: {amount} {currency}. 3 dice throws each. Accept?", reply_markup=kb.as_markup())
    
    await send_ad_if_enabled(group_id)
    
    logging.info(f"Пользователь {user_id} (username: {message.from_user.username}) вызвал {opponent_id} (username: {opponent_username}) на PVP в группе {group_id} с суммой {amount} {currency}")
    asyncio.create_task(schedule_game_task(group_id, GAME_TIMEOUT, process_pvp_accept_timeout))

@dp.callback_query(lambda c: c.data.startswith("pvp_accept:"))
async def pvp_accept_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await call.answer(TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    if group_data.get("active_game") != "pvp" or str(user_id) != group_data["game_data"]["opponent"]:
        await call.answer("Вы не можете принять этот вызов." if language == "ru" else "You cannot accept this challenge.")
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался принять недоступный вызов PVP в группе {group_id}")
        return
    
    game_data = group_data["game_data"]
    initiator_invoice = await create_xrocket_invoice(group_id, game_data["amount"] / 2, game_data["currency"], "PVP stake (initiator)" if language == "ru" else "PVP stake (initiator)")
    opponent_invoice = await create_xrocket_invoice(group_id, game_data["amount"] / 2, game_data["currency"], "PVP stake (opponent)" if language == "ru" else "PVP stake (opponent)")
    if not initiator_invoice or not opponent_invoice:
        await send_message_safe(group_id, TRANSLATIONS[language]["bank_invoice_error"])
        group_data["game_status"] = False
        group_data["active_game"] = None
        await save_group_data(group_id, group_data)
        logging.error(f"Не удалось создать инвойсы для PVP в группе {group_id}")
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Оплатить (инициатор)" if language == "ru" else "Pay (initiator)", url=initiator_invoice["url"])
    kb.button(text="Оплатить (оппонент)" if language == "ru" else "Pay (opponent)", url=opponent_invoice["url"])
    await send_message_safe(group_id, TRANSLATIONS[language]["pvp_accepted"].format(amount=game_data["amount"]/2, currency=game_data["currency"]), reply_markup=kb.as_markup())
    
    game_data["initiator_invoice_id"] = initiator_invoice["id"]
    game_data["opponent_invoice_id"] = opponent_invoice["id"]
    group_data["game_data"] = game_data
    await save_group_data(group_id, group_data)
    
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) принял вызов PVP в группе {group_id}")
    await call.answer()
    asyncio.create_task(schedule_game_task(group_id, GAME_TIMEOUT, process_pvp_payment))

async def process_pvp_accept_timeout(group_id: int):
    group_data = await get_group_data(group_id)
    if group_data.get("active_game") != "pvp" or not group_data.get("game_status"):
        logging.info(f"Процесс таймаута PVP пропущен для группы {group_id} - игра не активна или неверный тип")
        return
    
    game_data = group_data["game_data"]
    language = await get_user_language(int(game_data["initiator"]))
    if not (game_data["initiator_paid"] or game_data["opponent_paid"]):
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_not_paid"])
        group_data["game_status"] = False
        group_data["active_game"] = None
        await save_group_data(group_id, group_data)
        logging.info(f"PVP в группе {group_id} отменен из-за таймаута принятия")
    return

async def process_pvp_payment(group_id: int):
    group_data = await get_group_data(group_id)
    if group_data.get("active_game") != "pvp" or not group_data.get("game_status"):
        logging.info(f"Процесс оплаты PVP пропущен для группы {group_id} - игра не активна или неверный тип")
        return
    
    game_data = group_data["game_data"]
    language = await get_user_language(int(game_data["initiator"]))
    
    initiator_status = await check_invoice_status(game_data["initiator_invoice_id"])
    opponent_status = await check_invoice_status(game_data["opponent_invoice_id"])
    game_data["initiator_paid"] = initiator_status["paid"]
    game_data["opponent_paid"] = opponent_status["paid"]
    
    if not (game_data["initiator_paid"] and game_data["opponent_paid"]):
        if game_data["initiator_paid"]:
            await send_message_safe(group_id, "PVP отменено: оппонент не оплатил. Инициатору возвращено после комиссии xRocket." if language == "ru" else "PVP canceled: opponent didn’t pay. Initiator refunded after xRocket fee.")
            log_event(group_id, "Возврат", f"Возвращено {game_data['amount']/2 - (game_data['amount']/2 * XROCKET_FEE):.3f} {game_data['currency']} инициатору")
        elif game_data["opponent_paid"]:
            await send_message_safe(group_id, "PVP отменено: инициатор не оплатил. Оппоненту возвращено после комиссии xRocket." if language == "ru" else "PVP canceled: initiator didn’t pay. Opponent refunded after xRocket fee.")
            log_event(group_id, "Возврат", f"Возвращено {game_data['amount']/2 - (game_data['amount']/2 * XROCKET_FEE):.3f} {game_data['currency']} оппоненту")
        else:
            await send_message_safe(group_id, "PVP отменено: никто не оплатил." if language == "ru" else "PVP canceled: no one paid.")
        group_data["game_status"] = False
        group_data["active_game"] = None
        await save_group_data(group_id, group_data)
        logging.info(f"PVP в группе {group_id} отменен из-за неполной оплаты")
        return
    
    await send_message_safe(group_id, TRANSLATIONS[language]["pvp_started"])
    group_data["game_data"] = game_data
    await save_group_data(group_id, group_data)
    logging.info(f"PVP в группе {group_id} начался после оплаты")
    asyncio.create_task(schedule_game_task(group_id, GAME_TIMEOUT, process_pvp_game))

async def process_pvp_game(group_id: int):
    group_data = await get_group_data(group_id)
    if group_data.get("active_game") != "pvp" or not group_data.get("game_status"):
        logging.info(f"Процесс игры PVP пропущен для группы {group_id} - игра не активна или неверный тип")
        return
    
    game_data = group_data["game_data"]
    language = await get_user_language(int(game_data["initiator"]))
    
    initiator_score = sum(game_data["initiator_score"])
    opponent_score = sum(game_data["opponent_score"])
    initiator_data = await get_user_data(int(game_data["initiator"]))
    opponent_data = await get_user_data(int(game_data["opponent"]))
    initiator_username = initiator_data.get("username", "Инициатор" if language == "ru" else "Initiator")
    opponent_username = opponent_data.get("username", "Оппонент" if language == "ru" else "Opponent")
    
    if len(game_data["initiator_score"]) < 3 or len(game_data["opponent_score"]) < 3:
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_incomplete"])
        logging.info(f"PVP в группе {group_id} отменен - не все броски выполнены")
    elif initiator_score == opponent_score:
        await send_message_safe(group_id, TRANSLATIONS[language]["pvp_draw"].format(score1=initiator_score, score2=opponent_score))
        game_data["initiator_score"] = []
        game_data["opponent_score"] = []
        await send_message_safe(group_id, "Кидайте 🎲 еще 3 раза!" if language == "ru" else "Throw 🎲 3 more times!")
        group_data["game_data"] = game_data
        await save_group_data(group_id, group_data)
        logging.info(f"PVP в группе {group_id} завершился ничьей, перезапуск бросков")
        asyncio.create_task(schedule_game_task(group_id, GAME_TIMEOUT, process_pvp_game))
        return
    else:
        total = game_data["amount"]
        xrocket_fee = total * XROCKET_FEE
        bot_fee = max(0.1 if game_data["currency"] == "TON" else 0.5, (total - xrocket_fee) * group_data["admin_settings"]["bot_fee"])
        prize = total - xrocket_fee - bot_fee
        winner_id = int(game_data["initiator"]) if initiator_score > opponent_score else int(game_data["opponent"])
        winner_username = initiator_username if initiator_score > opponent_score else opponent_username
        loser_score = opponent_score if initiator_score > opponent_score else initiator_score
        success = await send_xrocket_transfer(group_id, winner_id, prize, game_data["currency"], "PVP prize")
        if success:
            await send_message_safe(group_id, f"PVP завершено! Победил {await format_user_mention(winner_id, winner_username)} со счетом {max(initiator_score, opponent_score)} против {loser_score}. Полная сумма: {total} {game_data['currency']}, выигрыш после комиссий xRocket ({xrocket_fee:.3f}) и бота ({bot_fee:.3f}): {prize:.3f} {game_data['currency']}." if language == "ru" else f"PVP completed! Winner: {await format_user_mention(winner_id, winner_username)} with score {max(initiator_score, opponent_score)} vs {loser_score}. Full amount: {total} {game_data['currency']}, prize after xRocket ({xrocket_fee:.3f}) and bot ({bot_fee:.3f}) fees: {prize:.3f} {game_data['currency']}.")
            if group_data["admin_settings"]["tickets_enabled"]:
                winner_data = await get_user_data(winner_id)
                winner_data["tickets"] = winner_data.get("tickets", 0) + 1
                await save_user_data(winner_id, winner_data)
                logging.info(f"Пользователь {winner_id} получил билет за победу в PVP в группе {group_id}")
            logging.info(f"PVP в группе {group_id} выиграл {winner_id} с призом {prize} {game_data['currency']}")
        else:
            await send_message_safe(group_id, TRANSLATIONS[language]["bank_transfer_error"])
            for admin_id in group_data["primary_admins"] + group_data["extra_admins"]:
                await send_message_safe(admin_id, "Ошибка при выдаче приза в PVP." if language == "ru" else "Error issuing PVP prize.")
            logging.error(f"Не удалось перевести приз для PVP в группе {group_id}")
    
    group_data["game_status"] = False
    group_data["active_game"] = None
    group_data["game_data"] = {}
    await save_group_data(group_id, group_data)
    logging.info(f"Процесс PVP завершен для группы {group_id}")

@dp.callback_query(lambda c: c.data.startswith("pvp_cancel:"))
async def pvp_cancel_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await call.answer(TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    if group_data.get("active_game") != "pvp" or str(user_id) != group_data["game_data"]["initiator"]:
        await call.answer("Вы не можете отменить этот PVP." if language == "ru" else "You cannot cancel this PVP.")
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался отменить недоступный PVP в группе {group_id}")
        return
    
    if group_data["game_data"]["initiator_paid"] or group_data["game_data"]["opponent_paid"]:
        await call.answer("PVP нельзя отменить после оплаты." if language == "ru" else "PVP cannot be canceled after payment.")
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался отменить оплаченный PVP в группе {group_id}")
        return
    
    await send_message_safe(group_id, "PVP отменено инициатором." if language == "ru" else "PVP canceled by initiator.")
    group_data["game_status"] = False
    group_data["active_game"] = None
    group_data["game_data"] = {}
    await save_group_data(group_id, group_data)
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) отменил PVP в группе {group_id}")

@dp.callback_query(lambda c: c.data.startswith("admin_games_wheel:"))
async def wheel_handler(call: CallbackQuery):
    group_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    group_data = await get_group_data(group_id)
    if group_data.get("blocked", False):
        await call.answer(TRANSLATIONS[await get_user_language(user_id)]["group_blocked"])
        return
    
    language = await get_user_language(user_id)
    if user_id not in group_data["primary_admins"] and user_id not in group_data["extra_admins"]:
        await call.answer("Вы не администратор этой группы." if language == "ru" else "You are not an admin of this group.")
        return
    
    if group_data["game_status"]:
        await send_message_safe(group_id, TRANSLATIONS[language]["game_already_running"])
        await call.answer()
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался запустить колесо, но другая игра уже идет в группе {group_id}")
        return
    
    if not check_game_settings(group_data):
        await send_message_safe(group_id, TRANSLATIONS[language]["game_settings_missing"])
        await call.answer()
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался запустить колесо без настроек в группе {group_id}")
        return
    
    async with async_session() as session:
        try:
            participants = await session.execute("SELECT user_id, tickets FROM users WHERE tickets > 0")
            participants = {str(row[0]): row[1] for row in participants.fetchall()}
        except Exception as e:
            logging.error(f"Ошибка получения участников для колеса фортуны: {e}")
            participants = {}
    
    if not participants:
        await send_message_safe(group_id, "Нет участников с билетиками." if language == "ru" else "No participants with tickets.")
        await call.answer()
        logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) попытался запустить колесо без участников в группе {group_id}")
        return
    
    wheel = FortuneWheel(participants)
    winner_id, gif_path = wheel.spin()
    winner_data = await get_user_data(int(winner_id))
    try:
        with open(gif_path, "rb") as gif:
            await bot.send_document(group_id, gif, caption=f"Билетики завершены! Победитель: {await format_user_mention(int(winner_id), winner_data['username'])}. Поздравляем!" if language == "ru" else f"Tickets completed! Winner: {await format_user_mention(int(winner_id), winner_data['username'])}. Congratulations!", filename="wheel.gif")
        async with async_session() as session:
            await session.execute("UPDATE users SET tickets = 0 WHERE tickets > 0")
            await session.commit()
        logging.info(f"Колесо фортуны в группе {group_id} выиграл {winner_id} (username: {winner_data['username']})")
    except Exception as e:
        logging.error(f"Ошибка отправки GIF колеса в группе {group_id}: {e}")
    finally:
        if os.path.exists(gif_path):
            try:
                os.remove(gif_path)
                logging.debug(f"Удален временный файл GIF колеса для группы {group_id}")
            except Exception as e:
                logging.error(f"Не удалось удалить временный файл GIF для группы {group_id}: {e}")
    
    await send_ad_if_enabled(group_id)
    await call.answer()

async def check_tickets():
    while True:
        async with async_session() as session:
            try:
                groups = await session.execute("SELECT group_id, admin_settings FROM groups WHERE admin_settings->>'tickets_enabled' = 'true'")
                groups = groups.fetchall()
            except Exception as e:
                logging.error(f"Ошибка получения групп для проверки билетов: {e}")
                groups = []
        
        for group_id, admin_settings in groups:
            tickets_start_time = admin_settings.get("tickets_start_time", 0)
            if int(time.time()) - tickets_start_time >= 24 * 3600:
                participants = {}
                async with async_session() as session:
                    try:
                        result = await session.execute("SELECT user_id, tickets FROM users WHERE tickets > 0")
                        participants = {str(row[0]): row[1] for row in result.fetchall()}
                    except Exception as e:
                        logging.error(f"Ошибка получения участников для автоматического колеса: {e}")
                
                if participants:
                    wheel = FortuneWheel(participants)
                    winner_id, gif_path = wheel.spin()
                    winner_data = await get_user_data(int(winner_id))
                    language = winner_data.get("language", "ru")
                    try:
                        with open(gif_path, "rb") as gif:
                            await bot.send_document(group_id, gif, caption=f"Билетики завершены! Победитель: {await format_user_mention(int(winner_id), winner_data['username'])}. Поздравляем!" if language == "ru" else f"Tickets completed! Winner: {await format_user_mention(int(winner_id), winner_data['username'])}. Congratulations!", filename="wheel.gif")
                        async with async_session() as session:
                            await session.execute("UPDATE users SET tickets = 0 WHERE tickets > 0")
                            await session.commit()
                        logging.info(f"Автоматическое колесо фортуны в группе {group_id} выиграл {winner_id} (username: {winner_data['username']})")
                    except Exception as e:
                        logging.error(f"Ошибка отправки GIF автоматического колеса в группе {group_id}: {e}")
                    finally:
                        if os.path.exists(gif_path):
                            try:
                                os.remove(gif_path)
                                logging.debug(f"Удален временный файл GIF колеса для группы {group_id}")
                            except Exception as e:
                                logging.error(f"Не удалось удалить временный файл GIF для группы {group_id}: {e}")
                    
                    group_data = await get_group_data(group_id)
                    group_data["admin_settings"]["tickets_start_time"] = int(time.time())
                    await save_group_data(group_id, group_data)
        
        await asyncio.sleep(60)

@dp.callback_query(lambda c: c.data == "user_connect_wallet")
async def connect_wallet_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    connector = get_connector(user_id)
    await connector.restore_connection()
    
    user_data = await get_user_data(user_id)
    if connector.connected and user_data.get("wallet_connected", False):
        await call.message.edit_text("Кошелек уже подключен. Сначала отключите текущий кошелек." if language == "ru" else "Wallet already connected. Disconnect the current wallet first.", reply_markup=await user_menu_keyboard(language))
        await call.answer()
        logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) попытался подключить уже подключенный кошелек")
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Tonkeeper", callback_data="connect_wallet:Tonkeeper")
    kb.button(text="Другие" if language == "ru" else "Others", callback_data="other_wallets")
    kb.adjust(1)
    await call.message.edit_text("Выберите кошелек:" if language == "ru" else "Select wallet:", reply_markup=kb.as_markup())
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) открыл меню подключения кошелька")

@dp.callback_query(lambda c: c.data == "other_wallets")
async def other_wallets_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    connector = get_connector(user_id)
    wallets = connector.get_wallets()
    language = await get_user_language(user_id)
    kb = InlineKeyboardBuilder()
    for wallet in wallets:
        if wallet["name"] != "Tonkeeper":
            kb.button(text=wallet["name"], callback_data=f"connect_wallet:{wallet['name']}")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data="user_connect_wallet")
    kb.adjust(2)
    await call.message.edit_text("Выберите другой кошелек:" if language == "ru" else "Select another wallet:", reply_markup=kb.as_markup())
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) открыл меню других кошельков")

@dp.callback_query(lambda c: c.data.startswith("connect_wallet:"))
async def connect_wallet_option_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    connector = get_connector(user_id)
    await connector.restore_connection()
    
    user_data = await get_user_data(user_id)
    if connector.connected and user_data.get("wallet_connected", False):
        await call.message.edit_text("Кошелек уже подключен. Сначала отключите текущий кошелек." if language == "ru" else "Wallet already connected. Disconnect the current wallet first.", reply_markup=await user_menu_keyboard(language))
        await call.answer()
        return
    
    wallet_name = call.data.split(":")[1]
    wallet = next((w for w in connector.get_wallets() if w["name"] == wallet_name), None)
    if not wallet:
        await call.message.edit_text("Неизвестный кошелек." if language == "ru" else "Unknown wallet.")
        await call.answer()
        return
    
    url = await connector.connect(wallet)
    key = f"{user_id}_{wallet_name}"
    url_storage[key] = url
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Подключить" if language == "ru" else "Connect", url=url)
    kb.button(text="Показать QR" if language == "ru" else "Show QR", callback_data=f"show_qr:{wallet_name}")
    back_callback = "other_wallets" if wallet_name != "Tonkeeper" else "user_connect_wallet"
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=back_callback)
    kb.adjust(2)
    await call.message.edit_text(f"Выберите способ подключения {wallet_name}:" if language == "ru" else f"Select connection method for {wallet_name}:", reply_markup=kb.as_markup())
    await call.answer()
    
    timeout = 180
    start_time = time.time()
    while time.time() - start_time < timeout:
        if connector.connected and connector.account and connector.account.address:
            addr = Address(connector.account.address).to_str(is_bounceable=False)
            user_data["wallet_address"] = addr
            user_data["wallet_connected"] = True
            await save_user_data(user_id, user_data)
            kb_success = InlineKeyboardBuilder()
            kb_success.button(text="Отключить кошелек" if language == "ru" else "Disconnect wallet", callback_data="user_disconnect_wallet")
            kb_success.adjust(1)
            try:
                await call.message.edit_text(f"Подключен адрес: <code>{addr}</code>" if language == "ru" else f"Connected address: <code>{addr}</code>", reply_markup=kb_success.as_markup())
            except TelegramBadRequest:
                await send_message_safe(user_id, f"Подключен адрес: <code>{addr}</code>" if language == "ru" else f"Connected address: <code>{addr}</code>", reply_markup=kb_success.as_markup())
            await send_message_safe(user_id, TRANSLATIONS[language]["welcome_user"], reply_markup=await user_menu_keyboard(language))
            del url_storage[key]
            logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) подключил кошелек с адресом {addr}")
            return
        await asyncio.sleep(1)
    
    try:
        await call.message.edit_text("Время подключения истекло. Попробуйте снова." if language == "ru" else "Connection time expired. Try again.", reply_markup=await user_menu_keyboard(language))
    except TelegramBadRequest:
        await send_message_safe(user_id, "Время подключения истекло. Попробуйте снова." if language == "ru" else "Connection time expired. Try again.", reply_markup=await user_menu_keyboard(language))
    del url_storage[key]
    logging.info(f"Время подключения кошелька истекло для пользователя {user_id}")

@dp.callback_query(lambda c: c.data.startswith("show_qr:"))
async def show_qr_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    wallet_name = call.data.split(":")[1]
    key = f"{user_id}_{wallet_name}"
    url = url_storage.get(key)
    language = await get_user_language(user_id)
    
    if not url:
        await call.message.edit_text("Ошибка: сессия устарела." if language == "ru" else "Error: session expired.")
        logging.warning(f"Пользователь {user_id} (username: {call.from_user.username}) попытался показать QR для устаревшей сессии {wallet_name}")
        return
    
    img = qrcode.make(url)
    stream = BytesIO()
    img.save(stream)
    file = BufferedInputFile(stream.getvalue(), filename="qrcode.png")
    kb = InlineKeyboardBuilder()
    kb.button(text="Назад" if language == "ru" else "Back", callback_data=f"connect_wallet:{wallet_name}")
    await call.message.delete()
    await bot.send_photo(user_id, photo=file, caption=f"QR для {wallet_name}" if language == "ru" else f"QR for {wallet_name}", reply_markup=kb.as_markup())
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) просмотрел QR для кошелька {wallet_name}")

@dp.callback_query(lambda c: c.data == "user_disconnect_wallet")
async def disconnect_wallet_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    connector = get_connector(user_id)
    await connector.restore_connection()
    await connector.disconnect()
    
    user_data = await get_user_data(user_id)
    user_data["wallet_connected"] = False
    user_data["wallet_address"] = None
    await save_user_data(user_id, user_data)
    
    await call.message.edit_text("Кошелек отключен." if language == "ru" else "Wallet disconnected.", reply_markup=await user_menu_keyboard(language))
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) отключил кошелек")

@dp.message(lambda m: m.new_chat_members)
async def new_member_handler(message: Message):
    group_id = message.chat.id
    group_data = await get_group_data(group_id)
    if not group_data or group_data.get("blocked", False):
        return
    
    for member in message.new_chat_members:
        user_id = member.id
        user_data = await get_user_data(user_id)
        language = user_data.get("language", "ru")
        if user_data and user_data.get("status"):
            await send_message_safe(group_id, f"{await format_user_mention(user_id, user_data['username'])} зашел к нам на огонек!" if language == "ru" else f"{await format_user_mention(user_id, user_data['username'])} joined us!")
            logging.info(f"Пользователь {user_id} (username: {user_data['username']}) с статусом вступил в группу {group_id}")

@dp.my_chat_member()
async def chat_member_update_handler(update: types.ChatMemberUpdated):
    me = await bot.get_me()
    if update.new_chat_member.user.id != me.id or update.chat.type not in ["group", "supergroup"]:
        return
    
    group_id = update.chat.id
    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    username = f"@{update.chat.username}" if update.chat.username else ""
    language = await get_user_language(update.from_user.id if update.from_user else 1)
    
    try:
        if old_status == "left" and new_status == "member":
            logging.info(f"Бот добавлен в чат {group_id} {username}")
            group_data = await get_group_data(group_id)
            if not group_data:
                group_data = {
                    "group_id": group_id,
                    "group_title": update.chat.title,
                    "group_username": update.chat.username,
                    "primary_admins": [],
                    "extra_admins": []
                }
            else:
                group_data["group_title"] = update.chat.title
                group_data["group_username"] = update.chat.username
            await save_group_data(group_id, group_data)
        
        elif old_status == "member" and new_status == "administrator":
            logging.info(f"Бот стал администратором в чате {group_id} {username}")
            admins = await bot.get_chat_administrators(group_id)
            primary_admins = [
                adm.user.id for adm in admins
                if adm.status == "creator" or all([adm.can_change_info, adm.can_delete_messages, adm.can_invite_users,
                                                   adm.can_restrict_members, adm.can_pin_messages, adm.can_promote_members])
            ]
            group_data = await get_group_data(group_id)
            if not group_data:
                group_data = {
                    "group_id": group_id,
                    "group_title": update.chat.title,
                    "group_username": update.chat.username,
                    "primary_admins": primary_admins,
                    "extra_admins": []
                }
            else:
                group_data["group_title"] = update.chat.title
                group_data["group_username"] = update.chat.username
                group_data["primary_admins"] = primary_admins
            await save_group_data(group_id, group_data)
        
        elif old_status == "administrator" and new_status == "member":
            logging.info(f"Бот потерял права администратора в чате {group_id} {username}")
            group_data = await get_group_data(group_id)
            if group_data and group_data["game_status"]:
                await send_message_safe(group_id, "Игра сброшена: бот больше не администратор." if language == "ru" else "Game reset: bot is no longer an admin.")
                async with async_session() as session:
                    await session.execute("DELETE FROM groups WHERE group_id = :group_id", {"group_id": group_id})
                    await session.commit()
        
        elif (old_status == "member" or old_status == "administrator") and new_status in ["left", "kicked"]:
            logging.info(f"Бот удален или заблокирован в чате {group_id} {username}")
            group_data = await get_group_data(group_id)
            if group_data:
                if group_data["game_status"]:
                    await send_message_safe(group_id, "Игра сброшена по техническим причинам." if language == "ru" else "Game reset due to technical reasons.")
                async with async_session() as session:
                    await session.execute("DELETE FROM groups WHERE group_id = :group_id", {"group_id": group_id})
                    await session.commit()
    except Exception as e:
        logging.error(f"Ошибка в обработке статуса бота в чате {group_id}: {e}")

@dp.callback_query(lambda c: c.data == "user_status_menu")
async def status_menu_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    kb = InlineKeyboardBuilder()
    kb.button(text="Себе" if language == "ru" else "For myself", callback_data="status_self")
    kb.button(text="Другому" if language == "ru" else "For another", callback_data="status_other")
    kb.button(text="Назад" if language == "ru" else "Back", callback_data="user_menu_back")
    kb.adjust(1)
    await call.message.edit_text("Меню статуса:" if language == "ru" else "Status menu:", reply_markup=kb.as_markup())
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) открыл меню статуса")

@dp.callback_query(lambda c: c.data == "user_menu_back")
async def user_menu_back_handler(call: CallbackQuery):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    await call.message.edit_text(TRANSLATIONS[language]["welcome_user"], reply_markup=await user_menu_keyboard(language))
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) вернулся в главное меню")

@dp.callback_query(lambda c: c.data == "status_self")
async def status_self_handler(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    await send_message_safe(call.message.chat.id, "Введите текст вашего статуса (до 50 символов):" if language == "ru" else "Enter your status text (up to 50 characters):")
    await state.set_state(Form.waiting_status_self)
    await state.update_data(user_id=user_id, message_id=call.message.message_id)
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) начал установку статуса для себя")

@dp.message(Form.waiting_status_self)
async def process_status_self(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        await state.clear()
        return
    
    text = message.text.strip()
    language = await get_user_language(user_id)
    if len(text) > 50:
        await send_message_safe(message.chat.id, "Статус не должен превышать 50 символов." if language == "ru" else "Status must not exceed 50 characters.")
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел статус длиннее 50 символов")
        await state.clear()
        return
    
    data = await state.get_data()
    amount = STATUS_FEE
    invoice = await create_xrocket_invoice(user_id, amount, "TON", f"Покупка статуса для @{message.from_user.username}" if language == "ru" else f"Status purchase for @{message.from_user.username}", is_group=False)
    if not invoice:
        await send_message_safe(message.chat.id, TRANSLATIONS[language]["bank_invoice_error"])
        logging.error(f"Не удалось создать инвойс для статуса пользователя {user_id}")
        await state.clear()
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Оплатить" if language == "ru" else "Pay", url=invoice["url"])
    await send_message_safe(message.chat.id, f"Статус: '{text}'. Оплатите {amount} TON для активации." if language == "ru" else f"Status: '{text}'. Pay {amount} TON to activate.", reply_markup=kb.as_markup())
    await state.update_data(invoice_id=invoice["id"], status_text=text)
    asyncio.create_task(check_status_payment(user_id, invoice["id"], text, message.chat.id, True))
    await state.clear()
    logging.info(f"Пользователь {user_id} (username: {message.from_user.username}) инициировал покупку статуса '{text}'")

@dp.callback_query(lambda c: c.data == "status_other")
async def status_other_handler(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        return
    
    language = await get_user_language(user_id)
    await send_message_safe(call.message.chat.id, "Введите @username пользователя, которому хотите установить статус:" if language == "ru" else "Enter @username of the user you want to set a status for:")
    await state.set_state(Form.waiting_status_other_username)
    await state.update_data(user_id=user_id, message_id=call.message.message_id)
    await call.answer()
    logging.info(f"Пользователь {user_id} (username: {call.from_user.username}) начал установку статуса для другого")

@dp.message(Form.waiting_status_other_username)
async def process_status_other_username(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        await state.clear()
        return
    
    username = message.text.strip().lstrip("@")
    language = await get_user_language(user_id)
    
    async with async_session() as session:
        try:
            result = await session.execute("SELECT user_id FROM users WHERE username = :username", {"username": username})
            target_id = result.scalar()
        except Exception as e:
            logging.error(f"Ошибка поиска пользователя для статуса: {e}")
            target_id = None
    
    if not target_id:
        await send_message_safe(message.chat.id, TRANSLATIONS[language]["pvp_user_not_found"])
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) указал несуществующий юзернейм для статуса: {username}")
        await state.clear()
        return
    
    await send_message_safe(message.chat.id, "Введите текст статуса для этого пользователя (до 50 символов):" if language == "ru" else "Enter status text for this user (up to 50 characters):")
    await state.set_state(Form.waiting_status_other_text)
    await state.update_data(target_id=target_id)
    logging.info(f"Пользователь {user_id} (username: {message.from_user.username}) выбрал цель {target_id} для статуса")

@dp.message(Form.waiting_status_other_text)
async def process_status_other_text(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await is_bot_active() and user_id not in DEV_USERS:
        await state.clear()
        return
    
    text = message.text.strip()
    language = await get_user_language(user_id)
    if len(text) > 50:
        await send_message_safe(message.chat.id, "Статус не должен превышать 50 символов." if language == "ru" else "Status must not exceed 50 characters.")
        logging.warning(f"Пользователь {user_id} (username: {message.from_user.username}) ввел статус длиннее 50 символов для другого")
        await state.clear()
        return
    
    data = await state.get_data()
    target_id = data["target_id"]
    target_data = await get_user_data(target_id)
    amount = STATUS_FEE * 1.5
    invoice = await create_xrocket_invoice(user_id, amount, "TON", f"Покупка статуса для @{target_data['username']}" if language == "ru" else f"Status purchase for @{target_data['username']}", is_group=False)
    if not invoice:
        await send_message_safe(message.chat.id, TRANSLATIONS[language]["bank_invoice_error"])
        logging.error(f"Не удалось создать инвойс для статуса другого пользователя {target_id} от {user_id}")
        await state.clear()
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Оплатить" if language == "ru" else "Pay", url=invoice["url"])
    await send_message_safe(message.chat.id, f"Статус для @{target_data['username']}: '{text}'. Оплатите {amount} TON." if language == "ru" else f"Status for @{target_data['username']}: '{text}'. Pay {amount} TON.", reply_markup=kb.as_markup())
    await state.update_data(invoice_id=invoice["id"], status_text=text)
    asyncio.create_task(check_status_payment(target_id, invoice["id"], text, message.chat.id, False))
    await state.clear()
    logging.info(f"Пользователь {user_id} (username: {message.from_user.username}) инициировал покупку статуса '{text}' для {target_id}")

async def check_status_payment(target_id: int, invoice_id: str, status_text: str, chat_id: int, is_self: bool):
    timeout = GAME_TIMEOUT
    start_time = time.time()
    language = await get_user_language(target_id)
    
    while time.time() - start_time < timeout:
        status = await check_invoice_status(invoice_id)
        if status["paid"]:
            target_data = await get_user_data(target_id)
            target_data["status"] = status_text
            await save_user_data(target_id, target_data)
            logging.info(f"Статус '{status_text}' установлен для пользователя {target_id}")
            
            if is_self:
                await send_message_safe(chat_id, f"Ваш статус установлен: {status_text}" if language == "ru" else f"Your status has been set: {status_text}")
            else:
                await send_message_safe(chat_id, f"Статус для {await format_user_mention(target_id, target_data['username'])} установлен!" if language == "ru" else f"Status for {await format_user_mention(target_id, target_data['username'])} has been set!")
            
            async with async_session() as session:
                try:
                    groups = await session.execute("SELECT group_id FROM groups WHERE :target_id = ANY(primary_admins) OR :target_id = ANY(extra_admins)", {"target_id": target_id})
                    groups = [row[0] for row in groups.fetchall()]
                except Exception as e:
                    logging.error(f"Ошибка получения групп для уведомления о статусе: {e}")
                    groups = []
            
            for group_id in groups:
                try:
                    member = await bot.get_chat_member(group_id, target_id)
                    if member.status in ["member", "administrator", "creator"]:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="Приобрести тоже" if language == "ru" else "Get one too", callback_data="user_status_menu")
                        await send_message_safe(group_id, f"{await format_user_mention(target_id, target_data['username'])} приобрел статус!" if language == "ru" else f"{await format_user_mention(target_id, target_data['username'])} acquired a status!", reply_markup=kb.as_markup())
                except Exception:
                    continue
            return
        await asyncio.sleep(5)
    
    await send_message_safe(chat_id, "Оплата не выполнена вовремя. Статус не установлен." if language == "ru" else "Payment not completed in time. Status not set.")
    logging.info(f"Оплата статуса для пользователя {target_id} не выполнена вовремя")

async def check_unfinished_games():
    async with async_session() as session:
        try:
            groups = await session.execute("SELECT group_id, game_status, game_data FROM groups WHERE game_status = TRUE")
            groups = groups.fetchall()
        except Exception as e:
            logging.error(f"Ошибка проверки незавершенных игр: {e}")
            groups = []
    
    for group_id, game_status, game_data in groups:
        if game_status:
            language = await get_user_language(int(game_data.get("initiator", 1)) if "initiator" in game_data else 1)
            await send_message_safe(group_id, "Игра сброшена по техническим причинам (перезапуск бота). Перезапустите." if language == "ru" else "Game reset due to technical reasons (bot restart). Restart it.")
            group_data = await get_group_data(group_id)
            group_data["game_status"] = False
            group_data["active_game"] = None
            group_data["game_data"] = {}
            await save_group_data(group_id, group_data)
            logging.info(f"Незавершенная игра в группе {group_id} сброшена из-за перезапуска бота")

async def send_ad_if_enabled(group_id: int):
    group_data = await get_group_data(group_id)
    if not group_data.get("ad_enabled", False) or not group_data.get("ad_message"):
        return
    
    now = datetime.now(MOSCOW_TZ)
    last_reset = group_data.get("ad_last_reset")
    if last_reset and now.date() > last_reset.date():
        group_data["ad_count_today"] = 0
        group_data["ad_last_reset"] = now
        await save_group_data(group_id, group_data)
    
    if group_data["ad_count_today"] >= AD_LIMIT_PER_DAY:
        logging.info(f"Лимит рекламы исчерпан для группы {group_id}: {group_data['ad_count_today']}/{AD_LIMIT_PER_DAY}")
        return
    
    ad_message = group_data["ad_message"]
    async with async_session() as session:
        try:
            users = await session.execute("SELECT user_id FROM users")
            users = [row[0] for row in users.fetchall()]
        except Exception as e:
            logging.error(f"Ошибка получения пользователей для рекламы: {e}")
            users = []
    
    for user_id in users:
        language = await get_user_language(user_id)
        try:
            if ad_message["media"]["type"] == "photo":
                await bot.send_photo(user_id, ad_message["media"]["file_id"], caption=ad_message["text"])
            else:
                await bot.send_message(user_id, ad_message["text"])
                if ad_message["media"]["type"] == "sticker":
                    await bot.send_sticker(user_id, ad_message["media"]["file_id"])
                elif ad_message["media"]["type"] == "gif":
                    await bot.send_animation(user_id, ad_message["media"]["file_id"])
        except Exception as e:
            logging.error(f"Не удалось отправить рекламу пользователю {user_id}: {e}")
    
    group_data["ad_count_today"] += 1
    group_data["ad_last_reset"] = now
    await save_group_data(group_id, group_data)
    
    for admin_id in group_data["primary_admins"] + group_data["extra_admins"]:
        try:
            await send_message_safe(admin_id, f"Реклама запущена в группе {group_data['group_title']}. Остаток лимита на сегодня: {AD_LIMIT_PER_DAY - group_data['ad_count_today']}/{AD_LIMIT_PER_DAY}")
        except Exception as e:
            logging.error(f"Не удалось уведомить админа {admin_id} о запуске рекламы в группе {group_id}: {e}")
    
    logging.info(f"Реклама отправлена из группы {group_id}, счетчик: {group_data['ad_count_today']}/{AD_LIMIT_PER_DAY}")

async def format_user_mention(user_id: int, username: str) -> str:
    user_data = await get_user_data(user_id)
    status = user_data.get("status", "")
    if status:
        return f"{status} @{username}"
    return f"@{username}"

async def check_game_tasks():
    while True:
        for task in asyncio.all_tasks():
            if hasattr(task, "group_id"):
                group_data = await get_group_data(task.group_id)
                if not group_data["game_status"]:
                    task.cancel()
                    logging.info(f"Отменена задача-ожидание для группы {task.group_id}")
        await asyncio.sleep(60)

async def main():
    await init_db()
    await check_unfinished_games()
    
    # Установка вебхука
    await bot.set_webhook(f"{WEBHOOK_URL}{WEBHOOK_PATH}")
    logging.info(f"Вебхук установлен на {WEBHOOK_URL}{WEBHOOK_PATH}")
    
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    
    # Запуск задач фонового выполнения
    asyncio.create_task(check_game_tasks())
    asyncio.create_task(check_tickets())
    
    # Запуск веб-сервера
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_SERVER_HOST, WEB_SERVER_PORT)
    await site.start()
    logging.info(f"Сервер запущен на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("Остановка сервера...")
    finally:
        await runner.cleanup()
        await bot.session.close()

if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not XROCKET_DEV_API_KEY:
        logging.critical("Отсутствует TELEGRAM_BOT_TOKEN или XROCKET_DEV_API_KEY. Завершение работы.")
        sys.exit(1)
    
    if sys.platform == "win32":
        os.system("chcp 65001 > nul")
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    
    try:
        asyncio.run(main())
        logging.info("Бот успешно запущен")
    except Exception as e:
        logging.critical(f"Не удалось запустить бота: {e}")