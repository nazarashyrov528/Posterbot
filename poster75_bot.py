#!/usr/bin/env python3
import asyncio
import time
import json
import logging
import heapq
import itertools
import sqlite3
import os
from uuid import uuid4
from typing import Dict, Any, List, Optional, Tuple, Set

try:
    import psutil
except Exception:
    psutil = None

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

# Configuration: use environment variables for secrets
BOT_TOKEN = "8479890419:AAECDA7Idv5iEv89H-w4WdceA3CcoVvlU2o"
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable not set")

OWNER_ID = 6185683417
ADMINS = {OWNER_ID}
DB_FILE = os.getenv("DATA_DB", "data.db")

MIN_SECOND = 30
MAX_SECOND = 300
PAGE_SIZE = 10
WELCOME_GIFT = 30

# In-memory structures
user_sessions: Dict[int, Dict[str, Any]] = {}
waiting_for: Dict[int, str] = {}
scheduled_posts: List[Dict[str, Any]] = []
posts_by_id: Dict[str, Dict[str, Any]] = {}
previous_messages: Dict[Tuple[str, str], int] = {}
menu_message_id: Dict[int, int] = {}

balances: Dict[int, int] = {}
usernames: Dict[str, int] = {}
unlimited_users: Set[int] = set()

data_lock = asyncio.Lock()
file_lock = asyncio.Lock()

_dirty = False

_post_heap: List[Tuple[float, int, str]] = []
_heap_counter = itertools.count()
heap_updated = asyncio.Event()

# Less frequent autosave reduces IO/CPU overhead (kept for structure)
AUTO_SAVE_INTERVAL = 120

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STR = {
    'welcome': "👋 𝐒𝐚𝐥𝐚𝐦! 𝐌𝐞𝐧𝐲́𝐮𝐝𝐚𝐧 𝛊𝐬𝐥𝐞𝐲́𝐚𝐧 𝛊𝐬̧𝛊𝐧𝛊𝐳𝛊 𝐬𝐚𝐲́𝐥𝐚𝐧:",
    'welcome_first_time': "🎉 𝐁𝐨𝐭𝐚 𝐡𝐨𝐬̧ 𝐠𝐞𝐥𝐝𝛊𝐧! 𝐈𝐥𝐤𝛊𝐧𝐜𝛊 𝐮𝐥𝐚𝐧𝐲̧𝐬̧𝐲𝐧 𝐮𝐜𝛊𝐧 𝐬𝐞𝐧𝛊𝐧 𝐛𝐚𝐥𝐚𝐧𝐬𝐲𝐧𝐚 30 𝐣𝐞𝐭𝐨𝐧 𝐬𝐨𝐯𝐠𝐚𝐭 𝐞𝐝𝛊𝐥𝐝𝛊. 🎁",
    'no_permission': "❌ 𝐒𝛊𝐳 𝐚𝐝𝐦𝛊𝐧 𝐝𝐚𝐥.",
    'help_text': "ℹ️ 𝐁𝐮𝐲𝐫𝐮𝐤𝐥𝐚𝐫:\n/start - 𝐏𝐨𝐬𝐭 𝐔𝐠𝐫𝐚𝐭𝐦𝐚𝐤\n/help - 𝐊𝐨𝐦𝐞𝐤",
    'enter_text': "✍ 𝐏𝐨𝐬𝐭𝐲𝐧 𝐦𝐚𝐳𝐦𝐮𝐧𝐲𝐧𝐲 𝐠𝛊𝐫𝛊𝐳𝛊𝐧 ( 𝐭𝐞𝐤𝐬𝐭 𝐲𝐚 𝐝𝐚 𝐬𝐭𝛊𝐜𝐤𝐞𝐫 ):",
    'enter_second': "⏱ 𝐍𝐚𝐜̧𝐞 𝐬𝐞𝐤𝐮𝐧𝐭𝐝𝐚 𝛊𝐛𝐞𝐫𝛊𝐥𝐬𝛊𝐧? ( 𝛊𝐧 𝐚𝐳 {min} 𝐬𝐞𝐤𝐮𝐧𝐭, 𝛊𝐧 𝐤𝐨̈𝐩 {max} 𝐬𝐞𝐤𝐮𝐧𝐭 )".format(min=MIN_SECOND, max=MAX_SECOND),
    'enter_channel': "📢 𝐇𝐚𝐲𝐬𝐲 𝐤𝐚𝐧𝐚𝐥𝐚? ( @username 𝐲𝐚 𝐝𝐚 𝛊𝐝 𝐠𝐨̈𝐫𝐧𝐮̈𝐬𝛊𝐧𝐝𝐞 𝐲́𝐚𝐳𝐲𝐧 )",
    'post_added': "✅ 𝐏𝐨𝐬𝐭 {channel} 𝐤𝐚𝐧𝐚𝐥𝐲𝐧𝐚 𝐮𝐠𝐫𝐚𝐝𝐲𝐥𝐲𝐩 𝐛𝐚𝐬́𝐥𝐚𝐝𝐲.",
    'min_seconds': f"⚠️ 𝐈𝐧 𝐚𝐳 {MIN_SECOND} 𝐬𝐞𝐤𝐮𝐧𝐭 𝐛𝐨𝐥𝐦𝐚𝐥𝐲!",
    'max_seconds': f"⚠️ 𝐈𝐧 𝐤𝐨̈𝐩 {MAX_SECOND} 𝐬𝐞𝐤𝐮𝐧𝐭 𝐛𝐨𝐥𝐦𝐚𝐥𝐲!",
    'not_number': "⚠️ 𝐒𝐚𝐧 𝐠𝛊𝐫𝛊𝐳𝛊𝐧!",
    'invalid_channel': "⚠️ 𝐁𝐞𝐲́𝐥𝐞 𝐤𝐚𝐧𝐚𝐥 𝐲́𝐨𝐤 𝐲𝐚 𝐝𝐚 𝐛𝐨𝐭 𝐤𝐚𝐧𝐚𝐥𝐝𝐚 𝐚𝐝𝐦𝛊𝐧 𝐝𝐚̂𝐥 ‼️",
    'post_none': "📭 𝐒𝛊𝐳𝐝𝐞 𝐡𝐚̂𝐳𝛊𝐫 𝐚𝐜𝐭𝛊𝐯 𝐩𝐨𝐬𝐭 𝐲́𝐨𝐤.",
    'stats_header': "📊 𝐒𝐭𝐚𝐭𝛊𝐬𝐭𝛊𝐤𝐚 (jemi {total}) — 𝐒𝐚𝐡𝐲𝐩𝐚 {page}/{pages}:\n\n{list}",
    'post_deleted': "✅ 𝐏𝐨𝐬𝐭 𝐨̈𝐜̧𝐮̈𝐫𝛊𝐥𝐝𝛊.",
    'post_toggled': "🔄 𝐏𝐨𝐬𝐭𝐮𝐧 𝐲́𝐚𝐠̆𝐝𝐚𝐲́𝐲 𝐮̈𝐲́𝐭𝐠𝐞𝐝𝛊𝐥𝐝𝛊.",
    'edit_prompt': "📝 𝐓𝐚̂𝐳𝐞 𝐭𝐞𝐤𝐬𝐭𝛊 𝐲́𝐚 𝐳𝐚 𝐬𝐚𝐧 𝐠𝛊𝐫𝛊𝐳𝛊𝐩 𝐰𝐚𝐠̆𝐭𝐲 𝐮̈𝐲́𝐭𝐠𝐞𝐝𝛊𝐧 ( 𝛊𝐧 𝐚𝐳 {min} — 𝛊𝐧 𝐤𝐨̈𝐩 {max}).".format(min=MIN_SECOND, max=MAX_SECOND),
    'duration_updated': "✅ 𝐖𝐚𝐠𝐭 𝐭𝐚̂𝐳𝐞𝐥𝐞𝐧𝐝𝛊",
    'text_updated': "✅ 𝐓𝐞𝐤𝐬 𝐭𝐚̂𝐳𝐞𝐥𝐞𝐧𝐝𝛊.",
    'post_not_found': "❌ 𝐏𝐨𝐬𝐭 𝐭𝐚𝐩𝐲𝐥𝐦𝐚𝐝𝐲.",
    'ram_psutil_missing': "⚠️ `psutil` gurnalmady. Serwera pip install psutil ediň.",
    'menu_new_post': "𝐓𝐚̂𝐙𝐞 𝐏𝐨𝐬𝐭",
    'menu_stats': "𝐒𝐭𝐚𝐭𝛊𝐬𝐭𝛊𝐤𝐚",
    'menu_posts': "𝐏𝐨𝐬𝐭𝐥𝐚𝐫𝛊𝐦",
    'menu_fill_account': "𝐇𝐚𝐬𝐚𝐛𝐲 𝐃𝐨𝐥𝐝𝐲𝐫",
    'menu_profile': "👤 𝐏𝐫𝐨𝐟𝛊𝐥",
    'menu_about': "ℹ️ 𝐁𝐨𝐭 𝐁𝐚𝐫𝐚𝐝𝐚",
    'menu_admin_panel': "🛠️ 𝐀𝐝𝐦𝛊𝐧 𝐏𝐚𝐧𝐞𝐥",
    'admin_unlimited_button': "♾️ 𝐋𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐣𝐞𝐭𝐨𝐧 𝐠𝐨𝐬̧𝐦𝐚𝐤 \\ 𝐚𝐥𝐦𝐚𝐤",
    'back_label': "⬅️ 𝐘𝐳𝐚",
    'fill_account_text': "𝐒𝐚𝐥𝐚𝐦 — 𝐛𝐨𝐭𝐲𝐦𝐲𝐳𝐲𝐧 𝐬𝛊𝐳𝛊𝐧 𝐤𝐚𝐧𝐚𝐥𝐲𝐧𝐲𝐳𝐚 𝐩𝐨𝐬𝐭 𝐮𝐠𝐫𝐚𝐭𝐦𝐚𝐬𝐲 𝐮̈𝐜̧𝛊𝐧 𝐡𝐚𝐬𝐚𝐛𝐲𝐧𝐲𝐳𝐲 𝐝𝐨𝐥𝐝𝐲𝐫𝐲𝐧.\n\n"
        "𝐊𝐮𝐫𝐬 : 1 𝐓𝐌𝐓 = 250 𝐉𝐄𝐓𝐎𝐍\n\n"
        "𝐈𝐥𝐤𝛊𝐧𝐜𝛊 𝐛𝛊𝐥𝐞𝐧 𝐬̧𝐮 𝐧𝐨𝐦𝐞𝐫𝐚 𝐩𝐮𝐥 𝐠𝐞𝐜̧𝛊𝐫𝛊𝐧 : +99363222850. 𝐒𝐨𝐧𝐫𝐚 𝐠𝐞𝐜̧𝛊𝐫𝛊𝐦𝛊𝐧 ( 𝐬𝐤𝐫𝛊𝐧𝐬̧𝐨𝐭𝐲𝐧𝐲 ) 𝐜̧𝐞𝐠𝛊𝐧𝛊 𝐚𝐝𝐦𝐛𝐢𝐧𝐚 𝐮𝐠̆𝐫𝐚𝐝𝐲𝐧‼️\n\n"
        "𝐆𝐲𝐬𝐠𝐚 𝐯𝐚𝐠𝐭𝐝𝐚 𝐡𝐚𝐬𝐚𝐛𝐲𝐧𝐲𝐳 𝐝𝐨𝐥𝐝𝐲𝐫𝐲𝐥𝐚𝐫 😊\n\n"
        "𝐀𝐝𝐦𝛊𝐧 : @Kodd_75\n\n"
        " 📝 𝐍𝐎𝐓 : 𝐁𝐨𝐭 𝐡𝐞𝐫 𝐛𝛊𝐫 𝐩𝐨𝐬𝐭 𝐮̈𝐜̧𝛊𝐧 1 𝐉𝐄𝐓𝐎𝐍 𝐚𝐥𝐲́𝐚𝐫.",
    'topup_start_admin': "🔧 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐚𝐝𝐲𝐧𝐲 𝐠𝛊𝐫𝛊𝐳𝛊𝐧 ( 𝐦𝐲𝐬𝐚𝐥: @username ):",
    'topup_amount_prompt': "💳 𝐆𝐨𝐬̧𝐮𝐥𝐣𝐚𝐤 𝐣𝐞𝐭𝐨𝐧 𝐦𝐮𝐤𝐝𝐚𝐫𝐲𝐧𝐲 𝐠𝛊𝐫𝛊𝐳𝛊𝐧 ( 𝐦𝐲𝐬𝐚𝐥 : 100 ):",
    'topup_success_owner': "✅ @{username} 𝐮𝐥𝐚𝐧𝐲𝐣𝐲𝐬𝐲𝐧𝐚 {amount} 𝐣𝐞𝐭𝐨𝐧 𝐮̈𝐬𝐭𝐮̈𝐧𝐥𝛊𝐤𝐥𝛊 𝐠𝐨𝐬̧𝐲𝐥𝐝𝛊. 𝐓𝐚̂𝐳𝐞 𝐁𝐚𝐥𝐚𝐧𝐬𝐲: {new}.",
    'topup_success_user_notify': "🎉 𝐀𝐝𝐦𝛊𝐧 @{admin} 𝐭𝐚𝐫𝐚𝐩𝐲𝐧𝐝𝐚𝐧 𝐬𝛊𝐳𝛊𝐧 𝐡𝐚𝐬𝐚𝐛𝐲𝐧𝐲𝐳𝐚 {amount} 𝐣𝐞𝐭𝐨𝐧 𝐠𝐨𝐬̧𝐮𝐥𝐝𝛊.\n𝐓𝐚̂𝐳𝐞 𝐛𝐚𝐥𝐚𝐧𝐬: {new}.",
    'topup_user_not_found': "❌ 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐭𝐚𝐩𝐲𝐥𝐦𝐚𝐝𝐲. 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐛𝐩𝐭𝐲 𝐮𝐥𝐚𝐧𝐦𝐚𝐲́𝐚𝐫 ‼️.",
    'balance_now_zero': "⚠️ 𝐇𝐨𝐫𝐦𝐚𝐭𝐥𝐲 𝐦𝐮̈𝐬̧𝐝𝐞𝐫𝛊𝐦𝛊𝐳 𝐛𝐚𝐥𝐚𝐧𝐬𝐲𝐧𝐲𝐳 0 𝐣𝐞𝐭𝐨𝐧. 𝐁𝐨𝐭 𝛊𝐧𝐝𝛊 𝐤𝐚𝐧𝐚𝐥𝐚 𝐩𝐨𝐬𝐭 𝐮𝐠𝐫𝐚𝐭𝐦𝐚𝐳. 𝐇𝐚𝐬𝐚𝐛𝐲 𝐝𝐨𝐥𝐝𝐲𝐫𝐲𝐧.",
    'users_header': "👥 𝐔𝐥𝐚𝐧𝐲𝐣𝐲𝐥𝐚𝐫: 𝐉𝐞𝐦𝛊 {total}\n𝐒𝐚𝐡𝐲𝐩𝐚 {page}/{pages}\n\n{list}",
    'no_users': "𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐘́𝐨𝐤.",
    'broadcast_prompt': "📣 𝐔𝐠̆𝐫𝐚𝐭𝐣𝐚𝐤 𝐡𝐚𝐛𝐚𝐫𝐲𝐧𝐲𝐳𝐲 𝐲́𝐚𝐳𝐲𝐧:",
    'broadcast_sending': "📣 𝐇𝐚𝐛𝐚𝐫 𝐮𝐥𝐚𝐧𝐲𝐣𝐲𝐥𝐚𝐫𝐚 𝐮𝐠𝐫𝐚𝐝𝐲𝐥𝐲́𝐚𝐫...",
    'broadcast_sent_summary': "✅ 𝐇𝐚𝐛𝐚𝐫 𝐮𝐠𝐫𝐚𝐭𝐨̈𝐚 𝐭𝐚𝐦𝐚𝐦𝐥𝐚𝐧𝐝𝐲.\n𝐔𝐠𝐫𝐚𝐝𝐲𝐥𝐚𝐧: {success}\n𝐔𝐠𝐫𝐚𝐝𝐲𝐥𝐦𝐚𝐝𝐲: {failed}",
    'profile_label': "👤 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐌𝐚𝐠̆𝐥𝐮𝐦𝐚𝐭𝐲",
    'profile_no_username': " 𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞 𝐲́𝐨𝐤",
    'profile_text': "👤 𝐏𝐫𝐨𝐟𝛊𝐥:\n🔵 𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞: {username}\n🆔️ 𝐈𝐃: {id}\n🪙 𝐁𝐚𝐥𝐚𝐧𝐬: {bal} 𝐣𝐞𝐭𝐨𝐧",
    'profile_text_unlimited': "👤 𝐏𝐫𝐨𝐟𝛊𝐥:\n🔵 𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞: {username}\n🆔️ 𝐈𝐃: {id}\n🪙 𝐁𝐚𝐥𝐚𝐧𝐬: 𝐋𝛊𝐦𝛊𝐭𝐬𝛊𝐳 (♾)",
    'about_text': "🤖 𝐁𝐮 𝐛𝐨𝐭 @Kodd_75 𝐭𝐚𝐫𝐚𝐩𝐲𝐧𝐝𝐚𝐧 𝐝𝐨̈𝐫𝐞𝐝𝛊𝐥𝐞𝐧𝐝𝛊𝐫.\n📌 𝐄𝐠𝐞𝐫-𝐝𝐞 𝐬𝛊𝐳𝛂 𝐬𝐞𝐲́𝐥𝐞 𝐛𝐨𝐭 𝐠𝐞𝐫𝐞𝐤 𝐛𝐨𝐥𝐬𝐚 𝐡𝐚𝐛𝐚𝐫𝐥𝐚𝐬̧𝐲𝐧.\n✉️ 𝐃𝐮̈𝐬̧𝐧𝛊𝐤𝐬𝛊𝐳𝐥𝛊𝐤 𝐲́𝐚 𝐝𝐚 𝐬𝐨𝐫𝐚𝐠𝐥𝐚𝐫𝐲𝐧𝐲𝐳 𝐛𝐚𝐫 𝐛𝐨𝐥𝐬𝐚, 𝐛𝛊𝐳𝐞 𝐲́𝐚𝐳𝐲𝐩 𝐛𝛊𝐥𝐞𝐫𝛊𝐧𝐳 : @Kodd_75\n💫 𝐒𝛊𝐳𝛊𝐧 𝐮̈𝐜̧𝛊𝐧 𝐤𝐨̈𝐦𝐞𝐠𝐞 𝐭𝐚𝐲́𝐲́𝐚𝐫‼️",
    'admin_unlimited_prompt': "♾️ 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐚𝐝𝐲𝐧𝐲 𝐠𝛊𝐫𝛊𝐳𝛊𝐧 ( 𝐦𝐲𝐬𝐚𝐥: @username 𝐲́𝐚 𝐝𝐚 𝛊𝐝 ):",
    'admin_unlimited_added': "✅ @{username} 𝐮𝐥𝐚𝐧𝐲𝐣𝐲𝐬𝐲𝐧𝐚 𝐥𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐣𝐞𝐭𝐨𝐧 𝐡𝐮𝐤𝐮𝐠𝐲 𝐛𝐞𝐫𝛊𝐥𝐝𝛊.",
    'admin_unlimited_removed': "✅ @{username} 𝐮𝐥𝐚𝐧𝐲𝐣𝐲𝐬𝐲𝐧𝐲𝐧 𝐥𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐣𝐞𝐭𝐨𝐧 𝐡𝐮𝐤𝐮𝐠𝐲 𝐚𝐲́𝐫𝐲𝐥𝐝𝛊.",
    'admin_unlimited_user_not_found': "❌ 𝐔𝐥𝐚𝐧𝐲𝐣𝐲 𝐭𝐚𝐩𝐲𝐥𝐦𝐚𝐝𝐲 𝐲𝐚 𝐝𝐚 𝐛𝐨𝐭𝐲 𝐮𝐥𝐚𝐧𝐦𝐚𝐲́𝐚𝐫.",
    'admin_unlimited_notify_user_added': "🎖️ 𝐒𝛊𝐳𝐞 𝐚𝐝𝐦𝛊𝐧 𝐭𝐚𝐫𝐚𝐩𝐲𝐧𝐝𝐚𝐧 𝐥𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐣𝐞𝐭𝐨𝐧 𝐡𝐮𝐤𝐮𝐠𝐲 𝐛𝐞𝐫𝛊𝐥𝐝𝛊. 𝐒𝛊𝐳 𝐮̈𝐜̧𝛊𝐧 𝐩𝐨𝐬𝐭𝐥𝐚𝐫 𝐣𝐞𝐭𝐨𝐧𝐬𝐲𝐳 𝐮𝐠𝐫𝐚𝐝𝐲𝐥𝐲́𝐚𝐫.",
    'admin_unlimited_notify_user_removed': "⚠️ 𝐀𝐝𝐦𝛊𝐧 𝐭𝐚𝐫𝐚𝐩𝐲𝐧𝐝𝐚𝐧 𝐬𝛊𝐳𝐞 𝐛𝐞𝐫𝛊𝐥𝐞𝐧 𝐥𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐣𝐞𝐭𝐨𝐧 𝐡𝐮𝐤𝐮𝐠𝐲𝐧𝐲 𝐚𝐥𝐲𝐧𝐝𝐲.",
}

def s(key: str, **kw):
    text = STR.get(key, key)
    return text.format(**kw) if kw else text

# --------------------- SQLite helpers --------------------- #
def _get_conn():
    # isolation_level=None -> autocommit off, we explicitly BEGIN/COMMIT in writes
    return sqlite3.connect(DB_FILE, timeout=10, isolation_level=None)

def init_db_sync():
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_posts (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            text TEXT,
            stickers TEXT,
            second INTEGER,
            channel TEXT,
            next_time REAL,
            sent_count INTEGER,
            max_count INTEGER,
            paused INTEGER
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_next_time ON scheduled_posts(next_time)")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS usernames (
            username TEXT PRIMARY KEY,
            user_id INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS unlimited_users (
            user_id INTEGER PRIMARY KEY
        )""")
        conn.commit()
    finally:
        conn.close()

async def init_db():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, init_db_sync)

async def run_db_write(query: str, params: tuple = ()):
    loop = asyncio.get_event_loop()
    def _sync():
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute(query, params)
            conn.commit()
        finally:
            conn.close()
    await loop.run_in_executor(None, _sync)

async def run_db_fetchall(query: str, params: tuple = ()):
    loop = asyncio.get_event_loop()
    def _sync():
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.fetchall()
        finally:
            conn.close()
    return await loop.run_in_executor(None, _sync)

# --------------------- DB <-> memory sync --------------------- #
async def load_data_from_db():
    global scheduled_posts, balances, usernames, unlimited_users, posts_by_id, _post_heap
    await init_db()
    async with data_lock:
        # balances
        rows = await run_db_fetchall("SELECT user_id, balance FROM balances")
        balances.clear()
        for uid, bal in rows:
            balances[int(uid)] = int(bal)
        # usernames
        rows = await run_db_fetchall("SELECT username, user_id FROM usernames")
        usernames.clear()
        for username, uid in rows:
            usernames[username.lower()] = int(uid)
        # unlimited
        rows = await run_db_fetchall("SELECT user_id FROM unlimited_users")
        unlimited_users.clear()
        for (uid,) in rows:
            unlimited_users.add(int(uid))
        # scheduled posts
        rows = await run_db_fetchall("SELECT id, user_id, text, stickers, second, channel, next_time, sent_count, max_count, paused FROM scheduled_posts")
        scheduled_posts.clear()
        posts_by_id.clear()
        _post_heap.clear()
        cnt = _heap_counter
        now = time.time()
        for r in rows:
            pid = r[0]
            post = {
                "id": pid,
                "user_id": int(r[1]) if r[1] is not None else None,
                "text": r[2] or "",
                "stickers": json.loads(r[3]) if r[3] else [],
                "second": int(r[4]) if r[4] is not None else MIN_SECOND,
                "channel": r[5],
                "next_time": float(r[6]) if r[6] is not None else now,
                "sent_count": int(r[7]) if r[7] is not None else 0,
                "max_count": int(r[8]) if r[8] is not None else None,
                "paused": bool(r[9])
            }
            scheduled_posts.append(post)
            posts_by_id[pid] = post
            heapq.heappush(_post_heap, (post.get("next_time", now), next(cnt), pid))

async def db_insert_post(post: Dict[str, Any]):
    q = """INSERT OR REPLACE INTO scheduled_posts
           (id,user_id,text,stickers,second,channel,next_time,sent_count,max_count,paused)
           VALUES (?,?,?,?,?,?,?,?,?,?)"""
    stickers_json = json.dumps(post.get("stickers") or [])
    params = (post["id"], post.get("user_id"), post.get("text"), stickers_json,
              post.get("second"), post.get("channel"), post.get("next_time"),
              post.get("sent_count", 0), post.get("max_count"), int(bool(post.get("paused"))))
    await run_db_write(q, params)

async def db_delete_post(post_id: str):
    await run_db_write("DELETE FROM scheduled_posts WHERE id=?", (post_id,))

async def db_update_post_next_and_counts(post: Dict[str, Any]):
    q = "UPDATE scheduled_posts SET next_time=?, sent_count=?, paused=?, second=? WHERE id=?"
    params = (post.get("next_time"), post.get("sent_count", 0), int(bool(post.get("paused"))), post.get("second"), post["id"])
    await run_db_write(q, params)

async def db_set_balance(user_id: int, balance: int):
    await run_db_write("INSERT OR REPLACE INTO balances (user_id, balance) VALUES (?,?)", (user_id, balance))

async def db_set_username(username: str, user_id: int):
    await run_db_write("INSERT OR REPLACE INTO usernames (username, user_id) VALUES (?,?)", (username.lower(), user_id))

async def db_delete_username(username: str):
    await run_db_write("DELETE FROM usernames WHERE username=?", (username.lower(),))

async def db_set_unlimited(user_id: int, add: bool):
    if add:
        await run_db_write("INSERT OR REPLACE INTO unlimited_users (user_id) VALUES (?)", (user_id,))
    else:
        await run_db_write("DELETE FROM unlimited_users WHERE user_id=?", (user_id,))

# --------------------- heap / push --------------------- #
async def push_post_to_heap(post: Dict[str, Any]):
    pid = post["id"]
    posts_by_id[pid] = post
    heapq.heappush(_post_heap, (post.get("next_time", time.time()), next(_heap_counter), pid))
    # wake scheduler if sleeping
    heap_updated.set()
    # persist
    await db_insert_post(post)

# --------------------- UI helpers (unchanged) --------------------- #
def main_menu(user_id: int):
    buttons = [
        [InlineKeyboardButton(f"📤 {s('menu_new_post')}", callback_data="yeni_post")],
        [InlineKeyboardButton(f"📊 {s('menu_stats')}", callback_data="stats_page_1")],
        [InlineKeyboardButton(f"📂 {s('menu_posts')}", callback_data="postlist_1")],
        [InlineKeyboardButton(f"💰 {s('menu_fill_account')}", callback_data="hesap_doldur")],
        [InlineKeyboardButton(f"👤 {s('menu_profile')}", callback_data="profile")],
        [InlineKeyboardButton(f"ℹ️ {s('menu_about')}", callback_data="about")]
    ]
    if user_id == OWNER_ID:
        buttons.append([InlineKeyboardButton(s('menu_admin_panel'), callback_data="admin_panel")])
    return InlineKeyboardMarkup(buttons)

def admin_panel_markup():
    buttons = [
        [InlineKeyboardButton("📊 𝐁𝐚𝐥𝐚𝐧𝐬 𝐃𝐨𝐥𝐝𝐲𝐫", callback_data="admin_topup")],
        [InlineKeyboardButton("👥 𝐔𝐥𝐚𝐧𝐲𝐣𝐲𝐥𝐚𝐫", callback_data="users_page_1")],
        [InlineKeyboardButton("📣 𝐇𝐚𝐛𝐚𝐫 𝐔𝐠𝐫𝐚𝐭𝐦𝐚𝐤", callback_data="admin_broadcast")],
        [InlineKeyboardButton("♾️ 𝐋𝛊𝐦𝛊𝐭𝐬𝛊𝐳 𝐉𝐞𝐭𝐨𝐧 𝐁𝐞𝐫𝐦𝐞𝐤 / 𝐀𝐥𝐦𝐚𝐤", callback_data="admin_unlimited")],
        [InlineKeyboardButton(s('back_label'), callback_data="geri")]
    ]
    return InlineKeyboardMarkup(buttons)

def back_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton(s('back_label'), callback_data="geri")]])

async def validate_channel_and_permissions(bot, channel: str) -> (bool, Optional[str]):
    try:
        chat = await bot.get_chat(channel)
        me = await bot.get_me()
        try:
            member = await bot.get_chat_member(chat.id, me.id)
            if getattr(member, "status", None) not in ("administrator", "creator"):
                return False, "err_bot_not_admin"
        except Exception:
            pass
        return True, None
    except Exception:
        return False, "err_channel_access"

def can_manage_post(user_id: int, post: Dict[str, Any]) -> bool:
    return user_id == OWNER_ID or post.get('user_id') == user_id

def paginate(items: List[Any], page: int, per_page: int = PAGE_SIZE):
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page)
    if page < 1:
        page = 1
    if page > pages:
        page = pages
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], page, pages, total

# --------------------- Handlers --------------------- #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    if user.username:
        usernames[user.username.lstrip('@').lower()] = user_id
        await db_set_username(user.username.lstrip('@'), user_id)
    first_time = False
    if user_id not in balances:
        balances[user_id] = WELCOME_GIFT
        await db_set_balance(user_id, WELCOME_GIFT)
        first_time = True
    if first_time:
        try:
            await update.message.reply_text(s('welcome_first_time'), reply_markup=main_menu(user_id))
        except Exception:
            await update.message.reply_text(s('welcome_first_time'))
    else:
        msg = await update.message.reply_text(s('welcome'), reply_markup=main_menu(user_id))
        menu_message_id[user_id] = msg.message_id

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if data == "yeni_post":
        user_sessions[user_id] = {}
        waiting_for[user_id] = "text"
        await query.edit_message_text(s('enter_text'), reply_markup=back_menu())
        return
    if data.startswith("stats_page_"):
        try:
            page = int(data.split("_", 2)[2])
        except Exception:
            page = 1
        async with data_lock:
            channels = sorted({p['channel'] for p in scheduled_posts})
            total_channels = len(channels)
        page_items, page, pages, total = paginate(channels, page)
        if not page_items:
            await query.edit_message_text(s('stats_header', total=total_channels, page=page, pages=pages, list="Ýok"), reply_markup=back_menu())
            return
        lines = [f"{i+1}) {ch}" for i, ch in enumerate(page_items, start=(page - 1) * PAGE_SIZE)]
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◀ Öňki", callback_data=f"stats_page_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
        if page < pages:
            nav.append(InlineKeyboardButton("Indiki ▶", callback_data=f"stats_page_{page+1}"))
        markup = InlineKeyboardMarkup([nav, [InlineKeyboardButton(s('back_label'), callback_data="geri")]])
        text = s('stats_header', total=total_channels, page=page, pages=pages, list="\n".join(lines))
        await query.edit_message_text(text, reply_markup=markup)
        return
    if data.startswith("postlist_"):
        try:
            page = int(data.split("_", 1)[1])
        except Exception:
            page = 1
        async with data_lock:
            if user_id == OWNER_ID:
                items = list(scheduled_posts)
            else:
                items = [p for p in scheduled_posts if p['user_id'] == user_id]
        page_items, page, pages, total = paginate(items, page)
        if not page_items:
            await query.edit_message_text(s('post_none'), reply_markup=back_menu())
            return
        lines = []
        buttons = []
        for i, p in enumerate(page_items, start=(page - 1) * PAGE_SIZE + 1):
            status = '⏸' if p.get('paused') else '▶'
            short = (p.get('text') or '')[:30].replace('\n', ' ')
            lines.append(f"{i}) {p['channel']} {status} - {short}")
            buttons.append([InlineKeyboardButton(f"{i}) {p['channel']} {status}", callback_data=f"post_{p['id']}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀ Öňki", callback_data=f"postlist_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
        if page < pages:
            nav_buttons.append(InlineKeyboardButton("Indiki ▶", callback_data=f"postlist_{page+1}"))
        buttons.append(nav_buttons)
        buttons.append([InlineKeyboardButton(s('back_label'), callback_data="geri")])
        text = "📂 𝐏𝐨𝐬𝐭𝐥𝐚𝐫 ( 𝐮𝐦𝐮𝐦𝐲 𝐩𝐨𝐬𝐭 {}):\n\n{}".format(total, "\n".join(lines))
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        return
    if data.startswith("users_page_"):
        if user_id != OWNER_ID:
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        try:
            page = int(data.split("_", 2)[2])
        except Exception:
            page = 1
        async with data_lock:
            user_ids = set(usernames.values()) | set(balances.keys()) | {p.get('user_id') for p in scheduled_posts}
        user_list = sorted([uid for uid in user_ids if uid is not None])
        page_items, page, pages, total = paginate(user_list, page)
        if not page_items:
            await query.edit_message_text(s('no_users'), reply_markup=back_menu())
            return
        lines = []
        for idx, uid in enumerate(page_items, start=(page - 1) * PAGE_SIZE + 1):
            uname = None
            for k, v in usernames.items():
                if v == uid:
                    uname = k
                    break
            if uname:
                name = f"@{uname}"
            else:
                name = f"id:{uid}"
            bal_display = "Limitsiz ♾" if uid in unlimited_users else str(balances.get(uid, 0))
            lines.append(f"{idx}) {name} — {bal_display} jeton")
        text = s('users_header', total=total, page=page, pages=pages, list="\n".join(lines))
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◀ Öňki", callback_data=f"users_page_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
        if page < pages:
            nav.append(InlineKeyboardButton("Indiki ▶", callback_data=f"users_page_{page+1}"))
        markup = InlineKeyboardMarkup([nav, [InlineKeyboardButton(s('back_label'), callback_data="geri")]])
        await query.edit_message_text(text, reply_markup=markup)
        return
    if data == "admin_broadcast":
        if user_id != OWNER_ID:
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        waiting_for[user_id] = "broadcast_message"
        await query.edit_message_text(s('broadcast_prompt'), reply_markup=back_menu())
        return
    if data == "profile":
        uname = None
        for k, v in usernames.items():
            if v == user_id:
                uname = k
                break
        username_display = f"@{uname}" if uname else s('profile_no_username')
        if user_id in unlimited_users:
            text = s('profile_text_unlimited', username=username_display, id=user_id)
        else:
            bal = balances.get(user_id, 0)
            text = s('profile_text', username=username_display, id=user_id, bal=bal)
        await query.edit_message_text(text, reply_markup=back_menu())
        return
    if data == "about":
        await query.edit_message_text(s('about_text'), reply_markup=back_menu())
        return
    if data == "admin_panel":
        if user_id != OWNER_ID:
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        await query.edit_message_text("🛠️ 𝐀𝐝𝐦𝛊𝐧 𝐏𝐚𝐧𝐞𝐥", reply_markup=admin_panel_markup())
        return
    if data == "admin_topup":
        if user_id != OWNER_ID:
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        waiting_for[user_id] = "topup_username"
        await query.edit_message_text(s('topup_start_admin'), reply_markup=back_menu())
        return
    if data == "admin_unlimited":
        if user_id != OWNER_ID:
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        waiting_for[user_id] = "grant_unlimited_username"
        await query.edit_message_text(s('admin_unlimited_prompt'), reply_markup=back_menu())
        return
    if data.startswith("post_"):
        post_id = data.split("_", 1)[1]
        async with data_lock:
            post = next((p for p in scheduled_posts if p['id'] == post_id), None)
        if not post:
            await query.edit_message_text(s('post_not_found'), reply_markup=back_menu())
            return
        if not can_manage_post(user_id, post):
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        controls = [
            InlineKeyboardButton("📝 𝐔𝐲́𝐭𝐠𝐞𝐭", callback_data=f"edit_{post_id}"),
            InlineKeyboardButton("🗑 𝐎𝐜̧𝐮̈𝐫", callback_data=f"delete_{post_id}"),
            InlineKeyboardButton("▶ 𝐃𝐨𝐯𝐚𝐦" if post.get('paused') else "⏸ 𝐃𝐮𝐫𝐮𝐙", callback_data=f"toggle_{post_id}"),
        ]
        info = (f"📤 𝐊𝐚𝐧𝐚𝐥: {post['channel']}\n"
                f"🕒 𝐀𝐫𝐚𝐥𝐲𝐤: {post['second']} 𝐬𝐞𝐤\n"
                f"📮 𝐈𝐛𝐞𝐫𝛊𝐥𝐞𝐧: {post.get('sent_count',0)}\n"
                f"👤 𝐄𝐲́𝐞𝐬𝛊: {post.get('user_id')}\n\n"
                f"📝 𝐓𝐞𝐤𝐬𝐭:\n{post.get('text')}\n"
                f"➕️ 𝐒𝐭𝛊𝐤𝐞𝐫𝐥𝐞𝐫: {len(post.get('stickers',[]))}")
        await query.edit_message_text(info, reply_markup=InlineKeyboardMarkup([controls, [InlineKeyboardButton(s('back_label'), callback_data="geri")]]))
        return
    if data.startswith("delete_"):
        post_id = data.split("_", 1)[1]
        async with data_lock:
            idx = next((i for i,p in enumerate(scheduled_posts) if p['id']==post_id), None)
            if idx is not None:
                post = scheduled_posts[idx]
                if not can_manage_post(user_id, post):
                    await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
                    return
                key = (post['channel'], post['id'])
                scheduled_posts.pop(idx)
                posts_by_id.pop(post_id, None)
                previous_messages.pop(key, None)
                await db_delete_post(post_id)
        await query.edit_message_text(s('post_deleted'), reply_markup=back_menu())
        return
    if data.startswith("toggle_"):
        post_id = data.split("_", 1)[1]
        async with data_lock:
            post = next((p for p in scheduled_posts if p['id']==post_id), None)
            if not post:
                await query.edit_message_text(s('post_not_found'), reply_markup=back_menu())
                return
            if not can_manage_post(user_id, post):
                await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
                return
            post['paused'] = not post.get('paused', False)
            if not post['paused']:
                post['next_time'] = time.time()
                await push_post_to_heap(post)
            await db_update_post_next_and_counts(post)
        await query.edit_message_text(s('post_toggled'), reply_markup=back_menu())
        return
    if data.startswith("edit_"):
        post_id = data.split("_", 1)[1]
        async with data_lock:
            post = next((p for p in scheduled_posts if p['id'] == post_id), None)
        if not post:
            await query.edit_message_text(s('post_not_found'), reply_markup=back_menu())
            return
        if not can_manage_post(user_id, post):
            await query.edit_message_text(s('no_permission'), reply_markup=back_menu())
            return
        waiting_for[user_id] = f"edit_{post_id}"
        await query.edit_message_text(s('edit_prompt'), reply_markup=back_menu())
        return
    if data == "geri":
        await query.edit_message_text(s('welcome'), reply_markup=main_menu(user_id))
        return
    if data == "hesap_doldur":
        await query.edit_message_text(s('fill_account_text'), reply_markup=back_menu())
        return
    if data == "noop":
        await query.answer()
        return

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.message.from_user
    user_id = user.id
    if user.username:
        usernames[user.username.lstrip('@').lower()] = user_id
        await db_set_username(user.username.lstrip('@'), user_id)
    if user_id in waiting_for:
        step = waiting_for[user_id]
        sess = user_sessions.get(user_id, {})
        if step == "text":
            text = update.message.text
            stickers = []
            if update.message.sticker:
                stickers.append(update.message.sticker.file_id)
            if text:
                sess['text'] = text
            if stickers:
                sess.setdefault('stickers', []).extend(stickers)
            user_sessions[user_id] = sess
            waiting_for[user_id] = "second"
            await update.message.reply_text(s('enter_second'), reply_markup=back_menu())
            return
        if step == "second":
            try:
                sec = int(update.message.text)
                if user_id != OWNER_ID:
                    if sec < MIN_SECOND:
                        await update.message.reply_text(s('min_seconds'), reply_markup=back_menu())
                        return
                    if sec > MAX_SECOND:
                        await update.message.reply_text(s('max_seconds'), reply_markup=back_menu())
                        return
                sess['second'] = sec
                waiting_for[user_id] = "channel"
                await update.message.reply_text(s('enter_channel'), reply_markup=back_menu())
            except Exception:
                await update.message.reply_text(s('not_number'), reply_markup=back_menu())
            return
        if step == "channel":
            channel = update.message.text.strip()
            key_ok, err_key = await validate_channel_and_permissions(context.bot, channel)
            if not key_ok:
                await update.message.reply_text(s('invalid_channel'), reply_markup=back_menu())
                return
            post = {
                'id': uuid4().hex,
                'user_id': user_id,
                'text': sess.get('text', ''),
                'stickers': sess.get('stickers', []),
                'second': sess['second'],
                'channel': channel,
                'next_time': time.time(),
                'sent_count': 0,
                'max_count': None,
                'paused': False
            }
            async with data_lock:
                scheduled_posts.append(post)
                await push_post_to_heap(post)
            waiting_for.pop(user_id, None)
            user_sessions.pop(user_id, None)
            await update.message.reply_text(s('post_added', channel=post['channel']), reply_markup=back_menu())
            return
        if step.startswith("edit_"):
            post_id = step.split("_", 1)[1]
            async with data_lock:
                post = next((p for p in scheduled_posts if p['id'] == post_id), None)
                if not post:
                    await update.message.reply_text(s('post_not_found'), reply_markup=back_menu())
                    waiting_for.pop(user_id, None)
                    return
                if not can_manage_post(user_id, post):
                    await update.message.reply_text(s('no_permission'), reply_markup=back_menu())
                    waiting_for.pop(user_id, None)
                    return
                text = update.message.text
                stickers = []
                if update.message.sticker:
                    stickers.append(update.message.sticker.file_id)
                if text and text.isdigit():
                    sec = int(text)
                    if user_id != OWNER_ID:
                        if sec < MIN_SECOND:
                            await update.message.reply_text(s('min_seconds'), reply_markup=back_menu())
                            return
                        if sec > MAX_SECOND:
                            await update.message.reply_text(s('max_seconds'), reply_markup=back_menu())
                            return
                    post['second'] = sec
                    post['next_time'] = time.time()
                    await db_update_post_next_and_counts(post)
                    await push_post_to_heap(post)
                    await update.message.reply_text(s('duration_updated'), reply_markup=back_menu())
                else:
                    updated = False
                    if text:
                        post['text'] = text
                        updated = True
                        await update.message.reply_text(s('text_updated'), reply_markup=back_menu())
                    if stickers:
                        post.setdefault('stickers', []).extend(stickers)
                        updated = True
                        await update.message.reply_text("✅ 𝐒𝐭𝐢𝐤𝐞𝐫𝐥𝐞𝐫 𝐠𝐨𝐬̧𝐮𝐥𝐝𝛊.", reply_markup=back_menu())
                    if updated:
                        await db_insert_post(post)
                # mark_dirty no-op with DB
            waiting_for.pop(user_id, None)
            return
        if step == "topup_username":
            if user_id != OWNER_ID:
                await update.message.reply_text(s('no_permission'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                return
            username_raw = update.message.text.strip().lstrip('@').lower()
            target_id = usernames.get(username_raw)
            if not target_id:
                await update.message.reply_text(s('topup_user_not_found'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                return
            user_sessions[user_id] = {'topup_target': target_id, 'topup_username': username_raw}
            waiting_for[user_id] = "topup_amount"
            await update.message.reply_text(s('topup_amount_prompt'), reply_markup=back_menu())
            return
        if step == "topup_amount":
            if user_id != OWNER_ID:
                await update.message.reply_text(s('no_permission'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                user_sessions.pop(user_id, None)
                return
            try:
                amount = int(update.message.text.strip())
                if amount <= 0:
                    raise ValueError("non-positive")
            except Exception:
                await update.message.reply_text(s('not_number'), reply_markup=back_menu())
                return
            sess = user_sessions.get(user_id, {})
            target_id = sess.get('topup_target')
            target_username = sess.get('topup_username')
            if not target_id:
                await update.message.reply_text(s('topup_user_not_found'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                user_sessions.pop(user_id, None)
                return
            async with data_lock:
                balances[target_id] = balances.get(target_id, 0) + amount
                new_bal = balances[target_id]
                await db_set_balance(target_id, new_bal)
            await update.message.reply_text(s('topup_success_owner', username=target_username, amount=amount, new=new_bal), reply_markup=main_menu(user_id))
            try:
                await context.bot.send_message(target_id, s('topup_success_user_notify', admin=(update.effective_user.username or str(OWNER_ID)), amount=amount, new=new_bal))
            except Exception:
                pass
            waiting_for.pop(user_id, None)
            user_sessions.pop(user_id, None)
            return
        if step == "broadcast_message":
            if user_id != OWNER_ID:
                await update.message.reply_text(s('no_permission'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                return
            broadcast_text = update.message.text or ""
            await update.message.reply_text(s('broadcast_sending'), reply_markup=back_menu())
            async with data_lock:
                uid_sources = (usernames.values(), balances.keys(), (p.get('user_id') for p in scheduled_posts))
                user_ids = set()
                for src in uid_sources:
                    for uid in src:
                        if uid:
                            user_ids.add(uid)
            success = 0
            failed = 0
            for uid in user_ids:
                try:
                    await context.bot.send_message(uid, broadcast_text)
                    success += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.06)
            await update.message.reply_text(s('broadcast_sent_summary', success=success, failed=failed), reply_markup=main_menu(user_id))
            waiting_for.pop(user_id, None)
            user_sessions.pop(user_id, None)
            return
        if step == "grant_unlimited_username":
            if user_id != OWNER_ID:
                await update.message.reply_text(s('no_permission'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                return
            username_raw = update.message.text.strip()
            target_id = None
            if username_raw.startswith("@"):
                username_raw_clean = username_raw.lstrip('@').lower()
                target_id = usernames.get(username_raw_clean)
            else:
                try:
                    maybe_id = int(username_raw)
                    target_id = maybe_id
                except Exception:
                    target_id = usernames.get(username_raw.lower())
            if not target_id:
                await update.message.reply_text(s('admin_unlimited_user_not_found'), reply_markup=back_menu())
                waiting_for.pop(user_id, None)
                return
            async with data_lock:
                if target_id in unlimited_users:
                    unlimited_users.remove(target_id)
                    await db_set_unlimited(target_id, False)
                    uname_display = next((k for k,v in usernames.items() if v==target_id), None)
                    await update.message.reply_text(s('admin_unlimited_removed', username=(uname_display or str(target_id))), reply_markup=main_menu(user_id))
                    try:
                        await context.bot.send_message(target_id, s('admin_unlimited_notify_user_removed'))
                    except Exception:
                        pass
                else:
                    unlimited_users.add(target_id)
                    await db_set_unlimited(target_id, True)
                    uname_display = next((k for k,v in usernames.items() if v==target_id), None)
                    await update.message.reply_text(s('admin_unlimited_added', username=(uname_display or str(target_id))), reply_markup=main_menu(user_id))
                    try:
                        await context.bot.send_message(target_id, s('admin_unlimited_notify_user_added'))
                    except Exception:
                        pass
            waiting_for.pop(user_id, None)
            return
    text = update.message.text or ""
    if text.startswith("/"):
        return
    await update.message.reply_text(s('welcome'), reply_markup=main_menu(user_id))

# --------------------- Scheduler --------------------- #
async def scheduler(app):
    bot = app.bot
    while True:
        if not _post_heap:
            await heap_updated.wait()
            heap_updated.clear()
            continue
        now = time.time()
        next_time, _, pid = _post_heap[0]
        wait = max(0, next_time - now)
        try:
            await asyncio.wait_for(heap_updated.wait(), timeout=wait)
            heap_updated.clear()
            continue
        except asyncio.TimeoutError:
            pass
        popped = heapq.heappop(_post_heap)
        nt, _, post_id = popped
        post = posts_by_id.get(post_id)
        if not post:
            continue
        if post.get('next_time', 0) != nt and abs(post.get('next_time', 0) - nt) > 0.5:
            continue
        try:
            if post.get('paused'):
                async with data_lock:
                    post['next_time'] = time.time() + max(30, post.get('second', MIN_SECOND))
                    await db_update_post_next_and_counts(post)
                    await push_post_to_heap(post)
                continue
            owner = post.get('user_id')
            if owner not in unlimited_users:
                owner_balance = balances.get(owner, 0)
                if owner_balance <= 0:
                    async with data_lock:
                        for p in scheduled_posts:
                            if p.get('user_id') == owner and not p.get('paused'):
                                p['paused'] = True
                                await db_update_post_next_and_counts(p)
                        # notify user once
                    try:
                        await bot.send_message(owner, s('balance_now_zero'))
                    except Exception:
                        pass
                    continue
            key = (post['channel'], post['id'])
            prev_mid = previous_messages.get(key)
            sent_success = False
            if prev_mid:
                try:
                    if post.get('text'):
                        await bot.edit_message_text(post['text'], chat_id=post['channel'], message_id=prev_mid)
                        sent_success = True
                except Exception:
                    sent_success = False
            if not sent_success:
                if prev_mid:
                    try:
                        await bot.delete_message(post['channel'], prev_mid)
                    except Exception:
                        pass
                try:
                    if post.get('text'):
                        msg = await bot.send_message(post['channel'], post['text'])
                        previous_messages[key] = msg.message_id
                    else:
                        dummy = await bot.send_message(post['channel'], " ")
                        previous_messages[key] = dummy.message_id
                    sent_success = True
                except Exception:
                    async with data_lock:
                        post['next_time'] = time.time() + max(30, post.get('second', MIN_SECOND))
                        await db_update_post_next_and_counts(post)
                        await push_post_to_heap(post)
                    continue
            if sent_success:
                stickers = post.get('stickers', []) or []
                for st in stickers:
                    try:
                        await bot.send_sticker(chat_id=post['channel'], sticker=st)
                    except Exception:
                        pass
                    await asyncio.sleep(0.08)
                async with data_lock:
                    if owner not in unlimited_users:
                        balances[owner] = balances.get(owner, 0) - 1
                        await db_set_balance(owner, balances[owner])
                    post['sent_count'] = post.get('sent_count', 0) + 1
                    post['next_time'] = time.time() + post.get('second', MIN_SECOND)
                    await db_update_post_next_and_counts(post)
                    new_bal = balances.get(owner, 0)
                if owner not in unlimited_users and new_bal <= 0:
                    async with data_lock:
                        for p in scheduled_posts:
                            if p.get('user_id') == owner and not p.get('paused'):
                                p['paused'] = True
                                await db_update_post_next_and_counts(p)
                    try:
                        await bot.send_message(owner, s('balance_now_zero'))
                    except Exception:
                        pass
                await push_post_to_heap(post)
        except Exception as e:
            logger.exception("Scheduler error: %s", e)
        await asyncio.sleep(0)

# --------------------- Utility commands --------------------- #
async def ram_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text(s('no_permission'))
        return
    if psutil is None:
        await update.message.reply_text(s('ram_psutil_missing'))
        return
    process = psutil.Process()
    mem = process.memory_info().rss / 1024 / 1024
    vm = psutil.virtual_memory().total / 1024 / 1024
    await update.message.reply_text(f"RAM: {mem:.1f} MB\nTotal VM: {vm:.1f} MB")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(s('help_text'))

async def set_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Boty işjeňleşdir"),
        BotCommand("ram", "RAM ulanyşyny görkez"),
        BotCommand("help", "Kömek")
    ])

# --------------------- Main --------------------- #
async def main():
    await load_data_from_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ram", ram_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), message_handler))
    app.create_task(scheduler(app))
    # keep a light autosave loop to preserve structure
    async def noop_autosave():
        while True:
            await asyncio.sleep(AUTO_SAVE_INTERVAL)
    app.create_task(noop_autosave())
    await set_commands(app)
    await app.run_polling()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.get_event_loop().run_until_complete(main())
