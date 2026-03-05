#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════
    Telegram Bot - APK File Manager with Star Support System
    Built with python-telegram-bot v20+
    Designed to run on Render (Free Plan) with run_polling
══════════════════════════════════════════════════════════════════
"""

import os
import json
import math
import time
import random
import signal
import secrets
import logging
import asyncio
import threading
import html
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    BotCommand,
    BotCommandScopeChat,
    WebAppInfo,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
    filters,
    ContextTypes,
)


# ══════════════════════════════════════════════════════════════════
#  التهيئة والإعدادات
# ══════════════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 7246473970
CHANNEL_ID = -1002489542574
BOT_USERNAME = "AE_Mode_bot"
DATA_FILE = "data.json"

# رابط صفحة الويب المصغرة لإعلانات Monetag
# تم ضبط الرابط الذي تم الحصول عليه من tiiny.site لضمان عمل زر المساهمة
WEBAPP_URL = "https://relimarco72-gif.github.io/telegram-apk-bot/"

# إعدادات الحماية السلوكية
RATE_LIMIT_SECONDS = 5       # الحد الأدنى بين العمليات
SPAM_WINDOW = 30             # نافذة كشف السبام (ثانية)
SPAM_THRESHOLD = 5           # عدد العمليات المسموحة في النافذة
MAX_VIOLATIONS = 3           # الحد الأقصى للمخالفات قبل الحظر
JUMP_MULTIPLIER = 5          # مضاعف كشف القفزات غير الطبيعية

# إعداد التسجيل
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  خادم Render الصحي (للحفاظ على تشغيل الخدمة)
# ══════════════════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    """معالج HTTP بسيط للاستجابة لفحوصات Render الصحية."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running")

    def log_message(self, format, *args):
        """تعطيل سجلات HTTP لعدم تشويش سجلات البوت."""
        pass


def _start_health_server():
    """تشغيل خادم HTTP صحي في خيط منفصل لمنع Render من إيقاف الخدمة."""
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info("Health server started on port %d", port)
    server.serve_forever()


# ══════════════════════════════════════════════════════════════════
#  إدارة قاعدة البيانات (data.json)
# ══════════════════════════════════════════════════════════════════

DEFAULT_DATA = {
    "activated_users": [],
    "banned_users": [],
    "violations": {},
    "user_last_action": {},
    "user_stats": {},
    "files": {},
    "logs": [],
    "active_watch_sessions": {},
}

DATA_LOCK = threading.Lock()
_cached_data = None

def load_data() -> dict:
    """تحميل البيانات مع نظام قفل (Lock) لضمان سلامة البيانات ومنع التضارب."""
    global _cached_data
    
    with DATA_LOCK:
        if _cached_data is not None:
            return _cached_data
            
        safe_path = os.path.join(os.getcwd(), os.path.basename(DATA_FILE))
        if not os.path.exists(safe_path):
            _cached_data = DEFAULT_DATA.copy()
            save_data_internal(_cached_data)
            return _cached_data
            
        try:
            with open(safe_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # التأكد من وجود كل المفاتيح المطلوبة
            for key, default_val in DEFAULT_DATA.items():
                if key not in data:
                    data[key] = type(default_val)()
            
            # تنظيف الجلسات القديمة (أكثر من 24 ساعة) لتوفير الذاكرة ومنع تضخم الملف
            now = time.time()
            if "active_watch_sessions" in data:
                expired = [k for k, v in data["active_watch_sessions"].items() 
                           if now - v.get("start_time", 0) > 86400]
                for k in expired:
                    del data["active_watch_sessions"][k]
            
            _cached_data = data
            return _cached_data
        except (json.JSONDecodeError, IOError):
            _cached_data = DEFAULT_DATA.copy()
            save_data_internal(_cached_data)
            return _cached_data

def save_data(data: dict) -> None:
    """حفظ البيانات للقرص مع تحديث الكاش والقفل."""
    global _cached_data
    with DATA_LOCK:
        _cached_data = data
        save_data_internal(data)

def save_data_internal(data: dict) -> None:
    """الحفظ الفعلي للملف (يجب استدعاؤها داخل lock)."""
    safe_path = os.path.join(os.getcwd(), os.path.basename(DATA_FILE))
    try:
        # الحفظ في ملف مؤقت ثم الاستبدال لضمان عدم تلف البيانات عند انقطاع الطاقة
        temp_path = safe_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp_path, safe_path)
    except IOError as e:
        logger.error("Failed to save data: %s", e)


def add_log(data: dict, action: str, user_id: int = 0, details: str = "") -> None:
    """إضافة سجل جديد مع الاحتفاظ بآخر 1000 سجل فقط."""
    data["logs"].append({
        "timestamp": time.time(),
        "action": action,
        "user_id": user_id,
        "details": details,
    })
    if len(data["logs"]) > 1000:
        data["logs"] = data["logs"][-1000:]


# ══════════════════════════════════════════════════════════════════
#  دوال مساعدة
# ══════════════════════════════════════════════════════════════════

def is_banned(user_id: int) -> bool:
    """التحقق مما إذا كان المستخدم محظوراً."""
    data = load_data()
    return user_id in data["banned_users"]


def generate_file_key() -> str:
    """إنشاء مفتاح عشوائي فريد للملف (8 أحرف)."""
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choices(chars, k=8))


def html_escape(text: str) -> str:
    """تهريب الأحرف الخاصة في HTML لتجنب أخطاء parse_entities."""
    return html.escape(str(text))


def create_progress_bar(current: int, total: int, length: int = 20) -> str:
    """إنشاء شريط تقدم بصري."""
    if total <= 0:
        return "░" * length + " 0%"
    ratio = min(current / total, 1.0)
    filled = math.floor(ratio * length)
    empty = length - filled
    bar = "█" * filled + "░" * empty
    pct = math.floor(ratio * 100)
    return f"[{bar}] {pct}%"


def _is_file_fully_unlocked(file_data: dict) -> bool:
    """التحقق مما إذا كان الملف مفتوحاً بأي من الطريقتين (نجوم أو مشاهدات)."""
    stars_done = file_data["current_stars"] >= file_data["total_stars"]
    views_done = file_data.get("current_views", 0) >= file_data.get("required_views", 999999)
    return stars_done or views_done


    return text


def build_channel_message(file_data: dict) -> str:
    """بناء نص رسالة القناة مع معالجة الأخطاء."""
    current_views = file_data.get("current_views", 0)
    required_views = file_data.get("required_views", 0)
    progress = create_progress_bar(current_views, required_views)
    
    unlocked = _is_file_fully_unlocked(file_data)
    
    if unlocked:
        return (
            f"✅ <b>تم فتح الملف بنجاح!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>{html_escape(file_data['name'])}</b>\n\n"
            f"🎉 أصبح الملف متاحاً للتحميل الآن للجميع!\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
    else:
        # ملاحظة: post_text لا يتم تهريبه لأنه قد يحتوي على كود HTML من المالك
        return (
            f"📦 <b>{html_escape(file_data['name'])}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{file_data.get('post_text', '')}\n\n"
            f"📊 التقدم الحالي: {progress}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )


def build_channel_keyboard(file_key: str, file_data: dict):
    """بناء لوحة مفاتيح رسالة القناة."""
    if _is_file_fully_unlocked(file_data):
        return None
    return build_channel_keyboard_deep(file_key, file_data)


def build_channel_keyboard_deep(file_key: str, file_data: dict):
    """بناء زر الانتقال للبوت لفتح الملف مع زر عرض النجوم."""
    keyboard = [
        [
            InlineKeyboardButton(
                "🔓 فتح الملف",
                url=f"https://t.me/{BOT_USERNAME}?start=watch_{file_key}",
            )
        ],
        [
            InlineKeyboardButton(
                "⭐ النجوم المطلوبة",
                url=f"https://t.me/{BOT_USERNAME}?start=support_{file_key}",
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)





# ══════════════════════════════════════════════════════════════════
#  نظام الحماية السلوكي
# ══════════════════════════════════════════════════════════════════

async def add_violation(
    data: dict, user_id: int, reason: str, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    إضافة مخالفة للمستخدم والتحقق من الحظر التلقائي.
    يتم إشعار المالك عند كل مخالفة وعند الحظر.
    """
    uid = str(user_id)

    if uid not in data["violations"]:
        data["violations"][uid] = []

    data["violations"][uid].append({
        "reason": reason,
        "timestamp": time.time(),
    })

    count = len(data["violations"][uid])
    add_log(data, "violation", user_id, f"Reason: {reason}, Count: {count}")

    # ترجمة سبب المخالفة
    reason_ar = {
        "rate_limit": "تجاوز حد المعدل (5 ثواني)",
        "spam": "سبام (عمليات متكررة)",
        "jump": "قفزة غير طبيعية في عدد النجوم",
    }.get(reason, reason)

    # إشعار المالك بالمخالفة
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"⚠️ <b>مخالفة جديدة</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 المستخدم: <code>{user_id}</code>\n"
                f"📋 السبب: {reason_ar}\n"
                f"🔢 عدد المخالفات: {count}/{MAX_VIOLATIONS}"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Failed to notify owner about violation: %s", e)

    # الحظر التلقائي عند بلوغ الحد الأقصى
    if count >= MAX_VIOLATIONS and user_id not in data["banned_users"]:
        data["banned_users"].append(user_id)
        add_log(data, "auto_ban", user_id, f"Banned after {MAX_VIOLATIONS} violations")

        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🚫 <b>تم حظر مستخدم تلقائياً!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 المستخدم: <code>{user_id}</code>\n"
                    f"📋 السبب: تجاوز {MAX_VIOLATIONS} مخالفات\n"
                    f"🕐 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                ),
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Failed to notify owner about ban: %s", e)

    save_data(data)


async def check_protection(
    user_id: int, stars: int, context: ContextTypes.DEFAULT_TYPE
) -> tuple:
    """
    فحص الحماية السلوكية. يعيد (مسموح: bool, سبب_الرفض: str).
    الفحوصات: حظر، معدل، سبام، قفزات غير طبيعية.
    """
    # المالك معفى من الحماية
    if user_id == ADMIN_ID:
        return True, ""

    data = load_data()
    uid = str(user_id)
    now = time.time()

    # ── التحقق من الحظر ──
    if user_id in data["banned_users"]:
        return False, "🚫 أنت محظور من استخدام البوت."

    # ── فحص معدل العمليات (Rate Limit) ──
    last_action = data["user_last_action"].get(uid, 0)
    if now - last_action < RATE_LIMIT_SECONDS:
        await add_violation(data, user_id, "rate_limit", context)
        return False, f"⚠️ انتظر {RATE_LIMIT_SECONDS} ثواني بين كل عملية."

    # ── تهيئة إحصائيات المستخدم ──
    if uid not in data["user_stats"]:
        data["user_stats"][uid] = {"actions": [], "total_stars": 0, "count": 0}

    stats = data["user_stats"][uid]

    # ── فحص السبام (5 عمليات في 30 ثانية) ──
    stats["actions"] = [t for t in stats["actions"] if now - t < SPAM_WINDOW]
    if len(stats["actions"]) >= SPAM_THRESHOLD:
        await add_violation(data, user_id, "spam", context)
        return False, "⚠️ تم كشف سبام! انتظر قليلاً قبل المحاولة مرة أخرى."

    # ── فحص القفزات غير الطبيعية ──
    if stats["count"] > 0:
        avg = stats["total_stars"] / stats["count"]
        if avg > 0 and stars > avg * JUMP_MULTIPLIER and stars > 10:
            await add_violation(data, user_id, "jump", context)
            return False, "⚠️ تم كشف قفزة غير طبيعية في عدد النجوم!"

    # ── تحديث الإحصائيات ──
    stats["actions"].append(now)
    data["user_last_action"][uid] = now
    save_data(data)

    return True, ""


# ══════════════════════════════════════════════════════════════════
#  معالجات الأوامر (Command Handlers)
# ══════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    معالج أمر /start مع دعم Deep Linking.
    عند الضغط على زر الدعم في القناة، يتم فتح البوت مع معامل support_FILEKEY.
    """
    user_id = update.effective_user.id

    # تجاهل المستخدمين المحظورين
    if is_banned(user_id):
        return

    # تسجيل المستخدم الجديد
    data = load_data()
    if user_id not in data["activated_users"]:
        data["activated_users"].append(user_id)
        add_log(data, "user_activated", user_id)
        save_data(data)

    # التحقق من Deep Linking (دعم النجوم والمشاهدات)
    if context.args and len(context.args) > 0:
        arg = context.args[0]
        if arg.startswith("support_"):
            file_key = arg[8:]  # إزالة البادئة "support_"
            await _handle_support_entry(update, context, file_key)
            return
        elif arg.startswith("watch_"):
            file_key = arg[6:]  # إزالة البادئة "watch_"
            await _handle_watch_entry(update, context, file_key)
            return
        elif arg.startswith("verify_"):
            raw_key = arg[7:]   # إزالة البادئة "verify_"
            # لتجنب كاش تيليجرام للرابط، نضيف وقت عشوائي في النهاية، لذا يجب فصله
            file_key = raw_key.split('_')[0]
            await _handle_view_completed(update, context, file_key)
            return

    # رسالة الترحيب مع أزرار الخدمات
    if user_id == ADMIN_ID:
        text = (
            "👋 <b>أهلاً بك في لوحة تحكم الإدارة!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🎛️ لوحة التحكم السريعة:\n"
            "├ /addfile - رفع ملف APK\n"
            "├ /listfiles - تصفح الملفات\n"
            "├ /deletefile - حذف ملف\n"
            "├ /stats - إحصائيات النظام\n"
            "├ /broadcast - إرسال رسالة للجميع\n"
            "└ /shutdown - إيقاف البوت"
        )
        keyboard = [
            [InlineKeyboardButton("📦 إضافة ملف جديد", callback_data="menu_addfile")],
            [
                InlineKeyboardButton("📂 الملفات", callback_data="menu_listfiles"),
                InlineKeyboardButton("📊 الإحصائيات", callback_data="menu_stats")
            ],
            [InlineKeyboardButton("👑 حسابي كمدير", callback_data="menu_profile")]
        ]
    else:
        text = (
            "✨ <b>مرحباً بك في نظام فتح الملفات!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "نحن نعتمد المجتمع لفتح الملفات المميزة.\n"
            "يمكنك المساهمة عبر مشاهدة الإعلانات 📹\n"
            "أو تسريع الفتح بشراء النجوم ⭐.\n\n"
            "اختر من القائمة أدناه لمعرفة المزيد:"
        )
        keyboard = [
            [
                InlineKeyboardButton("👤 حسابي ومساهماتي", callback_data="menu_profile"),
                InlineKeyboardButton("🏆 لوحة الشرف", callback_data="menu_leaderboard")
            ],
            [InlineKeyboardButton("⭐ شراء نجوم", url=f"https://t.me/{BOT_USERNAME}")],
            [InlineKeyboardButton("❓ كيفية عمل البوت", callback_data="menu_help")],
        ]

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _handle_support_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, file_key: str
) -> None:
    """معالجة دخول المستخدم عبر رابط الدعم - عرض أزرار اختيار عدد النجوم."""
    user_id = update.effective_user.id

    if is_banned(user_id):
        return

    data = load_data()

    if file_key not in data["files"]:
        await update.message.reply_text("هذا الملف غير موجود أو تم حذفه.")
        return

    file_data = data["files"][file_key]

    if file_data["current_stars"] >= file_data["total_stars"]:
        await update.message.reply_text("هذا الملف مكتمل الدعم بالفعل! شكرا لك.")
        return

    remaining = file_data["total_stars"] - file_data["current_stars"]
    progress = create_progress_bar(file_data["current_stars"], file_data["total_stars"])

    # أزرار اختيار عدد النجوم للدفع الحقيقي
    keyboard = [
        [
            InlineKeyboardButton("1 ⭐", callback_data=f"pay_1_{file_key}"),
            InlineKeyboardButton("5 ⭐", callback_data=f"pay_5_{file_key}"),
            InlineKeyboardButton("10 ⭐", callback_data=f"pay_10_{file_key}"),
        ],
        [
            InlineKeyboardButton("25 ⭐", callback_data=f"pay_25_{file_key}"),
            InlineKeyboardButton("50 ⭐", callback_data=f"pay_50_{file_key}"),
            InlineKeyboardButton("100 ⭐", callback_data=f"pay_100_{file_key}"),
        ],
    ]

    await update.message.reply_text(
        f"📦 {html_escape(file_data['name'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 التقدم: {progress}\n"
        f"⭐ المتبقي: {remaining} نجمة\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💫 اختر عدد النجوم للدعم:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _handle_watch_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, file_key: str
) -> None:
    """معالجة دخول المستخدم عبر رابط المشاهدة - عرض زر فتح صفحة الويب للمشاهدة."""
    user_id = update.effective_user.id

    if is_banned(user_id):
        return

    data = load_data()

    if file_key not in data["files"]:
        await update.message.reply_text("هذا الملف غير موجود أو تم حذفه.")
        return

    file_data = data["files"][file_key]

    if _is_file_fully_unlocked(file_data):
        await update.message.reply_text("هذا الملف مكتمل الدعم بالفعل! يمكنك تحميله مباشرة من القناة.")
        return

    current_views = file_data.get("current_views", 0)
    required_views = file_data.get("required_views", 0)
    
    if current_views >= required_views > 0:
        await update.message.reply_text("هذا الملف مكتمل الدعم بالفعل! يمكنك تحميله مباشرة من القناة.")
        return

    progress = create_progress_bar(current_views, required_views)
    
    # 🔒 توليد جلسة مشاهدة مؤمنة لمنع ثغرات الروابط
    if "active_watch_sessions" not in data:
        data["active_watch_sessions"] = {}
        
    watch_token = secrets.token_hex(12)
    
    # تخزين الجلسة ومشاركتها مع كود الـ WebApp
    data["active_watch_sessions"][watch_token] = {
        "user_id": user_id,
        "file_key": file_key,
        "start_time": time.time()
    }
    context.user_data["current_watch_token"] = watch_token
    
    # مسح جلسات هذا المستخدم القديمة
    for k in list(data["active_watch_sessions"].keys()):
        if data["active_watch_sessions"][k]["user_id"] == user_id and k != watch_token:
            del data["active_watch_sessions"][k]
            
    save_data(data)

    # نعطي الويب آب مفتاح الجلسة المشفر كأنه مفتاح الملف، ليعود به
    webapp_url = f"{WEBAPP_URL}?file_key={watch_token}"

    keyboard = [
        [
            InlineKeyboardButton(
                "📹 شاهد الإعلان للمساهمة",
                web_app=WebAppInfo(url=webapp_url),
            )
        ]
    ]

    await update.message.reply_text(
        f"📦 <b>{html_escape(file_data['name'])}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 تقدم المشاهدات: {progress}\n"
        f"📹 المتبقي: {max(0, required_views - current_views)} مشاهدة\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"اضغط على الزر أدناه لمشاهدة الإعلان والمساهمة في الفتح الجماعي:\n"
        f"<i>(ملاحظة: سيتم إشعارك تلقائياً عند اكتمال المشاهدة)</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_addfile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """أمر رفع ملف APK جديد (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    context.user_data["state"] = "waiting_apk"

    await update.message.reply_text(
        "📦 <b>رفع ملف جديد</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📎 أرسل ملف APK الآن:",
        parse_mode="HTML",
    )


async def cmd_listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض قائمة جميع الملفات (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    data = load_data()

    if not data["files"]:
        await update.message.reply_text("📂 لا توجد ملفات مرفوعة حالياً.")
        return

    text = "📂 <b>قائمة الملفات:</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for key, fdata in data["files"].items():
        progress = create_progress_bar(fdata["current_stars"], fdata["total_stars"])
        supporters_count = len(fdata.get("supporters", {}))
        completed = "✅" if fdata["current_stars"] >= fdata["total_stars"] else "⏳"

        text += (
            f"{completed} <b>{html_escape(fdata['name'])}</b>\n"
            f"   🔑 المفتاح: <code>{key}</code>\n"
            f"   📊 {progress}\n"
            f"   ⭐ {fdata['current_stars']}/{fdata['total_stars']}\n"
            f"   👥 الداعمون: {supporters_count}\n\n"
        )

    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض قائمة الملفات للحذف (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    data = load_data()

    if not data["files"]:
        await update.message.reply_text("📂 لا توجد ملفات للحذف.")
        return

    keyboard = []
    for key, fdata in data["files"].items():
        stars_info = f"{fdata['current_stars']}/{fdata['total_stars']}⭐"
        keyboard.append([
            InlineKeyboardButton(
                f"🗑️ {fdata['name']} ({stars_info})",
                callback_data=f"confirmdelete_{key}",
            )
        ])

    keyboard.append([
        InlineKeyboardButton("❌ إلغاء", callback_data="cancel_delete")
    ])

    await update.message.reply_text(
        "🗑️ <b>حذف ملف</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "اختر الملف الذي تريد حذفه:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض إحصائيات شاملة (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    data = load_data()

    total_files = len(data["files"])
    total_users = len(data["activated_users"])
    total_banned = len(data["banned_users"])
    total_violations = sum(len(v) for v in data["violations"].values())

    completed_files = sum(
        1 for f in data["files"].values()
        if f["current_stars"] >= f["total_stars"]
    )
    pending_files = total_files - completed_files

    total_stars_received = sum(
        f["current_stars"] for f in data["files"].values()
    )
    total_stars_needed = sum(
        f["total_stars"] for f in data["files"].values()
    )

    # أكثر الداعمين نشاطاً
    all_supporters = {}
    for fdata in data["files"].values():
        for uid, stars in fdata.get("supporters", {}).items():
            all_supporters[uid] = all_supporters.get(uid, 0) + stars

    top_supporters = sorted(all_supporters.items(), key=lambda x: x[1], reverse=True)[:5]
    top_text = ""
    for i, (uid, stars) in enumerate(top_supporters, 1):
        top_text += f"   {i}. <code>{uid}</code> → {stars}⭐\n"

    if not top_text:
        top_text = "   لا يوجد داعمون بعد.\n"

    text = (
        f"📊 <b>الإحصائيات الشاملة</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 <b>المستخدمون:</b>\n"
        f"   ├ المسجلون: {total_users}\n"
        f"   └ المحظورون: {total_banned}\n\n"
        f"📦 <b>الملفات:</b>\n"
        f"   ├ الإجمالي: {total_files}\n"
        f"   ├ المكتملة: {completed_files}\n"
        f"   └ قيد التقدم: {pending_files}\n\n"
        f"⭐ <b>النجوم:</b>\n"
        f"   ├ المستلمة: {total_stars_received}\n"
        f"   └ المطلوبة: {total_stars_needed}\n\n"
        f"⚠️ <b>المخالفات:</b> {total_violations}\n"
        f"📝 <b>السجلات:</b> {len(data['logs'])}\n\n"
        f"🏆 <b>أكثر الداعمين:</b>\n{top_text}"
    )

    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """بدء عملية الإرسال الجماعي (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    context.user_data["state"] = "waiting_broadcast"

    await update.message.reply_text(
        "📢 <b>إرسال جماعي</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "✍️ أرسل الرسالة التي تريد بثها\n"
        "لجميع المستخدمين المسجلين.\n\n"
        "💡 أرسل /cancel للإلغاء.",
        parse_mode="HTML",
    )


async def cmd_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """إيقاف البوت (المالك فقط)."""
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text(
        "🔴 <b>جاري إيقاف البوت...</b>\n"
        "سيتم إيقاف التشغيل خلال ثانية.",
        parse_mode="HTML",
    )

    data = load_data()
    add_log(data, "shutdown", ADMIN_ID, "Bot shutdown by owner")
    save_data(data)

    # إيقاف البوت – نستخدم SIGTERM على Linux/Railway، SIGINT كاحتياطي
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except (OSError, AttributeError):
        os.kill(os.getpid(), signal.SIGINT)


# ══════════════════════════════════════════════════════════════════
#  معالج الصور وملفات التوثيق (Media/Document Handler)
# ══════════════════════════════════════════════════════════════════

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الصور المرسلة لإرفاقها بالمنشور الجديد (المالك فقط)."""
    user_id = update.effective_user.id

    if user_id != ADMIN_ID:
        return

    state = context.user_data.get("state")
    if state != "waiting_post_image":
        return

    # التقط أكبر نسخة من الصورة
    photo = update.message.photo[-1]
    
    pending = context.user_data.get("pending_file")
    if not pending:
        await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
        context.user_data["state"] = None
        return
        
    pending["post_image"] = photo.file_id
    context.user_data["state"] = "waiting_stars_count"
    
    await update.message.reply_text(
        "✅ <b>تم استلام الصورة بنجاح!</b>\n\n"
        "⭐ الآن أدخل عدد النجوم المطلوبة لهذا الملف:",
        parse_mode="HTML",
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الملفات المرسلة (ملفات APK من المالك فقط)."""
    user_id = update.effective_user.id

    # المالك فقط يمكنه رفع الملفات
    if user_id != ADMIN_ID:
        return

    # التحقق من الحالة
    state = context.user_data.get("state")
    if state != "waiting_apk":
        return

    document = update.message.document

    # التحقق من امتداد الملف
    if not document.file_name or not (document.file_name.lower().endswith(".apk") or document.file_name.lower().endswith(".zip")):
        await update.message.reply_text(
            "❌ الرجاء إرسال ملف بصيغة (APK) أو (ZIP) فقط.",
            parse_mode="HTML",
        )
        return

    # حفظ معلومات الملف مؤقتاً
    context.user_data["pending_file"] = {
        "file_id": document.file_id,
        "file_name": document.file_name,
        "file_size": document.file_size,
    }
    context.user_data["state"] = "waiting_post_text"

    # حساب حجم الملف
    size_mb = round(document.file_size / (1024 * 1024), 2) if document.file_size else 0

    await update.message.reply_text(
        f"📥 <b>تم استلام الملف بنجاح!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 الاسم: {html_escape(document.file_name)}\n"
        f"📏 الحجم: {size_mb} MB\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"الآن أرسل الوصف (النص) الذي تريد ظهوره تحت الصورة.\n"
        f"💡 يمكنك استخدام HTML للتنسيق.",
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════════════════════════
#  معالج الرسائل النصية (Text Message Handler)
# ══════════════════════════════════════════════════════════════════

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الرسائل النصية حسب حالة المستخدم."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state")

    # ─── إلغاء العملية الحالية ───
    if text == "/cancel":
        context.user_data["state"] = None
        context.user_data["pending_file"] = None
        context.user_data["pending_support"] = None
        await update.message.reply_text("✅ تم إلغاء العملية.")
        return

    # ─── المالك: إدخال الوصف النصي ───
    if user_id == ADMIN_ID and state == "waiting_post_text":
        pending = context.user_data.get("pending_file")
        if not pending:
            await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
            context.user_data["state"] = None
            return
            
        pending["post_text"] = text
        context.user_data["state"] = "waiting_post_image"
        
        await update.message.reply_text(
            "🖼️ <b>جميل! الآن أرسل الصورة التي ستكون واجهة للمنشور.</b>",
            parse_mode="HTML",
        )
        return

    # ─── المالك: تجاوز الصورة ───
    if user_id == ADMIN_ID and state == "waiting_post_image" and text == "/skip":
        pending = context.user_data.get("pending_file")
        if not pending:
            await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
            context.user_data["state"] = None
            return
        
        pending["post_image"] = None
        context.user_data["state"] = "waiting_stars_count"
        await update.message.reply_text("⏩ تم تجاوز الصورة.\n\n⭐ أدخل عدد النجوم المطلوبة لهذا الملف:")
        return

    # ─── المالك: إدخال عدد النجوم لملف جديد ───
    if user_id == ADMIN_ID and state == "waiting_stars_count":
        await _process_stars_for_new_file(update, context, text)
        return

    # ─── المالك: إدخال عدد مشاهدات الإعلانات لملف جديد ───
    if user_id == ADMIN_ID and state == "waiting_views_count":
        await _process_views_for_new_file(update, context, text)
        return

    # ─── المالك: إرسال جماعي ───
    if user_id == ADMIN_ID and state == "waiting_broadcast":
        await _process_broadcast(update, context, text)
        return

    # (تم نقل دعم النجوم إلى نظام الدفع الحقيقي عبر send_invoice)


async def _process_stars_for_new_file(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """معالجة إدخال عدد النجوم لملف APK جديد - ثم السؤال عن عدد المشاهدات."""
    try:
        total_stars = int(text)
        if total_stars <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ أدخل رقماً صحيحاً أكبر من 0.")
        return
    pending = context.user_data.get("pending_file")

    if not pending:
        await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
        context.user_data["state"] = None
        return

    # حفظ عدد النجوم والانتقال لسؤال عدد المشاهدات
    pending["total_stars"] = total_stars
    context.user_data["pending_file"] = pending
    context.user_data["state"] = "waiting_views_count"

    await update.message.reply_text(
        f"✅ تم تحديد النجوم المطلوبة: {total_stars} ⭐\n\n"
        f"📹 كم عدد مشاهدات الإعلانات المطلوبة لفتح هذا الملف جماعياً؟\n"
        f"💡 أدخل 0 لتعطيل الفتح الجماعي عبر الإعلانات.",
    )


async def _process_views_for_new_file(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """معالجة إدخال عدد مشاهدات الإعلانات لملف APK جديد."""
    try:
        required_views = int(text)
        if required_views < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ أدخل رقماً صحيحاً (0 أو أكثر).")
        return
    pending = context.user_data.get("pending_file")

    if not pending or "total_stars" not in pending:
        await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
        context.user_data["state"] = None
        return

    total_stars = pending["total_stars"]

    # إنشاء مفتاح فريد للملف
    data = load_data()
    file_key = generate_file_key()
    while file_key in data["files"]:
        file_key = generate_file_key()

    # بناء بيانات الملف
    file_data = {
        "name": pending["file_name"],
        "file_id": pending["file_id"],
        "file_size": pending["file_size"],
        "post_text": pending.get("post_text", ""),
        "post_image": pending.get("post_image", None),
        "total_stars": total_stars,
        "current_stars": 0,
        "supporters": {},
        "required_views": required_views,
        "current_views": 0,
        "ad_viewers": [],
        "created_at": time.time(),
        "channel_message_id": None,
    }

    data["files"][file_key] = file_data
    add_log(data, "file_added", ADMIN_ID, f"Key: {file_key}, Name: {pending['file_name']}")
    save_data(data)

    # نشر الملف في القناة (النسخة الاحترافية: صورة + نص وقفل)
    channel_text = build_channel_message(file_data)
    keyboard = build_channel_keyboard(file_key, file_data)

    try:
        if file_data.get("post_image"):
            sent_msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=file_data["post_image"],
                caption=channel_text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            sent_msg = await context.bot.send_message(
                chat_id=CHANNEL_ID,
                text=channel_text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )

        # حفظ معرف رسالة القناة
        data = load_data()
        data["files"][file_key]["channel_message_id"] = sent_msg.message_id
        save_data(data)

    except Exception as e:
        logger.error("Failed to post to channel: %s", e)
        await update.message.reply_text(
            f"⚠️ تم حفظ الملف لكن فشل النشر في القناة.\n"
            f"الخطأ: <code>{html_escape(e)}</code>",
            parse_mode="HTML",
        )

    # مسح الحالة
    context.user_data["state"] = None
    context.user_data["pending_file"] = None

    await update.message.reply_text(
        f"✅ <b>تم النشر الاحترافي بنجاح!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 المفتاح: <code>{file_key}</code>\n"
        f"📦 الاسم: {html_escape(pending['file_name'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📢 تم تثبيت المنشور في القناة مع زر الفتح.",
        parse_mode="HTML",
    )


async def _process_broadcast(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """معالجة الإرسال الجماعي لجميع المستخدمين."""
    context.user_data["state"] = None

    data = load_data()
    users = data["activated_users"]

    if not users:
        await update.message.reply_text("📢 لا يوجد مستخدمون مسجلون.")
        return

    # إشعار البدء
    status_msg = await update.message.reply_text(
        f"📢 جاري الإرسال إلى {len(users)} مستخدم..."
    )

    success = 0
    failed = 0

    for uid in users:
        try:
            await context.bot.send_message(chat_id=uid, text=text)
            success += 1
        except Exception:
            failed += 1
        # تأخير بسيط لتجنب حدود Telegram
        await asyncio.sleep(0.05)

    add_log(data, "broadcast", ADMIN_ID, f"Success: {success}, Failed: {failed}")
    save_data(data)

    try:
        await status_msg.edit_text(
            f"📢 <b>تم الإرسال الجماعي:</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ نجح: <b>{success}</b>\n"
            f"❌ فشل: <b>{failed}</b>\n"
            f"📊 الإجمالي: <b>{len(users)}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await update.message.reply_text(
            f"📢 تم الإرسال: ✅{success} ❌{failed}"
        )


# ══════════════════════════════════════════════════════════════════
#  نظام الدفع الحقيقي بنجوم Telegram (XTR)
# ══════════════════════════════════════════════════════════════════

async def send_star_invoice(
    chat_id: int, file_key: str, amount: int, file_name: str,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """إرسال فاتورة نجوم Telegram الحقيقية."""
    await context.bot.send_invoice(
        chat_id=chat_id,
        title=f"دعم: {file_name}",
        description=f"دعم الملف بـ {amount} نجمة عبر Telegram Stars",
        payload=f"stars_{file_key}_{amount}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(f"{amount} نجمة", amount)],
    )


async def handle_pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """الموافقة التلقائية على طلبات الدفع المسبق."""
    query = update.pre_checkout_query
    payload = query.invoice_payload

    if not payload.startswith("stars_"):
        await query.answer(ok=False, error_message="فاتورة غير معروفة.")
        return

    parts = payload.split("_", 2)
    if len(parts) < 3:
        await query.answer(ok=False, error_message="بيانات غير صالحة.")
        return

    file_key = parts[1]
    data = load_data()

    if file_key not in data["files"]:
        await query.answer(ok=False, error_message="الملف لم يعد موجوداً.")
        return

    if query.from_user.id in data["banned_users"]:
        await query.answer(ok=False, error_message="تم حظرك من البوت.")
        return

    await query.answer(ok=True)


async def handle_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الدفع الناجح وتحديث عداد النجوم."""
    payment = update.message.successful_payment
    payload = payment.invoice_payload
    user_id = update.effective_user.id

    if not payload.startswith("stars_"):
        return

    parts = payload.split("_", 2)
    if len(parts) < 3:
        return

    file_key = parts[1]
    stars = int(parts[2])

    await _credit_stars(update, context, user_id, file_key, stars)


async def _credit_stars(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
    user_id: int, file_key: str, stars: int,
) -> None:
    """إضافة النجوم بعد تأكيد الدفع الحقيقي وتحديث القناة."""
    data = load_data()

    if file_key not in data["files"]:
        await update.message.reply_text("الملف لم يعد موجودا.")
        return

    file_data = data["files"][file_key]

    if file_data["current_stars"] >= file_data["total_stars"]:
        await update.message.reply_text("هذا الملف مكتمل الدعم بالفعل!")
        return

    # تحديث النجوم
    file_data["current_stars"] += stars

    uid = str(user_id)
    if uid not in file_data["supporters"]:
        file_data["supporters"][uid] = 0
    file_data["supporters"][uid] += stars

    if uid not in data["user_stats"]:
        data["user_stats"][uid] = {"actions": [], "total_stars": 0, "count": 0}
    data["user_stats"][uid]["total_stars"] += stars
    data["user_stats"][uid]["count"] += 1
    data["user_last_action"][uid] = time.time()

    add_log(data, "star_payment", user_id, f"Key: {file_key}, Stars: {stars}")
    save_data(data)

    # تحديث رسالة القناة
    completed = _is_file_fully_unlocked(file_data)
    msg_text = build_channel_message(file_data)
    kb = build_channel_keyboard(file_key, file_data)

    if file_data.get("channel_message_id"):
        try:
            if file_data.get("post_image"):
                await context.bot.edit_message_caption(
                    chat_id=CHANNEL_ID,
                    message_id=file_data["channel_message_id"],
                    caption=msg_text,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=CHANNEL_ID,
                    message_id=file_data["channel_message_id"],
                    text=msg_text,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
        except Exception as e:
            # إذا فشل التنسيق (غالباً بسبب خطأ في كود HTML من المالك)، محاولة الإرسال كنص مجرد مع تهريب
            logger.warning("HTML parsing failed, falling back to escaped text: %s", e)
            try:
                # محاولة الإرسال بنص آمن (مهرب بالكامل)
                safe_text = f"{html_escape(msg_text)}\n\n⚠️ (خطأ في تنسيق HTML من المالك)"
                if file_data.get("post_image"):
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID,
                        message_id=file_data["channel_message_id"],
                        caption=safe_text,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
                else:
                    await context.bot.edit_message_text(
                        chat_id=CHANNEL_ID,
                        message_id=file_data["channel_message_id"],
                        text=safe_text,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
            except Exception:
                pass

    # رسالة شكر
    progress = create_progress_bar(file_data["current_stars"], file_data["total_stars"])
    await update.message.reply_text(
        f"شكرا لدعمك!\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"تم اضافة: {stars} نجمة\n"
        f"التقدم: {progress}\n"
        f"الاجمالي: {file_data['current_stars']}/{file_data['total_stars']}",
        parse_mode="HTML"
    )

    # إشعار المالك وإرسال الملف للقناة عند الاكتمال
    if completed:
        await _unlock_file_for_channel(file_key, file_data, context, unlock_method="stars")


# ══════════════════════════════════════════════════════════════════
#  نظام الفتح الجماعي عبر إعلانات Monetag ومطابقة التحقق
# ══════════════════════════════════════════════════════════════════

async def _handle_view_completed(update: Update, context: ContextTypes.DEFAULT_TYPE, watch_token: str) -> None:
    """معالجة إتمام المشاهدة التي تأتي عبر زر (التحقق وإرسال للبوت) من المتصفح باستخدام جلسة مؤمنة."""
    user_id = update.effective_user.id
    
    if is_banned(user_id):
        return
        
    data = load_data()
    
    # 🔒 التحقق الأمني من الرمز
    if "active_watch_sessions" not in data or watch_token not in data["active_watch_sessions"]:
        await update.message.reply_text("❌ جلسة مشاهدة غير صالحة أو منتهية. يرجى البدء من زر الإعلان من جديد لتجنب التلاعب.")
        return
        
    session = data["active_watch_sessions"][watch_token]
    
    # التحقق من أن المستخدم هو صاحب الجلسة
    if session["user_id"] != user_id:
        await update.message.reply_text("❌ لا يمكنك استخدام رابط مشاهدة خاص بشخص آخر.")
        return
        
    # التحقق من أن الوقت المنقضي الفعلي على الخادم هو 10 ثانية على الأقل لضمان مشاهدة معتبرة
    time_elapsed = time.time() - session["start_time"]
    if time_elapsed < 10:
        remaining = int(10 - time_elapsed)
        await update.message.reply_text(f"⚠️ لقد عدت مبكراً جداً! يرجى مشاهدة الإعلان بشكل كامل. (حاول بعد {remaining} ثوانٍ)")
        return
        
    file_key = session["file_key"]
    
    # جلسة صحيحة! نمسحها فوراً لمنع التكرار (Replay attack)
    del data["active_watch_sessions"][watch_token]
    
    if file_key not in data["files"]:
        await update.message.reply_text("❌ الملف غير موجود أو تم حذفه.")
        save_data(data)
        return
        
    file_data = data["files"][file_key]
    
    # التحقق من أن الملف لم يُفتح بعد
    if _is_file_fully_unlocked(file_data):
        await update.message.reply_text(
            "✅ هذا الملف مفتوح بالفعل! جاري إرساله لك...",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_document(
                chat_id=user_id,
                document=file_data["file_id"],
                caption=f"📦 <b>الملف: {html_escape(file_data['name'])}</b>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return
        
    required_views = file_data.get("required_views", 0)
    if required_views <= 0:
        await update.message.reply_text("❌ هذا الملف لا يدعم الفتح الجماعي عبر الإعلانات.")
        save_data(data)
        return

    now = time.time()
    # زيادة عداد المشاهدات
    file_data["current_views"] = file_data.get("current_views", 0) + 1

    # تسجيل المشاهد
    if "ad_viewers" not in file_data:
        file_data["ad_viewers"] = []
    file_data["ad_viewers"].append({
        "user_id": user_id,
        "timestamp": now,
    })

    add_log(data, "ad_view", user_id, f"Key: {file_key}, Views: {file_data['current_views']}/{required_views}")
    save_data(data)

    current_views = file_data["current_views"]

    # رسالة شكر للمستخدم
    await update.message.reply_text("🎉 تهانينا! تم التحقق بنجاح واحتساب مشاهدتك.\n\nيمكنك الآن استكمال الدعم أو تحميل الملف إذا اكتمل الشريط في القناة.")

    # تحديث رسالة القناة للوضع الاحترافي
    current_views = file_data["current_views"]
    required_views = file_data["required_views"]
    
    channel_text = build_channel_message(file_data)
    kb = build_channel_keyboard(file_key, file_data)

    if file_data.get("channel_message_id"):
        try:
            if file_data.get("post_image"):
                await context.bot.edit_message_caption(
                    chat_id=CHANNEL_ID,
                    message_id=file_data["channel_message_id"],
                    caption=channel_text,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=CHANNEL_ID,
                    message_id=file_data["channel_message_id"],
                    text=channel_text,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
        except Exception as e:
            logger.warning("HTML parsing failed in view update: %s", e)
            try:
                safe_text = f"{html_escape(channel_text)}\n\n⚠️ (تنسيق الوصف به خلل)"
                if file_data.get("post_image"):
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID,
                        message_id=file_data["channel_message_id"],
                        caption=safe_text,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
                else:
                    await context.bot.edit_message_text(
                        chat_id=CHANNEL_ID,
                        message_id=file_data["channel_message_id"],
                        text=safe_text,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
            except Exception:
                pass

    # عند اكتمال المشاهدات -> إرسال الملف فوراً للمستخدم الحالي ونشره في القناة
    if unlocked:
        # إرسال الملف في القناة
        await _unlock_file_for_channel(file_key, file_data, context, unlock_method="ad_views")
        
        # إرسال الملف لهذا المستخدم تحديداً كمكافأة فورية
        try:
            await context.bot.send_document(
                chat_id=user_id,
                document=file_data["file_id"],
                caption=(
                    f"🎉 <b>تهانينا! لقد كنت الشخص الذي فتح الملف للجميع.</b>\n\n"
                    f"📦 <b>الملف: {html_escape(file_data['name'])}</b>"
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass


async def _unlock_file_for_channel(
    file_key: str, file_data: dict,
    context: ContextTypes.DEFAULT_TYPE,
    unlock_method: str = "stars",
) -> None:
    """إرسال الملف المفتوح للقناة وإشعار المالك."""
    # إرسال الملف الفعلي للقناة
    try:
        if unlock_method == "ad_views":
            caption = f"✅ <b>تم فتح الملف جماعياً بالإعلانات!</b>\n\n📦 الملف: {html_escape(file_data['name'])}"
        else:
            caption = f"✅ <b>تم فك القفل بنجاح!</b>\n\n📦 الملف: {html_escape(file_data['name'])}"

        await context.bot.send_document(
            chat_id=CHANNEL_ID,
            document=file_data["file_id"],
            caption=caption,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Failed to send unlocked file to channel: %s", e)

    # إشعار المالك
    viewers_count = len(file_data.get("ad_viewers", []))
    supporters_count = len(file_data.get("supporters", {}))

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🔓 <b>تم فتح ملف!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 الملف: {html_escape(file_data['name'])}\n"
                f"🔑 المفتاح: <code>{file_key}</code>\n"
                f"🔓 طريقة الفتح: {'إعلانات جماعية 📹' if unlock_method == 'ad_views' else 'نجوم ⭐'}\n"
                f"⭐ النجوم: {file_data['current_stars']}/{file_data['total_stars']}\n"
                f"📹 المشاهدات: {file_data.get('current_views', 0)}/{file_data.get('required_views', 0)}\n"
                f"👥 الداعمون بالنجوم: {supporters_count}\n"
                f"👁️ المشاهدون: {viewers_count}"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Failed to notify owner about unlock: %s", e)


async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة إشارات WebApp للـ Rewarded Ads (view_completed)."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    # استلام البيانات من WebApp
    raw_data = update.effective_message.web_app_data.data
    logger.info(f"WebApp Data received from {user_id}: {raw_data}")
    
    if raw_data == "view_completed":
        data = load_data()
        watch_token = None
        
        # البحث عن الجلسة النشطة لهذا المستخدم في قاعدة البيانات
        if "active_watch_sessions" in data:
            for token, session in data["active_watch_sessions"].items():
                if session["user_id"] == user_id:
                    watch_token = token
                    break
        
        # إذا لم نجدها، نجرب في context
        if not watch_token:
            watch_token = context.user_data.get("current_watch_token")
            
        if watch_token:
            # رسالة فورية حسب طلب المستخدم
            await update.message.reply_text(
                "✅ <b>تم تأكيد مشاهدة الإعلان بنجاح! شكراً لمساهمتك.</b>",
                parse_mode="HTML"
            )
            # معالجة إكمال المشاهدة (زيادة العداد وتحديث القناة)
            await _handle_view_completed(update, context, watch_token)
        else:
            await update.message.reply_text(
                "❌ <b>عذراً! انتهت صلاحية جلسة المشاهدة.</b>\n"
                "يرجى المحاولة مرة أخرى من زر المشاهدة في المنشور.",
                parse_mode="HTML"
            )


# ══════════════════════════════════════════════════════════════════
#  معالج أزرار الاستجابة (Callback Query Handler)
# ══════════════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة ضغطات الأزرار."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    cb_data = query.data

    # ─── دفع نجوم حقيقي (pay_AMOUNT_FILEKEY) ───
    if cb_data.startswith("pay_"):
        parts = cb_data.split("_", 2)
        if len(parts) == 3:
            amount = int(parts[1])
            file_key = parts[2]
            data = load_data()
            if file_key in data["files"]:
                file_name = data["files"][file_key]["name"]
                await send_star_invoice(user_id, file_key, amount, file_name, context)
            else:
                await query.edit_message_text("الملف لم يعد موجودا.")
        return

    # ─── أزرار القائمة الرئيسية ───
    if cb_data == "menu_addfile":
        if user_id == ADMIN_ID:
            context.user_data["state"] = "waiting_apk"
            await query.edit_message_text("📦 أرسل ملف APK الآن:")
        else:
            await query.answer("🚫 غير مصرح لك باستخدام لوحة الإدارة.", show_alert=True)
        return

    if cb_data == "menu_listfiles":
        if user_id == ADMIN_ID:
            await query.edit_message_text("استخدم /listfiles لعرض القائمة.")
        else:
            await query.answer("🚫 غير مصرح لك.", show_alert=True)
        return

    if cb_data == "menu_stats":
        if user_id == ADMIN_ID:
            await query.edit_message_text("استخدم /stats لعرض الإحصائيات.")
        else:
            await query.answer("🚫 غير مصرح لك.", show_alert=True)
        return

    if cb_data == "menu_help":
        await query.edit_message_text(
            "❓ <b>كيفية العمل</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ <b>طريقة النجوم ⭐</b>: يمكنك التبرع بنجوم تيليجرام لتسريع فتح الملف فورا.\n"
            "2️⃣ <b>المشاهدات المجانية 📹</b>: بدلاً من الدفع، يكفيك مشاهدة إعلان قصير لمدة 30 ثانية لتسجيل نقطة مساهمة.\n\n"
            "عند وصول العداد إلى الرقم المطلوب، سيفتح الملف للجميع في القناة تلقائياً! 🔓",
            parse_mode="HTML",
        )
        return
        
    if cb_data == "menu_profile":
        data = load_data()
        uid = str(user_id)
        stats = data["user_stats"].get(uid, {"total_stars": 0, "count": 0})
        
        # حساب المشاهدات الخاصة به
        total_views = 0
        for file_data in data["files"].values():
            for viewer in file_data.get("ad_viewers", []):
                if viewer["user_id"] == user_id:
                    total_views += 1
                    
        await query.edit_message_text(
            f"👤 <b>الملف الشخصي</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 الايدي: <code>{user_id}</code>\n"
            f"⭐ مجموع النجوم المهداة: <b>{stats['total_stars']}</b>\n"
            f"📹 الإعلانات المشاهدة: <b>{total_views}</b>\n\n"
            f"شكراً لمساهمتك العظيمة في المجتمع! 🌟",
            parse_mode="HTML"
        )
        return
        
    if cb_data == "menu_leaderboard":
        data = load_data()
        
        # أكثر الداعمين نشاطاً بالنجوم
        all_supporters = {}
        for fdata in data["files"].values():
            for uid, stars in fdata.get("supporters", {}).items():
                all_supporters[uid] = all_supporters.get(uid, 0) + stars

        top_supporters = sorted(all_supporters.items(), key=lambda x: x[1], reverse=True)[:5]
        top_text = ""
        for i, (uid, stars) in enumerate(top_supporters, 1):
            badge = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🏅"
            top_text += f"{badge} المستتر <code>{uid}</code> ← <b>{stars}</b> ⭐\n"

        if not top_text:
            top_text = "لا يوجد داعمون بالنجوم حتى الآن.\n"
            
        await query.edit_message_text(
            f"🏆 <b>لوحة الشرف الذهبية</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{top_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"بفضلكم تستمر الخدمات مجانية! ✨",
            parse_mode="HTML"
        )
        return

    # ─── تأكيد حذف ملف ───
    if cb_data.startswith("confirmdelete_"):
        if user_id != ADMIN_ID:
            await query.answer("🚫 غير مصرح لك.", show_alert=True)
            return

        file_key = cb_data[14:]  # إزالة "confirmdelete_"

        # زر تأكيد الحذف
        keyboard = [
            [
                InlineKeyboardButton("✅ نعم، احذف", callback_data=f"dodelete_{file_key}"),
                InlineKeyboardButton("❌ إلغاء", callback_data="cancel_delete"),
            ]
        ]

        data = load_data()
        if file_key in data["files"]:
            file_name = data["files"][file_key]["name"]
            await query.edit_message_text(
                f"⚠️ <b>هل أنت متأكد من حذف:</b>\n\n"
                f"📦 {html_escape(file_name)}\n"
                f"🔑 <code>{file_key}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await query.edit_message_text("❌ الملف غير موجود.")
        return

    # ─── تنفيذ الحذف ───
    if cb_data.startswith("dodelete_"):
        if user_id != ADMIN_ID:
            await query.answer("🚫 غير مصرح لك.", show_alert=True)
            return

        file_key = cb_data[9:]  # إزالة "dodelete_"
        data = load_data()

        if file_key in data["files"]:
            file_name = data["files"][file_key]["name"]
            del data["files"][file_key]
            add_log(data, "file_deleted", user_id, f"Key: {file_key}, Name: {file_name}")
            save_data(data)

            await query.edit_message_text(
                f"✅ <b>تم حذف الملف بنجاح:</b>\n\n"
                f"📦 {html_escape(file_name)}\n"
                f"🔑 <code>{file_key}</code>",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text("❌ الملف غير موجود أو تم حذفه مسبقاً.")
        return

    # ─── إلغاء الحذف ───
    if cb_data == "cancel_delete":
        if user_id != ADMIN_ID:
            return
        await query.edit_message_text("✅ تم إلغاء عملية الحذف.")
        return


# ══════════════════════════════════════════════════════════════════
#  معالج الأخطاء العامة
# ══════════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الأخطاء غير المتوقعة وتسجيلها."""
    logger.error("Exception while handling an update:", exc_info=context.error)

    data = load_data()
    add_log(data, "error", 0, str(context.error)[:200])
    save_data(data)

    # إشعار المالك بالخطأ
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🔴 <b>خطأ في البوت:</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{html_escape(str(context.error)[:500])}</code>"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
#  دالة بدء التشغيل (post_init)
# ══════════════════════════════════════════════════════════════════

async def post_init(application) -> None:
    """يتم استدعاؤها بعد تهيئة التطبيق بنجاح."""
    bot_info = await application.bot.get_me()
    logger.info("🤖 Bot started: @%s (ID: %s)", bot_info.username, bot_info.id)

    # تعيين قائمة الأوامر (Menu) للمستخدمين العاديين
    user_commands = [
        BotCommand("start", "بدء البوت"),
    ]
    await application.bot.set_my_commands(user_commands)

    # تعيين قائمة الأوامر للمالك فقط بجانب خانة الكتابة
    admin_commands = [
        BotCommand("start", "بدء البوت"),
        BotCommand("addfile", "رفع ملف APK"),
        BotCommand("listfiles", "قائمة الملفات"),
        BotCommand("deletefile", "حذف ملف"),
        BotCommand("stats", "الإحصائيات"),
        BotCommand("broadcast", "إرسال جماعي"),
    ]
    await application.bot.set_my_commands(
        admin_commands, scope=BotCommandScopeChat(chat_id=ADMIN_ID)
    )
    logger.info("✅ Bot commands menu set")

    # إرسال إشعار للمالك
    try:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🟢 <b>البوت يعمل الآن!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 @{bot_info.username}\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
#  نقطة الدخول الرئيسية
# ══════════════════════════════════════════════════════════════════

def main() -> None:
    """تشغيل البوت."""

    # ── التحقق من التوكن ──
    if not BOT_TOKEN:
        logger.error(
            "❌ BOT_TOKEN environment variable is not set!\n"
            "Set it with: export BOT_TOKEN='your-token-here'"
        )
        return

    # ── تهيئة ملف البيانات ──
    load_data()
    logger.info("✅ Data file initialized")

    # ── تشغيل خادم HTTP الصحي (Render / Railway) ──
    health_thread = threading.Thread(target=_start_health_server, daemon=True)
    health_thread.start()
    logger.info("✅ Health server thread started")

    # ── بناء التطبيق مع إعدادات الاتصال المحسّنة ──
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .post_init(post_init)
        .build()
    )

    # ── تسجيل أوامر المالك ──
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("addfile", cmd_addfile))
    application.add_handler(CommandHandler("listfiles", cmd_listfiles))
    application.add_handler(CommandHandler("deletefile", cmd_deletefile))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("broadcast", cmd_broadcast))
    application.add_handler(CommandHandler("shutdown", cmd_shutdown))

    # ── تسجيل معالجات الرسائل ──
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, handle_successful_payment))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # ── تسجيل معالج الدفع المسبق ──
    application.add_handler(PreCheckoutQueryHandler(handle_pre_checkout))

    # ── تسجيل معالج الأزرار ──
    application.add_handler(CallbackQueryHandler(handle_callback))

    # ── تسجيل معالج الأخطاء ──
    application.add_error_handler(error_handler)

    # ── بدء التشغيل بنظام Polling ──
    logger.info("🤖 Bot is starting with polling...")
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
