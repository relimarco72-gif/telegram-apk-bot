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
import logging
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    BotCommand,
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
OWNER_ID = 7246473970
CHANNEL_ID = -1002489542574
BOT_USERNAME = "AE_Mode_bot"
DATA_FILE = "data.json"

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
}


def load_data() -> dict:
    """تحميل البيانات من data.json، إنشاء الملف تلقائياً إذا لم يكن موجوداً."""
    if not os.path.exists(DATA_FILE):
        save_data(DEFAULT_DATA.copy())
        return DEFAULT_DATA.copy()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # التأكد من وجود كل المفاتيح المطلوبة
        for key, default_val in DEFAULT_DATA.items():
            if key not in data:
                data[key] = type(default_val)()
        return data
    except (json.JSONDecodeError, IOError):
        save_data(DEFAULT_DATA.copy())
        return DEFAULT_DATA.copy()


def save_data(data: dict) -> None:
    """حفظ البيانات إلى data.json."""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
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

def is_owner(user_id: int) -> bool:
    """التحقق مما إذا كان المستخدم هو المالك."""
    return user_id == OWNER_ID


def is_banned(user_id: int) -> bool:
    """التحقق مما إذا كان المستخدم محظوراً."""
    data = load_data()
    return user_id in data["banned_users"]


def generate_file_key() -> str:
    """إنشاء مفتاح عشوائي فريد للملف (8 أحرف)."""
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choices(chars, k=8))


def escape_md(text: str) -> str:
    """تهريب الأحرف الخاصة في Markdown لتجنب أخطاء parse_entities."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


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


def build_channel_message(file_data: dict) -> str:
    """بناء نص رسالة القناة مع شريط التقدم."""
    name = file_data["name"]
    current = file_data["current_stars"]
    total = file_data["total_stars"]
    progress = create_progress_bar(current, total)

    if current >= total:
        status = "✅ مكتمل"
    else:
        status = f"⭐ {current}/{total}"

    safe_name = escape_md(name)
    text = (
        f"📦 *{safe_name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 التقدم: {progress}\n"
        f"⭐ النجوم: {status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    if current >= total:
        text += "🎉 تم اكتمال الدعم! شكراً لكم جميعاً!"
    else:
        text += "💫 ادعم هذا الملف بالنجوم عبر الزر أدناه!"

    return text


def build_channel_keyboard(file_key: str, completed: bool = False):
    """بناء لوحة أزرار رسالة القناة."""
    if completed:
        return None  # إزالة الزر عند الاكتمال

    keyboard = [[
        InlineKeyboardButton(
            "⭐ ادعم بالنجوم",
            url=f"https://t.me/{BOT_USERNAME}?start=support_{file_key}",
        )
    ]]
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
            chat_id=OWNER_ID,
            text=(
                f"⚠️ *مخالفة جديدة*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 المستخدم: `{user_id}`\n"
                f"📋 السبب: {reason_ar}\n"
                f"🔢 عدد المخالفات: {count}/{MAX_VIOLATIONS}"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("Failed to notify owner about violation: %s", e)

    # الحظر التلقائي عند بلوغ الحد الأقصى
    if count >= MAX_VIOLATIONS and user_id not in data["banned_users"]:
        data["banned_users"].append(user_id)
        add_log(data, "auto_ban", user_id, f"Banned after {MAX_VIOLATIONS} violations")

        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"🚫 *تم حظر مستخدم تلقائياً!*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 المستخدم: `{user_id}`\n"
                    f"📋 السبب: تجاوز {MAX_VIOLATIONS} مخالفات\n"
                    f"🕐 الوقت: {time.strftime('%Y-%m-%d %H:%M:%S')}"
                ),
                parse_mode="Markdown",
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
    if is_owner(user_id):
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

    # التحقق من Deep Linking (دعم النجوم)
    if context.args and len(context.args) > 0:
        arg = context.args[0]
        if arg.startswith("support_"):
            file_key = arg[8:]  # إزالة البادئة "support_"
            await _handle_support_entry(update, context, file_key)
            return

    # رسالة الترحيب مع أزرار الخدمات
    if is_owner(user_id):
        text = (
            "👋 مرحبا أيها المالك!\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🎛️ لوحة التحكم:\n"
            "├ /addfile - رفع ملف APK\n"
            "├ /listfiles - قائمة الملفات\n"
            "├ /deletefile - حذف ملف\n"
            "├ /stats - الإحصائيات\n"
            "├ /broadcast - إرسال جماعي\n"
            "└ /shutdown - إيقاف البوت"
        )
        keyboard = [
            [InlineKeyboardButton("📦 رفع ملف", callback_data="menu_addfile")],
            [InlineKeyboardButton("📂 قائمة الملفات", callback_data="menu_listfiles")],
            [InlineKeyboardButton("📊 الإحصائيات", callback_data="menu_stats")],
        ]
    else:
        text = (
            "👋 مرحبا بك!\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "يمكنك دعم الملفات بالنجوم عبر\n"
            "أزرار الدعم في القناة.\n\n"
            "⭐ شكرا لدعمك!"
        )
        keyboard = [
            [InlineKeyboardButton("⭐ شراء نجوم", url=f"https://t.me/{BOT_USERNAME}")],
            [InlineKeyboardButton("❓ مساعدة", callback_data="menu_help")],
        ]

    await update.message.reply_text(
        text,
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
        f"📦 {file_data['name']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 التقدم: {progress}\n"
        f"⭐ المتبقي: {remaining} نجمة\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💫 اختر عدد النجوم للدعم:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_addfile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """أمر رفع ملف APK جديد (المالك فقط)."""
    if not is_owner(update.effective_user.id):
        return

    context.user_data["state"] = "waiting_apk"

    await update.message.reply_text(
        "📦 *رفع ملف جديد*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📎 أرسل ملف APK الآن:",
        parse_mode="Markdown",
    )


async def cmd_listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض قائمة جميع الملفات (المالك فقط)."""
    if not is_owner(update.effective_user.id):
        return

    data = load_data()

    if not data["files"]:
        await update.message.reply_text("📂 لا توجد ملفات مرفوعة حالياً.")
        return

    text = "📂 *قائمة الملفات:*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for key, fdata in data["files"].items():
        progress = create_progress_bar(fdata["current_stars"], fdata["total_stars"])
        supporters_count = len(fdata.get("supporters", {}))
        completed = "✅" if fdata["current_stars"] >= fdata["total_stars"] else "⏳"

        text += (
            f"{completed} *{escape_md(fdata['name'])}*\n"
            f"   🔑 المفتاح: `{key}`\n"
            f"   📊 {progress}\n"
            f"   ⭐ {fdata['current_stars']}/{fdata['total_stars']}\n"
            f"   👥 الداعمون: {supporters_count}\n\n"
        )

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض قائمة الملفات للحذف (المالك فقط)."""
    if not is_owner(update.effective_user.id):
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
        "🗑️ *حذف ملف*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "اختر الملف الذي تريد حذفه:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """عرض إحصائيات شاملة (المالك فقط)."""
    if not is_owner(update.effective_user.id):
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
        top_text += f"   {i}. `{uid}` → {stars}⭐\n"

    if not top_text:
        top_text = "   لا يوجد داعمون بعد.\n"

    text = (
        f"📊 *الإحصائيات الشاملة*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 *المستخدمون:*\n"
        f"   ├ المسجلون: {total_users}\n"
        f"   └ المحظورون: {total_banned}\n\n"
        f"📦 *الملفات:*\n"
        f"   ├ الإجمالي: {total_files}\n"
        f"   ├ المكتملة: {completed_files}\n"
        f"   └ قيد التقدم: {pending_files}\n\n"
        f"⭐ *النجوم:*\n"
        f"   ├ المستلمة: {total_stars_received}\n"
        f"   └ المطلوبة: {total_stars_needed}\n\n"
        f"⚠️ *المخالفات:* {total_violations}\n"
        f"📝 *السجلات:* {len(data['logs'])}\n\n"
        f"🏆 *أكثر الداعمين:*\n{top_text}"
    )

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """بدء عملية الإرسال الجماعي (المالك فقط)."""
    if not is_owner(update.effective_user.id):
        return

    context.user_data["state"] = "waiting_broadcast"

    await update.message.reply_text(
        "📢 *إرسال جماعي*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "✍️ أرسل الرسالة التي تريد بثها\n"
        "لجميع المستخدمين المسجلين.\n\n"
        "💡 أرسل /cancel للإلغاء.",
        parse_mode="Markdown",
    )


async def cmd_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """إيقاف البوت (المالك فقط)."""
    if not is_owner(update.effective_user.id):
        return

    await update.message.reply_text(
        "🔴 *جاري إيقاف البوت...*\n"
        "سيتم إيقاف التشغيل خلال ثانية.",
        parse_mode="Markdown",
    )

    data = load_data()
    add_log(data, "shutdown", OWNER_ID, "Bot shutdown by owner")
    save_data(data)

    # إيقاف البوت – نستخدم SIGTERM على Linux/Railway، SIGINT كاحتياطي
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except (OSError, AttributeError):
        os.kill(os.getpid(), signal.SIGINT)


# ══════════════════════════════════════════════════════════════════
#  معالج الملفات (Document Handler)
# ══════════════════════════════════════════════════════════════════

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الملفات المرسلة (ملفات APK من المالك فقط)."""
    user_id = update.effective_user.id

    # المالك فقط يمكنه رفع الملفات
    if not is_owner(user_id):
        return

    # التحقق من الحالة
    state = context.user_data.get("state")
    if state != "waiting_apk":
        return

    document = update.message.document

    # التحقق من امتداد الملف
    if not document.file_name or not document.file_name.lower().endswith(".apk"):
        await update.message.reply_text(
            "❌ الرجاء إرسال ملف بصيغة *.apk* فقط.",
            parse_mode="Markdown",
        )
        return

    # حفظ معلومات الملف مؤقتاً
    context.user_data["pending_file"] = {
        "file_id": document.file_id,
        "file_name": document.file_name,
        "file_size": document.file_size,
    }
    context.user_data["state"] = "waiting_stars_count"

    # حساب حجم الملف
    size_mb = round(document.file_size / (1024 * 1024), 2) if document.file_size else 0

    await update.message.reply_text(
        f"✅ تم استلام الملف:\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 الاسم: {document.file_name}\n"
        f"📏 الحجم: {size_mb} MB\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⭐ أدخل عدد النجوم المطلوبة لهذا الملف:",
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

    # ─── المالك: إدخال عدد النجوم لملف جديد ───
    if is_owner(user_id) and state == "waiting_stars_count":
        await _process_stars_for_new_file(update, context, text)
        return

    # ─── المالك: إرسال جماعي ───
    if is_owner(user_id) and state == "waiting_broadcast":
        await _process_broadcast(update, context, text)
        return

    # (تم نقل دعم النجوم إلى نظام الدفع الحقيقي عبر send_invoice)


async def _process_stars_for_new_file(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """معالجة إدخال عدد النجوم لملف APK جديد."""
    if not text.isdigit() or int(text) <= 0:
        await update.message.reply_text("❌ أدخل رقماً صحيحاً أكبر من 0.")
        return

    total_stars = int(text)
    pending = context.user_data.get("pending_file")

    if not pending:
        await update.message.reply_text("❌ حدث خطأ. استخدم /addfile للبدء من جديد.")
        context.user_data["state"] = None
        return

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
        "total_stars": total_stars,
        "current_stars": 0,
        "supporters": {},
        "created_at": time.time(),
        "channel_message_id": None,
    }

    data["files"][file_key] = file_data
    add_log(data, "file_added", OWNER_ID, f"Key: {file_key}, Name: {pending['file_name']}")
    save_data(data)

    # نشر الملف في القناة مع شريط التقدم وزر الدعم
    msg_text = build_channel_message(file_data)
    keyboard = build_channel_keyboard(file_key)

    try:
        sent_msg = await context.bot.send_message(
            chat_id=CHANNEL_ID,
            text=msg_text,
            parse_mode="Markdown",
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
            f"الخطأ: `{e}`",
            parse_mode="Markdown",
        )

    # مسح الحالة
    context.user_data["state"] = None
    context.user_data["pending_file"] = None

    await update.message.reply_text(
        f"✅ *تم نشر الملف بنجاح!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 المفتاح: `{file_key}`\n"
        f"📦 الاسم: {escape_md(pending['file_name'])}\n"
        f"⭐ النجوم المطلوبة: {total_stars}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📢 تم النشر في القناة.",
        parse_mode="Markdown",
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

    add_log(data, "broadcast", OWNER_ID, f"Success: {success}, Failed: {failed}")
    save_data(data)

    try:
        await status_msg.edit_text(
            f"📢 *تم الإرسال الجماعي:*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ نجح: {success}\n"
            f"❌ فشل: {failed}\n"
            f"📊 الإجمالي: {len(users)}",
            parse_mode="Markdown",
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
    completed = file_data["current_stars"] >= file_data["total_stars"]
    msg_text = build_channel_message(file_data)
    kb = build_channel_keyboard(file_key, completed)

    if file_data.get("channel_message_id"):
        try:
            await context.bot.edit_message_text(
                chat_id=CHANNEL_ID,
                message_id=file_data["channel_message_id"],
                text=msg_text,
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except Exception as e:
            logger.warning("Could not update channel message: %s", e)

    # رسالة شكر
    progress = create_progress_bar(file_data["current_stars"], file_data["total_stars"])
    await update.message.reply_text(
        f"شكرا لدعمك!\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"تم اضافة: {stars} نجمة\n"
        f"التقدم: {progress}\n"
        f"الاجمالي: {file_data['current_stars']}/{file_data['total_stars']}",
    )

    # إشعار المالك وإرسال الملف للقناة عند الاكتمال
    if completed:
        # إرسال الملف الفعلي للقناة
        try:
            await context.bot.send_document(
                chat_id=CHANNEL_ID,
                document=file_data["file_id"],
                caption=f"✅ تم فك القفل بنجاح!\n\n📦 الملف: {escape_md(file_data['name'])}",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error("Failed to send unlocked file to channel: %s", e)

        # إشعار المالك
        supporters_list = ""
        for s_uid, s_stars in file_data["supporters"].items():
            supporters_list += f"   {s_uid} = {s_stars}\n"

        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"اكتمل دعم ملف!\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"الملف: {file_data['name']}\n"
                    f"المفتاح: {file_key}\n"
                    f"النجوم: {file_data['current_stars']}/{file_data['total_stars']}\n"
                    f"الداعمون ({len(file_data['supporters'])}):\n"
                    f"{supporters_list}"
                ),
            )
        except Exception as e:
            logger.error("Failed to notify owner about completion: %s", e)


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
        if is_owner(user_id):
            context.user_data["state"] = "waiting_apk"
            await query.edit_message_text("📦 أرسل ملف APK الآن:")
        return

    if cb_data == "menu_listfiles":
        if is_owner(user_id):
            await query.edit_message_text("استخدم /listfiles لعرض القائمة.")
        return

    if cb_data == "menu_stats":
        if is_owner(user_id):
            await query.edit_message_text("استخدم /stats لعرض الإحصائيات.")
        return

    if cb_data == "menu_help":
        await query.edit_message_text(
            "❓ المساعدة\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "يمكنك دعم الملفات بالنجوم عبر\n"
            "الضغط على زر الدعم في القناة.\n\n"
            "سيتم فتح نافذة دفع Telegram Stars\n"
            "الرسمية لإتمام العملية.",
        )
        return

    # ─── تأكيد حذف ملف ───
    if cb_data.startswith("confirmdelete_"):
        if not is_owner(user_id):
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
                f"⚠️ *هل أنت متأكد من حذف:*\n\n"
                f"📦 {escape_md(file_name)}\n"
                f"🔑 `{file_key}`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await query.edit_message_text("❌ الملف غير موجود.")
        return

    # ─── تنفيذ الحذف ───
    if cb_data.startswith("dodelete_"):
        if not is_owner(user_id):
            return

        file_key = cb_data[9:]  # إزالة "dodelete_"
        data = load_data()

        if file_key in data["files"]:
            file_name = data["files"][file_key]["name"]
            del data["files"][file_key]
            add_log(data, "file_deleted", user_id, f"Key: {file_key}, Name: {file_name}")
            save_data(data)

            await query.edit_message_text(
                f"✅ *تم حذف الملف بنجاح:*\n\n"
                f"📦 {escape_md(file_name)}\n"
                f"🔑 `{file_key}`",
                parse_mode="Markdown",
            )
        else:
            await query.edit_message_text("❌ الملف غير موجود أو تم حذفه مسبقاً.")
        return

    # ─── إلغاء الحذف ───
    if cb_data == "cancel_delete":
        if not is_owner(user_id):
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
            chat_id=OWNER_ID,
            text=(
                f"🔴 *خطأ في البوت:*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"`{str(context.error)[:500]}`"
            ),
            parse_mode="Markdown",
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

    # تعيين قائمة الأوامر (Menu) بجانب خانة الكتابة
    commands = [
        BotCommand("start", "بدء البوت"),
        BotCommand("addfile", "رفع ملف APK"),
        BotCommand("listfiles", "قائمة الملفات"),
        BotCommand("deletefile", "حذف ملف"),
        BotCommand("stats", "الإحصائيات"),
        BotCommand("broadcast", "إرسال جماعي"),
    ]
    await application.bot.set_my_commands(commands)
    logger.info("✅ Bot commands menu set")

    # إرسال إشعار للمالك
    try:
        await application.bot.send_message(
            chat_id=OWNER_ID,
            text=(
                f"🟢 البوت يعمل الآن!\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 @{bot_info.username}\n"
                f"🕐 {time.strftime('%Y-%m-%d %H:%M:%S')}"
            ),
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
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
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
