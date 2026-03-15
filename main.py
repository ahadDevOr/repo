import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler,
    filters, ContextTypes
)
from database import (
    init_db, save_child_bot, get_user_bots,
    get_bot, delete_child_bot, get_all_active_bots, set_setting
)
from child_runner import (
    start_child_bot, stop_child_bot, restart_child_bot,
    is_bot_running, get_running_count
)

# ── তোমার Main Bot Token ──────────────────────────────────────
import os
MAIN_BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
# ─────────────────────────────────────────────────────────────

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

ASK_TOKEN, ASK_NAME = range(2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    kb   = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 নতুন Bot বানাও", callback_data="m:create")],
        [InlineKeyboardButton("📋 আমার Bots",       callback_data="m:mybots")],
        [InlineKeyboardButton("📊 Stats",           callback_data="m:stats")],
    ])
    await update.message.reply_text(
        f"👋 *স্বাগতম, {user.first_name}!*\n\n"
        f"🏭 *MegaBot — Bot Maker*\n\n"
        f"নিজের *Refer & Earn Bot* বানাও!\n\n"
        f"✅ Multi-level referral (L1+L2)\n"
        f"✅ Daily bonus + streak\n"
        f"✅ Leaderboard + Milestones\n"
        f"✅ Withdraw system (photo সহ)\n"
        f"✅ Force join channels\n"
        f"✅ Full inline /adminpanel\n\n"
        f"👇 শুরু করো:",
        parse_mode='Markdown', reply_markup=kb
    )


# ── Create Bot Conversation ───────────────────────────────────

async def create_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🤖 *Step 1/2 — Bot Token*\n\n"
        "@BotFather → /newbot → Token কপি করো\n\n"
        "Token: `1234567890:ABCdef...`\n\n"
        "❌ বাতিল: /cancel",
        parse_mode='Markdown'
    )
    return ASK_TOKEN


async def got_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    if ':' not in token or len(token) < 30 or ' ' in token:
        await update.message.reply_text("❌ Token ভুল! আবার দাও:")
        return ASK_TOKEN
    await update.message.reply_text("⏳ যাচাই করছি...")
    try:
        from telegram import Bot
        info = await Bot(token).get_me()
    except Exception as e:
        await update.message.reply_text(f"❌ Token কাজ করে না!\n`{str(e)[:60]}`\n\nআবার দাও:", parse_mode='Markdown')
        return ASK_TOKEN
    context.user_data['token'] = token
    context.user_data['uname'] = info.username
    await update.message.reply_text(
        f"✅ @{info.username}\n\n*Step 2/2:* Bot এর নাম দাও\n(যেমন: আমার রেফার বট)\n\n❌ /cancel",
        parse_mode='Markdown'
    )
    return ASK_NAME


async def got_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name     = update.message.text.strip()
    token    = context.user_data['token']
    uname    = context.user_data.get('uname')
    owner_id = update.effective_user.id

    if len(name) < 2:
        await update.message.reply_text("নাম কমপক্ষে ২ অক্ষর!")
        return ASK_NAME

    ok = save_child_bot(owner_id, token, name, uname)
    if not ok:
        await update.message.reply_text("❌ এই Token already registered!")
        return ConversationHandler.END

    # Default settings
    set_setting(token, 'admin_id',     str(owner_id))
    set_setting(token, 'ref_bonus',    '10')
    set_setting(token, 'ref_bonus_l2', '2')
    set_setting(token, 'min_withdraw', '50')
    set_setting(token, 'daily_bonus',  '2')
    set_setting(token, 'currency',     'টাকা')
    set_setting(token, 'bot_title',    name)

    started = await start_child_bot(token, name)
    context.user_data.clear()

    await update.message.reply_text(
        f"🎉 *Bot তৈরি সফল!*\n\n"
        f"📛 {name} | @{uname}\n"
        f"Status: {'🟢 চালু!' if started else '🔴 সেভ হয়েছে'}\n\n"
        f"তোমার bot এ যাও:\n"
        f"• /start → Bot দেখো\n"
        f"• /adminpanel → Admin Panel",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 আমার Bots", callback_data="m:mybots")]])
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("❌ বাতিল।")
    return ConversationHandler.END


# ── My Bots ───────────────────────────────────────────────────

async def my_bots_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid  = update.effective_user.id
    bots = get_user_bots(uid)

    if not bots:
        await q.edit_message_text(
            "📭 কোনো Bot নেই!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🤖 Bot বানাও", callback_data="m:create")]])
        )
        return

    kb   = []
    text = f"🤖 *তোমার Bots ({len(bots)}টি):*\n\n"
    for b in bots:
        icon  = "🟢" if is_bot_running(b['bot_token']) else "🔴"
        uname = b.get('bot_username') or '?'
        text += f"{icon} *{b['bot_name']}* @{uname}\n"
        kb.append([InlineKeyboardButton(f"{icon} {b['bot_name']}", callback_data=f"bm:{b['bot_token']}")])

    kb.append([InlineKeyboardButton("➕ নতুন Bot", callback_data="m:create"),
               InlineKeyboardButton("🏠 Home",     callback_data="m:home")])
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))


async def bot_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q        = update.callback_query
    await q.answer()
    token    = q.data.split(':')[1]
    bot_data = get_bot(token)
    if not bot_data or bot_data['owner_id'] != update.effective_user.id:
        await q.answer("⛔ এটা তোমার Bot না!", show_alert=True)
        return
    running = is_bot_running(token)
    kb = []
    if running:
        kb.append([InlineKeyboardButton("⛔ Stop",    callback_data=f"ba:stop:{token}"),
                   InlineKeyboardButton("🔄 Restart", callback_data=f"ba:restart:{token}")])
    else:
        kb.append([InlineKeyboardButton("▶️ Start",   callback_data=f"ba:start:{token}")])
    kb.append([InlineKeyboardButton("🗑️ Delete",     callback_data=f"ba:delete:{token}")])
    kb.append([InlineKeyboardButton("🔙 আমার Bots",  callback_data="m:mybots")])
    await q.edit_message_text(
        f"🤖 *{bot_data['bot_name']}* @{bot_data.get('bot_username','?')}\n"
        f"Status: {'🟢 Running' if running else '🔴 Stopped'}",
        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb)
    )


async def bot_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q        = update.callback_query
    await q.answer()
    parts    = q.data.split(':')
    action   = parts[1]
    token    = parts[2]
    bot_data = get_bot(token)
    if not bot_data or bot_data['owner_id'] != update.effective_user.id:
        await q.answer("⛔ Permission নেই!", show_alert=True)
        return

    if action == 'start':
        ok = await start_child_bot(token, bot_data['bot_name'])
        await q.answer("✅ Started!" if ok else "❌ Failed!", show_alert=True)
    elif action == 'stop':
        await stop_child_bot(token)
        await q.answer("⛔ Stopped!", show_alert=True)
    elif action == 'restart':
        ok = await restart_child_bot(token, bot_data['bot_name'])
        await q.answer("🔄 Restarted!" if ok else "❌ Failed!", show_alert=True)
    elif action == 'delete':
        await q.edit_message_text(
            f"⚠️ *{bot_data['bot_name']} delete করবে?*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ হ্যাঁ", callback_data=f"bx:{token}"),
                 InlineKeyboardButton("❌ না",    callback_data=f"bm:{token}")]
            ])
        )
        return

    q.data = f"bm:{token}"
    await bot_menu(update, context)


async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    token = q.data.split(':')[1]
    uid   = update.effective_user.id
    await stop_child_bot(token)
    delete_child_bot(uid, token)
    await q.edit_message_text(
        "✅ Bot মুছে ফেলা হয়েছে!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 My Bots", callback_data="m:mybots")]])
    )


async def main_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q      = update.callback_query
    await q.answer()
    action = q.data.split(':')[1]
    if action == 'mybots':
        await my_bots_show(update, context)
    elif action == 'stats':
        await q.edit_message_text(
            f"📊 *Stats*\n🟢 Running: {get_running_count()}\n💾 Total: {len(get_all_active_bots())}",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Home", callback_data="m:home")]])
        )
    elif action in ('home', 'create'):
        if action == 'home':
            await q.edit_message_text(
                "🏠 *Main Menu*", parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🤖 নতুন Bot", callback_data="m:create")],
                    [InlineKeyboardButton("📋 My Bots",   callback_data="m:mybots")],
                ])
            )
        else:
            await create_start(update, context)


# ── Startup ───────────────────────────────────────────────────

async def load_saved_bots(app):
    logger.info("Loading saved bots...")
    bots = get_all_active_bots()
    ok   = 0
    for b in bots:
        if await start_child_bot(b['bot_token'], b['bot_name']):
            ok += 1
        await asyncio.sleep(0.5)
    logger.info(f"✅ {ok}/{len(bots)} bots loaded!")


def main():
    init_db()
    app = Application.builder().token(MAIN_BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(create_start, pattern='^m:create$')],
        states={
            ASK_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_token)],
            ASK_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_name)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("mybots", lambda u,c: my_bots_show(u,c)))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(main_cb,        pattern=r'^m:'))
    app.add_handler(CallbackQueryHandler(bot_menu,       pattern=r'^bm:'))
    app.add_handler(CallbackQueryHandler(bot_action,     pattern=r'^ba:'))
    app.add_handler(CallbackQueryHandler(confirm_delete, pattern=r'^bx:'))

    async def post_init(application):
        await load_saved_bots(application)

    app.post_init = post_init
    logger.info("🚀 MegaBot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
