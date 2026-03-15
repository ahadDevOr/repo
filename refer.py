import asyncio
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram import Update
from database import (
    register_user, get_user, get_leaderboard, get_milestones,
    claim_daily, create_withdrawal, get_setting, set_user_wallet,
    get_channels, get_withdrawal, update_withdrawal, get_bot
)


# ── Force Join ────────────────────────────────────────────────

async def _check_fj(bot, token, user_id):
    channels = get_channels(token)
    if not channels:
        return True, []
    missing = []
    for ch in channels:
        try:
            m = await bot.get_chat_member(ch['channel_id'], user_id)
            if m.status in ('left', 'kicked', 'banned'):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return len(missing) == 0, missing


def _fj_kb(missing):
    kb = []
    for ch in missing:
        name = ch.get('channel_name') or ch['channel_id']
        link = ch.get('invite_link') or f"https://t.me/{ch['channel_id'].lstrip('@')}"
        kb.append([InlineKeyboardButton(f"📢 {name}", url=link)])
    kb.append([InlineKeyboardButton("✅ Join করেছি!", callback_data="fj:check")])
    return InlineKeyboardMarkup(kb)


async def cb_fj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    token = context.bot.token
    uid   = q.from_user.id
    ok, missing = await _check_fj(context.bot, token, uid)
    if not ok:
        await q.edit_message_text("❌ এখনো সব channel join করোনি!", reply_markup=_fj_kb(missing))
    else:
        await q.delete_message()
        await _show_home(q.message.reply_text if hasattr(q.message, 'reply_text') else None,
                         context, token, uid, q.from_user.first_name)


# ── /start ────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = context.bot.token
    user  = update.effective_user
    uid   = user.id

    # Maintenance check
    bot_data = get_bot(token)
    if bot_data and bot_data.get('maintenance'):
        await update.message.reply_text("🔧 *Bot maintenance এ আছে। একটু পরে আসো!*", parse_mode='Markdown')
        return

    # Parse referral — /start ref_USERID
    referred_by = None
    level1_ref  = None
    if context.args:
        parts = context.args[0].split('_')
        try:
            if len(parts) >= 2:
                referred_by = int(parts[1])
                if referred_by == uid:
                    referred_by = None
            if len(parts) >= 3:
                level1_ref = int(parts[2])
        except Exception:
            pass

    register_user(token, uid, user.username, user.first_name, referred_by, level1_ref)

    # Force join check
    ok, missing = await _check_fj(context.bot, token, uid)
    if not ok:
        await update.message.reply_text(
            "📢 *নিচের channel গুলোতে join করো তারপর আসো:*",
            parse_mode='Markdown',
            reply_markup=_fj_kb(missing)
        )
        return

    await _show_home(update.message.reply_text, context, token, uid, user.first_name)


async def _show_home(send_fn, context, token, uid, fname):
    bot_data = get_bot(token)
    title    = get_setting(token, 'bot_title', '🎁 Refer & Earn Bot')
    bonus    = get_setting(token, 'ref_bonus', '10')
    currency = get_setting(token, 'currency', 'টাকা')

    text = (
        f"👋 *স্বাগতম, {fname}!*\n\n"
        f"🏷️ *{title}*\n\n"
        f"বন্ধু refer করো → প্রতিজনে *{bonus} {currency}* পাও! 💸\n\n"
        f"👇 মেনু:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Balance",      callback_data="r:balance"),
         InlineKeyboardButton("🔗 Refer Link",   callback_data="r:link")],
        [InlineKeyboardButton("🎯 Daily Bonus",  callback_data="r:daily"),
         InlineKeyboardButton("🏆 Leaderboard",  callback_data="r:leader")],
        [InlineKeyboardButton("💸 Withdraw",     callback_data="r:withdraw"),
         InlineKeyboardButton("👛 Wallet",       callback_data="r:wallet")],
        [InlineKeyboardButton("🎖️ Milestones",  callback_data="r:milestones"),
         InlineKeyboardButton("🎫 Support",      callback_data="r:ticket")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="r:howto")],
    ])

    photo = bot_data.get('welcome_photo') if bot_data else None
    if photo and send_fn:
        try:
            await context.bot.send_photo(
                chat_id=context._chat_id if hasattr(context, '_chat_id') else None,
                photo=photo, caption=text, parse_mode='Markdown', reply_markup=kb
            )
            return
        except Exception:
            pass
    if send_fn:
        await send_fn(text, parse_mode='Markdown', reply_markup=kb)


# ── Callbacks ─────────────────────────────────────────────────

async def cb_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q      = update.callback_query
    await q.answer()
    token  = context.bot.token
    uid    = q.from_user.id
    data   = q.data

    # Withdraw channel approve/reject
    if data.startswith('wa:'):
        parts  = data.split(':')
        action = parts[1]
        wid    = int(parts[2])
        currency = get_setting(token, 'currency', 'টাকা')
        if action == 'approve':
            update_withdrawal(wid, 'approved')
            wd = get_withdrawal(wid)
            if wd:
                try:
                    await context.bot.send_message(
                        wd['user_id'],
                        f"🎉 *Withdrawal #{wid} Approved!*\n💰 {wd['amount']} {currency} → {wd['method']}\n📋 `{wd['address']}`",
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass
            await q.answer("✅ Approved!", show_alert=True)
        elif action == 'reject':
            context.user_data['ap_step'] = f'wd_reject:{wid}'
            await q.edit_message_text(f"❌ Reject reason লিখো for #{wid}:")
        return

    user     = get_user(token, uid)
    currency = get_setting(token, 'currency', 'টাকা')
    bonus    = get_setting(token, 'ref_bonus', '10')
    bonus2   = get_setting(token, 'ref_bonus_l2', '2')

    if not user:
        await q.edit_message_text("❌ /start দাও আগে!")
        return
    if user.get('is_banned'):
        await q.edit_message_text("🚫 তুমি এই bot থেকে banned!")
        return

    action = data.split(':')[1]

    if action == 'balance':
        minw = get_setting(token, 'min_withdraw', '50')
        await q.edit_message_text(
            f"💰 *তোমার Dashboard*\n\n"
            f"💵 Balance: *{user['balance']} {currency}*\n"
            f"👥 L1 Refs: *{user['total_refs']}* (+{bonus} each)\n"
            f"👥 L2 Refs: *{user['level2_refs']}* (+{bonus2} each)\n"
            f"🔥 Streak: *{user['daily_streak']} দিন*\n"
            f"📉 Min Withdraw: *{minw} {currency}*",
            parse_mode='Markdown', reply_markup=_back_kb()
        )

    elif action == 'link':
        me  = await context.bot.get_me()
        url = f"https://t.me/{me.username}?start=ref_{uid}"
        await q.edit_message_text(
            f"🔗 *তোমার Referral Link:*\n\n`{url}`\n\n"
            f"প্রতিজন join করলে *{bonus} {currency}* পাবে!\n"
            f"তারা refer করলে তুমি পাবে *{bonus2} {currency}* (L2)",
            parse_mode='Markdown', reply_markup=_back_kb()
        )

    elif action == 'daily':
        ok, earned, streak = claim_daily(token, uid)
        if ok:
            await q.edit_message_text(
                f"🎯 *Daily Bonus!*\n\n✅ আজ: *+{earned} {currency}*\n🔥 Streak: *{streak} দিন*\n\nকাল আবার এসো! 🙏",
                parse_mode='Markdown', reply_markup=_back_kb()
            )
        else:
            await q.edit_message_text(
                "⏰ আজকের daily bonus নেওয়া হয়ে গেছে!\n\nকাল আবার এসো 🔥",
                reply_markup=_back_kb()
            )

    elif action == 'leader':
        top  = get_leaderboard(token, 10)
        text = "🏆 *Top 10 Referrers*\n\n"
        medals = ['🥇','🥈','🥉','4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
        for i, u in enumerate(top):
            name = f"@{u['username']}" if u.get('username') else (u.get('first_name') or '?')
            text += f"{medals[i]} {name} — *{u['total_refs']} refs*\n"
        if not top:
            text += "_এখনো কেউ নেই!_"
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=_back_kb())

    elif action == 'milestones':
        mss  = get_milestones(token)
        refs = user['total_refs']
        text = f"🎖️ *Milestones*\n\n📊 তোমার refs: *{refs}*\n\n"
        if mss:
            for ms in mss:
                done = "✅" if refs >= ms['ref_count'] else "⭕"
                text += f"{done} *{ms['ref_count']} refs* → +{ms['bonus']} {currency}\n"
        else:
            text += "_Admin এখনো milestone সেট করেননি।_"
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=_back_kb())

    elif action == 'wallet':
        wallet = user.get('wallet') or 'সেট করা হয়নি'
        locked = user.get('wallet_locked')
        kb     = []
        if not locked:
            kb.append([InlineKeyboardButton("✏️ Wallet সেট করো", callback_data="r:setwallet")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="r:home")])
        await q.edit_message_text(
            f"👛 *Wallet*\n\n📋 Address: `{wallet}`\n🔐 {'🔒 Locked' if locked else '🔓 Unlocked'}",
            parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb)
        )

    elif action == 'setwallet':
        context.user_data['refer_step'] = 'wallet'
        await q.edit_message_text("✏️ Wallet address / bKash/Nagad number দাও:\n\n❌ বাতিল: /start")

    elif action == 'withdraw':
        bal  = user['balance']
        minw = float(get_setting(token, 'min_withdraw', 50))
        if bal < minw:
            await q.edit_message_text(
                f"❌ Balance কম!\n\nতোমার: *{bal} {currency}*\nদরকার: *{minw} {currency}*",
                parse_mode='Markdown', reply_markup=_back_kb()
            )
        else:
            context.user_data['refer_step'] = 'wd_method'
            context.user_data['wd_bal']     = bal
            await q.edit_message_text(
                f"💸 *Withdraw*\n\nAvailable: *{bal} {currency}*\n\nPayment method লিখো:\n(যেমন: bKash, Nagad, USDT)\n\n❌ বাতিল: /start"
            )

    elif action == 'ticket':
        context.user_data['refer_step'] = 'ticket'
        await q.edit_message_text("🎫 সমস্যা বা প্রশ্ন লিখো:\n\n❌ বাতিল: /start")

    elif action == 'howto':
        await q.edit_message_text(
            f"ℹ️ *কীভাবে আয় করবে?*\n\n"
            f"1️⃣ Referral Link নাও\n"
            f"2️⃣ বন্ধুদের পাঠাও\n"
            f"3️⃣ তারা join করলে *{bonus} {currency}*\n"
            f"4️⃣ তারা refer করলে *{bonus2} {currency}* (L2)\n"
            f"5️⃣ Daily bonus নাও প্রতিদিন\n"
            f"6️⃣ Milestone পূরণ করে extra bonus\n"
            f"7️⃣ Balance জমলে Withdraw করো!",
            parse_mode='Markdown', reply_markup=_back_kb()
        )

    elif action == 'home':
        title    = get_setting(token, 'bot_title', '🎁 Refer & Earn Bot')
        await q.edit_message_text(
            f"🏠 *{title}*\n\nকী করতে চাও?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Balance",     callback_data="r:balance"),
                 InlineKeyboardButton("🔗 Refer Link",  callback_data="r:link")],
                [InlineKeyboardButton("🎯 Daily Bonus", callback_data="r:daily"),
                 InlineKeyboardButton("🏆 Leaderboard", callback_data="r:leader")],
                [InlineKeyboardButton("💸 Withdraw",    callback_data="r:withdraw"),
                 InlineKeyboardButton("👛 Wallet",      callback_data="r:wallet")],
                [InlineKeyboardButton("🎖️ Milestones", callback_data="r:milestones"),
                 InlineKeyboardButton("🎫 Support",     callback_data="r:ticket")],
            ])
        )


# ── Text messages ─────────────────────────────────────────────

async def msg_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token    = context.bot.token
    uid      = update.effective_user.id
    step     = context.user_data.get('refer_step', '')
    text     = update.message.text.strip()
    currency = get_setting(token, 'currency', 'টাকা')

    if step == 'wallet':
        set_user_wallet(token, uid, text)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Wallet সেট: `{text}`\n🔒 Locked", parse_mode='Markdown')

    elif step == 'wd_method':
        context.user_data['wd_method'] = text
        context.user_data['refer_step'] = 'wd_address'
        await update.message.reply_text(f"✅ Method: *{text}*\n\nAccount number / address দাও:", parse_mode='Markdown')

    elif step == 'wd_address':
        method  = context.user_data.get('wd_method')
        amount  = context.user_data.get('wd_bal', 0)
        address = text
        wid     = create_withdrawal(token, uid, amount, method, address)
        uname   = update.effective_user.username or str(uid)

        # Withdraw channel এ পাঠাও
        await _notify_wd_channel(context.bot, token, wid, uid, uname, amount, method, address, currency)

        context.user_data.clear()
        await update.message.reply_text(
            f"✅ *Withdraw Request #{wid} পাঠানো হয়েছে!*\n\n"
            f"💰 {amount} {currency} | 📱 {method}\n📋 `{address}`\n\nAdmin approve করলে পাবে! 🙏",
            parse_mode='Markdown'
        )

    elif step == 'ticket':
        from database import create_ticket
        tid      = create_ticket(token, uid, text)
        admin_id = get_setting(token, 'admin_id')
        uname    = update.effective_user.username or str(uid)
        if admin_id:
            try:
                await context.bot.send_message(
                    int(admin_id),
                    f"🎫 *Ticket #{tid}*\n👤 @{uname} (`{uid}`)\n\n{text}\n\nReply: /adminpanel → Tickets",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        context.user_data.clear()
        await update.message.reply_text(f"✅ Ticket #{tid} পাঠানো হয়েছে!")

    else:
        await update.message.reply_text("👋 /start দাও!")


async def _notify_wd_channel(bot, token, wid, uid, uname, amount, method, address, currency):
    wd_ch    = get_setting(token, 'withdraw_channel')
    admin_id = get_setting(token, 'admin_id')
    target   = int(wd_ch) if wd_ch else (int(admin_id) if admin_id else None)
    if not target:
        return

    now  = datetime.now().strftime('%Y-%m-%d %H:%M')
    text = (
        f"💸 *New Withdraw Request*\n\n"
        f"🆔 Request: `#{wid}`\n"
        f"👤 @{uname} (`{uid}`)\n"
        f"💰 Amount: *{amount} {currency}*\n"
        f"📱 Method: *{method}*\n"
        f"📋 Address: `{address}`\n"
        f"🕐 Time: {now}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"wa:approve:{wid}"),
         InlineKeyboardButton("❌ Reject",  callback_data=f"wa:reject:{wid}")]
    ])

    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count > 0:
            await bot.send_photo(target, photo=photos.photos[0][0].file_id,
                                 caption=text, parse_mode='Markdown', reply_markup=kb)
        else:
            await bot.send_message(target, text, parse_mode='Markdown', reply_markup=kb)
    except Exception:
        try:
            await bot.send_message(target, text, parse_mode='Markdown', reply_markup=kb)
        except Exception:
            pass


def _back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="r:home")]])
