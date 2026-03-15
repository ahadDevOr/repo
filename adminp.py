import asyncio
import io
import csv
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database import (
    get_db_stats, get_all_users, get_user,
    get_pending_withdrawals, get_withdrawal, update_withdrawal,
    get_channels, add_channel, remove_channel,
    get_setting, set_setting,
    ban_user, unban_user, add_balance_db,
    get_milestones, add_milestone,
    get_open_tickets, reply_ticket,
    set_welcome_photo, set_log_chat, set_maintenance, get_bot
)


def _is_admin(token, uid):
    data = get_bot(token)
    if not data:
        return False
    if uid == data['owner_id']:
        return True
    aid = get_setting(token, 'admin_id')
    return aid and str(uid) == str(aid)


# ── /adminpanel ───────────────────────────────────────────────

async def cmd_adminpanel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = context.bot.token
    uid   = update.effective_user.id
    if not _is_admin(token, uid):
        await update.message.reply_text("⛔ তুমি এই bot এর admin না!")
        return
    await update.message.reply_text(**(await _main_msg(token)))


async def _main_msg(token):
    s        = get_db_stats(token)
    currency = get_setting(token, 'currency', 'টাকা')
    maint    = (get_bot(token) or {}).get('maintenance', 0)
    name     = get_setting(token, 'bot_title', 'My Bot')
    text = (
        f"⚙️ *Admin Panel — {name}*\n\n"
        f"👥 Users: *{s['total_users']}*\n"
        f"💰 Balance: *{s['total_balance']} {currency}*\n"
        f"⏳ Pending WD: *{s['pending_withdrawals']}*\n"
        f"✅ Total Paid: *{s['total_paid']} {currency}*\n"
        f"🚫 Banned: *{s['banned_users']}*\n"
        f"🎫 Tickets: *{s['open_tickets']}*\n"
        f"🔧 Maintenance: *{'🔴 ON' if maint else '🟢 OFF'}*"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard",    callback_data="ap:dash"),
         InlineKeyboardButton("💸 Withdrawals",  callback_data="ap:wdlist")],
        [InlineKeyboardButton("📡 Channels",     callback_data="ap:channels"),
         InlineKeyboardButton("⚙️ Settings",     callback_data="ap:settings")],
        [InlineKeyboardButton("📢 Broadcast",    callback_data="ap:broadcast"),
         InlineKeyboardButton("👥 Users",        callback_data="ap:users")],
        [InlineKeyboardButton("🎖️ Milestones",  callback_data="ap:milestones"),
         InlineKeyboardButton("🎫 Tickets",      callback_data="ap:tickets")],
        [InlineKeyboardButton("📥 Export CSV",   callback_data="ap:export"),
         InlineKeyboardButton(
             "🔴 Maint ON" if not maint else "🟢 Maint OFF",
             callback_data="ap:toggle_maint"
         )],
    ])
    return {'text': text, 'parse_mode': 'Markdown', 'reply_markup': kb}


# ── Callback Router ───────────────────────────────────────────

async def cb_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    token = context.bot.token
    uid   = q.from_user.id

    if not _is_admin(token, uid):
        await q.answer("⛔ Permission নেই!", show_alert=True)
        return

    data     = q.data
    parts    = data.split(':')
    action   = parts[1]
    currency = get_setting(token, 'currency', 'টাকা')

    # ── Home ─────────────────────────────────────────────────
    if action == 'home':
        msg = await _main_msg(token)
        await q.edit_message_text(msg['text'], parse_mode='Markdown', reply_markup=msg['reply_markup'])

    # ── Dashboard ────────────────────────────────────────────
    elif action == 'dash':
        s    = get_db_stats(token)
        now  = datetime.now().strftime('%d %b %Y, %H:%M')
        text = (
            f"📊 *Dashboard*\n🕐 `{now}`\n\n"
            f"👥 Total Users: *{s['total_users']}*\n"
            f"🚫 Banned: *{s['banned_users']}*\n"
            f"💰 Total Balance: *{s['total_balance']} {currency}*\n"
            f"✅ Total Paid: *{s['total_paid']} {currency}*\n"
            f"⏳ Pending WD: *{s['pending_withdrawals']}*\n"
            f"🔗 Total Refs: *{s['total_refs']}*\n"
            f"🎫 Open Tickets: *{s['open_tickets']}*"
        )
        await q.edit_message_text(text, parse_mode='Markdown',
                                   reply_markup=InlineKeyboardMarkup([
                                       [InlineKeyboardButton("🔄 Refresh", callback_data="ap:dash"),
                                        InlineKeyboardButton("🔙 Back",    callback_data="ap:home")]
                                   ]))

    # ── Withdrawals ──────────────────────────────────────────
    elif action == 'wdlist':
        rows = get_pending_withdrawals(token)
        if not rows:
            await q.edit_message_text("✅ কোনো pending withdrawal নেই!",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:home")]]))
            return
        text = f"💸 *Pending Withdrawals ({len(rows)})*\n\n"
        kb   = []
        for r in rows[:8]:
            un = r.get('username') or r.get('first_name') or str(r['user_id'])
            text += f"🆔 `#{r['id']}` @{un} | *{r['amount']} {currency}* | {r['method']}\n📋 `{r['address']}`\n─────\n"
            kb.append([
                InlineKeyboardButton(f"✅ #{r['id']}", callback_data=f"ap:wd_ok:{r['id']}"),
                InlineKeyboardButton(f"❌ #{r['id']}", callback_data=f"ap:wd_no:{r['id']}"),
            ])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="ap:home")])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif action == 'wd_ok':
        wid = int(parts[2])
        update_withdrawal(wid, 'approved')
        wd  = get_withdrawal(wid)
        if wd:
            try:
                await context.bot.send_message(
                    wd['user_id'],
                    f"🎉 *Withdrawal #{wid} Approved!*\n💰 {wd['amount']} {currency} → {wd['method']}\n📋 `{wd['address']}`",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        await q.answer(f"✅ #{wid} Approved!", show_alert=True)
        q.data = 'ap:wdlist'
        await cb_admin(update, context)

    elif action == 'wd_no':
        wid = int(parts[2])
        context.user_data['ap_step'] = f'wd_reject:{wid}'
        await q.edit_message_text(
            f"❌ *Withdrawal #{wid} Reject*\n\nReason লিখো:",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:wdlist")]])
        )

    # ── Channels ─────────────────────────────────────────────
    elif action == 'channels':
        chs  = get_channels(token)
        text = f"📡 *Force Join Channels ({len(chs)})*\n\n"
        kb   = []
        for ch in chs:
            name = ch.get('channel_name') or ch['channel_id']
            text += f"• {name} — `{ch['channel_id']}`\n"
            kb.append([InlineKeyboardButton(f"🗑️ Remove {name}", callback_data=f"ap:ch_del:{ch['channel_id']}")])
        if not chs:
            text += "_কোনো channel নেই।_"
        kb.append([InlineKeyboardButton("➕ Channel যোগ করো", callback_data="ap:ch_add")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="ap:home")])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif action == 'ch_add':
        context.user_data['ap_step'] = 'ch_id'
        await q.edit_message_text(
            "📡 *Channel যোগ — Step 1/3*\n\nChannel ID দাও:\n(যেমন: @mychannel বা -1001234567890)\n\n⚠️ Bot কে channel এ admin করো!",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:channels")]])
        )

    elif action == 'ch_del':
        remove_channel(token, parts[2])
        await q.answer("✅ Channel সরানো হয়েছে!", show_alert=True)
        q.data = 'ap:channels'
        await cb_admin(update, context)

    # ── Settings ─────────────────────────────────────────────
    elif action == 'settings':
        currency = get_setting(token, 'currency', 'টাকা')
        b1   = get_setting(token, 'ref_bonus', '10')
        b2   = get_setting(token, 'ref_bonus_l2', '2')
        minw = get_setting(token, 'min_withdraw', '50')
        daily = get_setting(token, 'daily_bonus', '2')
        wdch = get_setting(token, 'withdraw_channel', '—')
        name = get_setting(token, 'bot_title', 'My Bot')
        text = (
            f"⚙️ *Settings*\n\n"
            f"📛 Bot Name: *{name}*\n"
            f"💱 Currency: *{currency}*\n"
            f"💵 L1 Bonus: *{b1} {currency}*\n"
            f"💵 L2 Bonus: *{b2} {currency}*\n"
            f"📉 Min Withdraw: *{minw} {currency}*\n"
            f"🎯 Daily Bonus: *{daily} {currency}*\n"
            f"💸 WD Channel: `{wdch}`"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📛 Bot Name",     callback_data="ap:set:botname"),
             InlineKeyboardButton("💱 Currency",     callback_data="ap:set:currency")],
            [InlineKeyboardButton("💵 L1 Bonus",     callback_data="ap:set:bonus"),
             InlineKeyboardButton("💵 L2 Bonus",     callback_data="ap:set:bonus2")],
            [InlineKeyboardButton("📉 Min Withdraw", callback_data="ap:set:minw"),
             InlineKeyboardButton("🎯 Daily Bonus",  callback_data="ap:set:daily")],
            [InlineKeyboardButton("💸 WD Channel",   callback_data="ap:set:wdchannel"),
             InlineKeyboardButton("📋 Log Chat",     callback_data="ap:set:logchat")],
            [InlineKeyboardButton("🖼️ Welcome Photo/Video", callback_data="ap:set:welcome")],
            [InlineKeyboardButton("🔙 Back", callback_data="ap:home")],
        ])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=kb)

    elif action == 'set':
        field    = parts[2]
        labels   = {
            'botname': ('📛 Bot Name', 'bot_title'),
            'currency': ('💱 Currency (যেমন: টাকা, USDT)', 'currency'),
            'bonus': ('💵 L1 Referral Bonus', 'ref_bonus'),
            'bonus2': ('💵 L2 Referral Bonus', 'ref_bonus_l2'),
            'minw': ('📉 Min Withdraw', 'min_withdraw'),
            'daily': ('🎯 Daily Bonus', 'daily_bonus'),
            'wdchannel': ('💸 Withdraw Channel ID', 'withdraw_channel'),
            'logchat': ('📋 Log Chat ID', 'log_chat'),
        }
        if field == 'welcome':
            context.user_data['ap_step'] = 'set_welcome'
            await q.edit_message_text(
                "🖼️ Welcome Photo বা Video পাঠাও:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:settings")]])
            )
        elif field in labels:
            label, db_key = labels[field]
            cur = get_setting(token, db_key, '—')
            context.user_data['ap_step'] = f'setting:{db_key}'
            await q.edit_message_text(
                f"*{label}*\n\nআগের মান: `{cur}`\n\nনতুন মান লিখো:",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:settings")]])
            )

    # ── Broadcast ─────────────────────────────────────────────
    elif action == 'broadcast':
        users = get_all_users(token)
        await q.edit_message_text(
            f"📢 *Broadcast*\n\n👥 {len(users)} জন আছে\n\nকী পাঠাবে?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Text",  callback_data="ap:bc:text"),
                 InlineKeyboardButton("🖼️ Photo", callback_data="ap:bc:photo"),
                 InlineKeyboardButton("🎥 Video", callback_data="ap:bc:video")],
                [InlineKeyboardButton("🔙 Back", callback_data="ap:home")],
            ])
        )

    elif action == 'bc':
        btype = parts[2]
        msgs  = {'text': '📝 Message লিখো:', 'photo': '🖼️ Photo পাঠাও:', 'video': '🎥 Video পাঠাও:'}
        context.user_data['ap_step'] = f'bc:{btype}'
        await q.edit_message_text(
            msgs[btype],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:broadcast")]])
        )

    # ── Users ─────────────────────────────────────────────────
    elif action == 'users':
        users    = get_all_users(token)
        text     = f"👥 *Users ({len(users)})*\n\n"
        kb       = []
        for u in users[:10]:
            icon  = "🚫" if u.get('is_banned') else "✅"
            uname = u.get('username') or u.get('first_name') or str(u['user_id'])
            text += f"{icon} `{u['user_id']}` @{uname} | 💰{u['balance']}\n"
            kb.append([InlineKeyboardButton(f"{icon} {uname}", callback_data=f"ap:udetail:{u['user_id']}")])
        if len(users) > 10:
            text += f"\n...আরো {len(users)-10} জন। Export করো সব দেখতে।"
        kb.append([InlineKeyboardButton("🔍 Search", callback_data="ap:usearch"),
                   InlineKeyboardButton("📥 Export", callback_data="ap:export")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="ap:home")])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif action == 'usearch':
        context.user_data['ap_step'] = 'user_search'
        await q.edit_message_text(
            "🔍 User ID বা @username লিখো:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:users")]])
        )

    elif action == 'udetail':
        uid_t    = int(parts[2])
        u        = get_user(token, uid_t)
        currency = get_setting(token, 'currency', 'টাকা')
        if not u:
            await q.answer("❌ User নেই!", show_alert=True)
            return
        icon  = "🚫 Banned" if u.get('is_banned') else "✅ Active"
        uname = u.get('username') or '—'
        text  = (
            f"👤 *User Details*\n\n"
            f"🆔 ID: `{uid_t}`\n"
            f"📛 {u.get('first_name') or '—'} @{uname}\n"
            f"📊 {icon}\n"
            f"💰 Balance: *{u['balance']} {currency}*\n"
            f"👥 L1 Refs: *{u['total_refs']}*\n"
            f"👥 L2 Refs: *{u.get('level2_refs', 0)}*\n"
            f"🔥 Streak: *{u.get('daily_streak', 0)}*\n"
            f"👛 Wallet: `{u.get('wallet') or '—'}`\n"
            f"📅 Joined: {str(u.get('joined_at',''))[:10]}"
        )
        ban_btn = InlineKeyboardButton("✅ Unban", callback_data=f"ap:unban:{uid_t}") if u.get('is_banned') else \
                  InlineKeyboardButton("🚫 Ban",   callback_data=f"ap:ban:{uid_t}")
        kb = InlineKeyboardMarkup([
            [ban_btn, InlineKeyboardButton("💰 Add Balance", callback_data=f"ap:addbal:{uid_t}")],
            [InlineKeyboardButton("🔙 Users", callback_data="ap:users")],
        ])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=kb)

    elif action == 'ban':
        ban_user(token, int(parts[2]))
        await q.answer("🚫 Banned!", show_alert=True)
        q.data = f"ap:udetail:{parts[2]}"
        await cb_admin(update, context)

    elif action == 'unban':
        unban_user(token, int(parts[2]))
        await q.answer("✅ Unbanned!", show_alert=True)
        q.data = f"ap:udetail:{parts[2]}"
        await cb_admin(update, context)

    elif action == 'addbal':
        uid_t = int(parts[2])
        context.user_data['ap_step'] = f'addbal:{uid_t}'
        currency = get_setting(token, 'currency', 'টাকা')
        await q.edit_message_text(
            f"💰 `{uid_t}` কে কত {currency} দেবে?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"ap:udetail:{uid_t}")]])
        )

    # ── Milestones ────────────────────────────────────────────
    elif action == 'milestones':
        mss      = get_milestones(token)
        currency = get_setting(token, 'currency', 'টাকা')
        text     = f"🎖️ *Milestones ({len(mss)})*\n\n"
        for ms in mss:
            text += f"• *{ms['ref_count']} refs* → +{ms['bonus']} {currency}\n"
        if not mss:
            text += "_কোনো milestone নেই।_"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Milestone যোগ", callback_data="ap:ms_add")],
            [InlineKeyboardButton("🔙 Back", callback_data="ap:home")],
        ])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=kb)

    elif action == 'ms_add':
        context.user_data['ap_step'] = 'ms_refs'
        await q.edit_message_text(
            "🎖️ কত referral এ bonus? (সংখ্যা লিখো, যেমন: 10)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:milestones")]])
        )

    # ── Tickets ───────────────────────────────────────────────
    elif action == 'tickets':
        tickets = get_open_tickets(token)
        text    = f"🎫 *Tickets ({len(tickets)} open)*\n\n"
        kb      = []
        for t in tickets[:8]:
            un = f"@{t.get('username')}" if t.get('username') else str(t['user_id'])
            text += f"🎫 *#{t['id']}* {un}\n{t['message'][:50]}...\n─────\n"
            kb.append([InlineKeyboardButton(f"↩️ Reply #{t['id']}", callback_data=f"ap:treply:{t['id']}")])
        if not tickets:
            text += "✅ কোনো open ticket নেই!"
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="ap:home")])
        await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif action == 'treply':
        tid = int(parts[2])
        context.user_data['ap_step'] = f'treply:{tid}'
        await q.edit_message_text(
            f"↩️ Ticket #{tid} এ reply লিখো:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ap:tickets")]])
        )

    # ── Export ────────────────────────────────────────────────
    elif action == 'export':
        users    = get_all_users(token)
        currency = get_setting(token, 'currency', 'টাকা')
        output   = io.StringIO()
        writer   = csv.writer(output)
        writer.writerow(['User ID','Username','First Name','Balance','Total Refs','L2 Refs','Wallet','Banned','Streak','Joined'])
        for u in users:
            writer.writerow([u['user_id'], u.get('username',''), u.get('first_name',''),
                             u.get('balance',0), u.get('total_refs',0), u.get('level2_refs',0),
                             u.get('wallet',''), 'Yes' if u.get('is_banned') else 'No',
                             u.get('daily_streak',0), str(u.get('joined_at',''))[:10]])
        output.seek(0)
        buf = io.BytesIO(output.getvalue().encode('utf-8'))
        s   = get_db_stats(token)
        await context.bot.send_document(
            uid, document=buf,
            filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            caption=f"📥 *Export*\n👥 {s['total_users']} users | 💰 {s['total_balance']} {currency}",
            parse_mode='Markdown'
        )
        await q.answer("✅ CSV পাঠানো হয়েছে!", show_alert=True)

    # ── Maintenance toggle ────────────────────────────────────
    elif action == 'toggle_maint':
        current = (get_bot(token) or {}).get('maintenance', 0)
        set_maintenance(token, not current)
        await q.answer(f"Maintenance {'🔴 ON' if not current else '🟢 OFF'}!", show_alert=True)
        q.data = 'ap:home'
        await cb_admin(update, context)


# ── Text/Media Handler ────────────────────────────────────────

async def msg_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token    = context.bot.token
    uid      = update.effective_user.id
    step     = context.user_data.get('ap_step', '')
    msg      = update.message
    currency = get_setting(token, 'currency', 'টাকা')

    if not step:
        return

    def done(text):
        context.user_data.clear()
        return 
