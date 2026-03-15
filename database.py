import sqlite3
import os
from datetime import datetime, date, timedelta

# DB path — /tmp সবসময় available Render এ
_db_path = os.environ.get("DB_PATH", "megabot.db")
os.makedirs(os.path.dirname(_db_path), exist_ok=True) if os.path.dirname(_db_path) else None
DB = _db_path


def get_conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    db = get_conn()
    c  = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS child_bots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_id      INTEGER NOT NULL,
        bot_token     TEXT    UNIQUE NOT NULL,
        bot_name      TEXT    NOT NULL,
        bot_username  TEXT,
        is_active     INTEGER DEFAULT 1,
        maintenance   INTEGER DEFAULT 0,
        welcome_photo TEXT,
        log_chat_id   TEXT,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token     TEXT    NOT NULL,
        user_id       INTEGER NOT NULL,
        username      TEXT,
        first_name    TEXT,
        referred_by   INTEGER DEFAULT NULL,
        level1_ref    INTEGER DEFAULT NULL,
        balance       REAL    DEFAULT 0,
        total_refs    INTEGER DEFAULT 0,
        level2_refs   INTEGER DEFAULT 0,
        is_banned     INTEGER DEFAULT 0,
        wallet        TEXT,
        wallet_locked INTEGER DEFAULT 0,
        last_daily    TEXT    DEFAULT NULL,
        daily_streak  INTEGER DEFAULT 0,
        joined_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(bot_token, user_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
        bot_token TEXT NOT NULL,
        key       TEXT NOT NULL,
        value     TEXT,
        PRIMARY KEY(bot_token, key)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS force_channels (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token    TEXT NOT NULL,
        channel_id   TEXT NOT NULL,
        channel_name TEXT,
        invite_link  TEXT,
        UNIQUE(bot_token, channel_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS withdrawals (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token     TEXT NOT NULL,
        user_id       INTEGER NOT NULL,
        amount        REAL NOT NULL,
        method        TEXT,
        address       TEXT,
        status        TEXT DEFAULT 'pending',
        reject_reason TEXT,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS milestones (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token TEXT NOT NULL,
        ref_count INTEGER NOT NULL,
        bonus     REAL NOT NULL,
        UNIQUE(bot_token, ref_count)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS tickets (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token  TEXT NOT NULL,
        user_id    INTEGER NOT NULL,
        message    TEXT NOT NULL,
        status     TEXT DEFAULT 'open',
        reply      TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    db.commit()
    db.close()
    print("✅ Database ready!")


# ── Settings ──────────────────────────────────────────────────

def set_setting(token, key, value):
    db = get_conn()
    db.execute('INSERT OR REPLACE INTO bot_settings VALUES (?,?,?)', (token, key, str(value)))
    db.commit()
    db.close()


def get_setting(token, key, default=None):
    db  = get_conn()
    row = db.execute('SELECT value FROM bot_settings WHERE bot_token=? AND key=?', (token, key)).fetchone()
    db.close()
    return row['value'] if row else default


# ── Child Bots ────────────────────────────────────────────────

def save_child_bot(owner_id, token, name, username):
    db = get_conn()
    try:
        db.execute('INSERT INTO child_bots (owner_id,bot_token,bot_name,bot_username) VALUES (?,?,?,?)',
                   (owner_id, token, name, username))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        db.close()


def get_bot(token):
    db  = get_conn()
    row = db.execute('SELECT * FROM child_bots WHERE bot_token=?', (token,)).fetchone()
    db.close()
    return dict(row) if row else None


def get_all_active_bots():
    db   = get_conn()
    rows = db.execute('SELECT bot_token, bot_name, owner_id FROM child_bots WHERE is_active=1').fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_user_bots(owner_id):
    db   = get_conn()
    rows = db.execute('SELECT * FROM child_bots WHERE owner_id=?', (owner_id,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def delete_child_bot(owner_id, token):
    db = get_conn()
    db.execute('DELETE FROM child_bots WHERE owner_id=? AND bot_token=?', (owner_id, token))
    aff = db.total_changes
    db.commit()
    db.close()
    return aff > 0


def set_maintenance(token, on):
    db = get_conn()
    db.execute('UPDATE child_bots SET maintenance=? WHERE bot_token=?', (1 if on else 0, token))
    db.commit()
    db.close()


def set_welcome_photo(token, file_id):
    db = get_conn()
    db.execute('UPDATE child_bots SET welcome_photo=? WHERE bot_token=?', (file_id, token))
    db.commit()
    db.close()


def set_log_chat(token, chat_id):
    db = get_conn()
    db.execute('UPDATE child_bots SET log_chat_id=? WHERE bot_token=?', (str(chat_id), token))
    db.commit()
    db.close()


# ── Users ─────────────────────────────────────────────────────

def register_user(token, user_id, username, first_name, referred_by=None, level1_ref=None):
    db = get_conn()
    try:
        db.execute('INSERT INTO users (bot_token,user_id,username,first_name,referred_by,level1_ref) VALUES (?,?,?,?,?,?)',
                   (token, user_id, username, first_name, referred_by, level1_ref))
        db.commit()
        is_new = True
    except sqlite3.IntegrityError:
        is_new = False

    if is_new:
        bonus  = float(get_setting(token, 'ref_bonus', 10))
        bonus2 = float(get_setting(token, 'ref_bonus_l2', 2))
        if referred_by:
            db.execute('UPDATE users SET balance=balance+?, total_refs=total_refs+1 WHERE bot_token=? AND user_id=?',
                       (bonus, token, referred_by))
            _check_milestone(db, token, referred_by)
        if level1_ref:
            db.execute('UPDATE users SET balance=balance+?, level2_refs=level2_refs+1 WHERE bot_token=? AND user_id=?',
                       (bonus2, token, level1_ref))
        db.commit()

    db.close()
    return is_new


def _check_milestone(db, token, user_id):
    row = db.execute('SELECT total_refs FROM users WHERE bot_token=? AND user_id=?', (token, user_id)).fetchone()
    if not row:
        return
    ms = db.execute('SELECT bonus FROM milestones WHERE bot_token=? AND ref_count=?',
                    (token, row['total_refs'])).fetchone()
    if ms:
        db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?',
                   (ms['bonus'], token, user_id))


def get_user(token, user_id):
    db  = get_conn()
    row = db.execute('SELECT * FROM users WHERE bot_token=? AND user_id=?', (token, user_id)).fetchone()
    db.close()
    return dict(row) if row else None


def get_all_users(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM users WHERE bot_token=?', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def ban_user(token, user_id):
    db = get_conn()
    db.execute('UPDATE users SET is_banned=1 WHERE bot_token=? AND user_id=?', (token, user_id))
    db.commit()
    db.close()


def unban_user(token, user_id):
    db = get_conn()
    db.execute('UPDATE users SET is_banned=0 WHERE bot_token=? AND user_id=?', (token, user_id))
    db.commit()
    db.close()


def add_balance_db(token, user_id, amount):
    db = get_conn()
    db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?', (amount, token, user_id))
    db.commit()
    db.close()


def set_user_wallet(token, user_id, wallet):
    db = get_conn()
    db.execute('UPDATE users SET wallet=?, wallet_locked=1 WHERE bot_token=? AND user_id=?', (wallet, token, user_id))
    db.commit()
    db.close()


def get_leaderboard(token, limit=10):
    db   = get_conn()
    rows = db.execute('SELECT user_id,username,first_name,total_refs,balance FROM users WHERE bot_token=? AND is_banned=0 ORDER BY total_refs DESC LIMIT ?',
                      (token, limit)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_db_stats(token):
    db = get_conn()
    tu = db.execute('SELECT COUNT(*) as n FROM users WHERE bot_token=?', (token,)).fetchone()['n']
    tb = db.execute('SELECT COALESCE(SUM(balance),0) as s FROM users WHERE bot_token=?', (token,)).fetchone()['s']
    tr = db.execute('SELECT COALESCE(SUM(total_refs),0) as s FROM users WHERE bot_token=?', (token,)).fetchone()['s']
    pw = db.execute("SELECT COUNT(*) as n FROM withdrawals WHERE bot_token=? AND status='pending'", (token,)).fetchone()['n']
    pa = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE bot_token=? AND status='approved'", (token,)).fetchone()['s']
    bn = db.execute('SELECT COUNT(*) as n FROM users WHERE bot_token=? AND is_banned=1', (token,)).fetchone()['n']
    tk = db.execute("SELECT COUNT(*) as n FROM tickets WHERE bot_token=? AND status='open'", (token,)).fetchone()['n']
    db.close()
    return {'total_users': tu, 'total_balance': tb, 'total_refs': tr,
            'pending_withdrawals': pw, 'total_paid': pa, 'banned_users': bn, 'open_tickets': tk}


# ── Daily Bonus ───────────────────────────────────────────────

def claim_daily(token, user_id):
    today = datetime.now().strftime('%Y-%m-%d')
    db    = get_conn()
    row   = db.execute('SELECT last_daily, daily_streak FROM users WHERE bot_token=? AND user_id=?',
                       (token, user_id)).fetchone()
    if not row:
        db.close()
        return False, 0, 0

    last   = row['last_daily']
    streak = row['daily_streak'] or 0

    if last == today:
        db.close()
        return False, 0, streak

    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    streak    = (streak + 1) if last == yesterday else 1
    base      = float(get_setting(token, 'daily_bonus', 2))
    bonus     = round(base + (streak - 1) * 0.5, 2)

    db.execute('UPDATE users SET balance=balance+?, last_daily=?, daily_streak=? WHERE bot_token=? AND user_id=?',
               (bonus, today, streak, token, user_id))
    db.commit()
    db.close()
    return True, bonus, streak


# ── Force Channels ────────────────────────────────────────────

def add_channel(token, channel_id, name, invite):
    db = get_conn()
    try:
        db.execute('INSERT INTO force_channels (bot_token,channel_id,channel_name,invite_link) VALUES (?,?,?,?)',
                   (token, str(channel_id), name, invite))
        db.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    db.close()
    return result


def remove_channel(token, channel_id):
    db = get_conn()
    db.execute('DELETE FROM force_channels WHERE bot_token=? AND channel_id=?', (token, str(channel_id)))
    db.commit()
    db.close()


def get_channels(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM force_channels WHERE bot_token=?', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Withdrawals ───────────────────────────────────────────────

def create_withdrawal(token, user_id, amount, method, address):
    db = get_conn()
    db.execute('UPDATE users SET balance=balance-? WHERE bot_token=? AND user_id=?', (amount, token, user_id))
    db.execute('INSERT INTO withdrawals (bot_token,user_id,amount,method,address) VALUES (?,?,?,?,?)',
               (token, user_id, amount, method, address))
    db.commit()
    wid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return wid


def get_pending_withdrawals(token):
    db   = get_conn()
    rows = db.execute('''SELECT w.*, u.username, u.first_name
                         FROM withdrawals w
                         LEFT JOIN users u ON w.bot_token=u.bot_token AND w.user_id=u.user_id
                         WHERE w.bot_token=? AND w.status='pending'
                         ORDER BY w.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def update_withdrawal(wid, status, reason=''):
    db = get_conn()
    db.execute('UPDATE withdrawals SET status=?, reject_reason=? WHERE id=?', (status, reason, wid))
    if status == 'rejected':
        row = db.execute('SELECT bot_token, user_id, amount FROM withdrawals WHERE id=?', (wid,)).fetchone()
        if row:
            db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?',
                       (row['amount'], row['bot_token'], row['user_id']))
    db.commit()
    db.close()


def get_withdrawal(wid):
    db  = get_conn()
    row = db.execute('SELECT * FROM withdrawals WHERE id=?', (wid,)).fetchone()
    db.close()
    return dict(row) if row else None


# ── Milestones ────────────────────────────────────────────────

def add_milestone(token, ref_count, bonus):
    db = get_conn()
    try:
        db.execute('INSERT INTO milestones (bot_token,ref_count,bonus) VALUES (?,?,?)', (token, ref_count, bonus))
        db.commit()
        result = True
    except Exception:
        result = False
    db.close()
    return result


def get_milestones(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM milestones WHERE bot_token=? ORDER BY ref_count', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Tickets ───────────────────────────────────────────────────

def create_ticket(token, user_id, message):
    db = get_conn()
    db.execute('INSERT INTO tickets (bot_token,user_id,message) VALUES (?,?,?)', (token, user_id, message))
    db.commit()
    tid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return tid


def get_open_tickets(token):
    db   = get_conn()
    rows = db.execute('''SELECT t.*, u.username, u.first_name
                         FROM tickets t
                         LEFT JOIN users u ON t.bot_token=u.bot_token AND t.user_id=u.user_id
                         WHERE t.bot_token=? AND t.status='open'
                         ORDER BY t.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def reply_ticket(tid, reply_text):
    db = get_conn()
    db.execute("UPDATE tickets SET reply=?, status='closed' WHERE id=?", (reply_text, tid))
    db.commit()
    db.close()
r_id)).fetchone()
    if not row:
        db.close()
        return False, 0, 0

    last   = row['last_daily']
    streak = row['daily_streak'] or 0

    if last == today:
        db.close()
        return False, 0, streak

    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    streak    = (streak + 1) if last == yesterday else 1
    base      = float(get_setting(token, 'daily_bonus', 2))
    bonus     = round(base + (streak - 1) * 0.5, 2)

    db.execute('UPDATE users SET balance=balance+?, last_daily=?, daily_streak=? WHERE bot_token=? AND user_id=?',
               (bonus, today, streak, token, user_id))
    db.commit()
    db.close()
    return True, bonus, streak


# ── Force Channels ────────────────────────────────────────────

def add_channel(token, channel_id, name, invite):
    db = get_conn()
    try:
        db.execute('INSERT INTO force_channels (bot_token,channel_id,channel_name,invite_link) VALUES (?,?,?,?)',
                   (token, str(channel_id), name, invite))
        db.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    db.close()
    return result


def remove_channel(token, channel_id):
    db = get_conn()
    db.execute('DELETE FROM force_channels WHERE bot_token=? AND channel_id=?', (token, str(channel_id)))
    db.commit()
    db.close()


def get_channels(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM force_channels WHERE bot_token=?', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Withdrawals ───────────────────────────────────────────────

def create_withdrawal(token, user_id, amount, method, address):
    db = get_conn()
    db.execute('UPDATE users SET balance=balance-? WHERE bot_token=? AND user_id=?', (amount, token, user_id))
    db.execute('INSERT INTO withdrawals (bot_token,user_id,amount,method,address) VALUES (?,?,?,?,?)',
               (token, user_id, amount, method, address))
    db.commit()
    wid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return wid


def get_pending_withdrawals(token):
    db   = get_conn()
    rows = db.execute('''SELECT w.*, u.username, u.first_name
                         FROM withdrawals w
                         LEFT JOIN users u ON w.bot_token=u.bot_token AND w.user_id=u.user_id
                         WHERE w.bot_token=? AND w.status='pending'
                         ORDER BY w.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def update_withdrawal(wid, status, reason=''):
    db = get_conn()
    db.execute('UPDATE withdrawals SET status=?, reject_reason=? WHERE id=?', (status, reason, wid))
    if status == 'rejected':
        row = db.execute('SELECT bot_token, user_id, amount FROM withdrawals WHERE id=?', (wid,)).fetchone()
        if row:
            db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?',
                       (row['amount'], row['bot_token'], row['user_id']))
    db.commit()
    db.close()


def get_withdrawal(wid):
    db  = get_conn()
    row = db.execute('SELECT * FROM withdrawals WHERE id=?', (wid,)).fetchone()
    db.close()
    return dict(row) if row else None


# ── Milestones ────────────────────────────────────────────────

def add_milestone(token, ref_count, bonus):
    db = get_conn()
    try:
        db.execute('INSERT INTO milestones (bot_token,ref_count,bonus) VALUES (?,?,?)', (token, ref_count, bonus))
        db.commit()
        result = True
    except Exception:
        result = False
    db.close()
    return result


def get_milestones(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM milestones WHERE bot_token=? ORDER BY ref_count', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Tickets ───────────────────────────────────────────────────

def create_ticket(token, user_id, message):
    db = get_conn()
    db.execute('INSERT INTO tickets (bot_token,user_id,message) VALUES (?,?,?)', (token, user_id, message))
    db.commit()
    tid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return tid


def get_open_tickets(token):
    db   = get_conn()
    rows = db.execute('''SELECT t.*, u.username, u.first_name
                         FROM tickets t
                         LEFT JOIN users u ON t.bot_token=u.bot_token AND t.user_id=u.user_id
                         WHERE t.bot_token=? AND t.status='open'
                         ORDER BY t.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def reply_ticket(tid, reply_text):
    db = get_conn()
    db.execute("UPDATE tickets SET reply=?, status='closed' WHERE id=?", (reply_text, tid))
    db.commit()
    db.close()
        balance       REAL    DEFAULT 0,
        total_refs    INTEGER DEFAULT 0,
        level2_refs   INTEGER DEFAULT 0,
        is_banned     INTEGER DEFAULT 0,
        wallet        TEXT,
        wallet_locked INTEGER DEFAULT 0,
        last_daily    TEXT    DEFAULT NULL,
        daily_streak  INTEGER DEFAULT 0,
        joined_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(bot_token, user_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
        bot_token TEXT NOT NULL,
        key       TEXT NOT NULL,
        value     TEXT,
        PRIMARY KEY(bot_token, key)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS force_channels (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token    TEXT NOT NULL,
        channel_id   TEXT NOT NULL,
        channel_name TEXT,
        invite_link  TEXT,
        UNIQUE(bot_token, channel_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS withdrawals (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token     TEXT NOT NULL,
        user_id       INTEGER NOT NULL,
        amount        REAL NOT NULL,
        method        TEXT,
        address       TEXT,
        status        TEXT DEFAULT 'pending',
        reject_reason TEXT,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS milestones (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token TEXT NOT NULL,
        ref_count INTEGER NOT NULL,
        bonus     REAL NOT NULL,
        UNIQUE(bot_token, ref_count)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS tickets (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token  TEXT NOT NULL,
        user_id    INTEGER NOT NULL,
        message    TEXT NOT NULL,
        status     TEXT DEFAULT 'open',
        reply      TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    db.commit()
    db.close()
    print("✅ Database ready!")


# ── Settings ──────────────────────────────────────────────────

def set_setting(token, key, value):
    db = get_conn()
    db.execute('INSERT OR REPLACE INTO bot_settings VALUES (?,?,?)', (token, key, str(value)))
    db.commit()
    db.close()


def get_setting(token, key, default=None):
    db  = get_conn()
    row = db.execute('SELECT value FROM bot_settings WHERE bot_token=? AND key=?', (token, key)).fetchone()
    db.close()
    return row['value'] if row else default


# ── Child Bots ────────────────────────────────────────────────

def save_child_bot(owner_id, token, name, username):
    db = get_conn()
    try:
        db.execute('INSERT INTO child_bots (owner_id,bot_token,bot_name,bot_username) VALUES (?,?,?,?)',
                   (owner_id, token, name, username))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        db.close()


def get_bot(token):
    db  = get_conn()
    row = db.execute('SELECT * FROM child_bots WHERE bot_token=?', (token,)).fetchone()
    db.close()
    return dict(row) if row else None


def get_all_active_bots():
    db   = get_conn()
    rows = db.execute('SELECT bot_token, bot_name, owner_id FROM child_bots WHERE is_active=1').fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_user_bots(owner_id):
    db   = get_conn()
    rows = db.execute('SELECT * FROM child_bots WHERE owner_id=?', (owner_id,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def delete_child_bot(owner_id, token):
    db = get_conn()
    db.execute('DELETE FROM child_bots WHERE owner_id=? AND bot_token=?', (owner_id, token))
    aff = db.total_changes
    db.commit()
    db.close()
    return aff > 0


def set_maintenance(token, on):
    db = get_conn()
    db.execute('UPDATE child_bots SET maintenance=? WHERE bot_token=?', (1 if on else 0, token))
    db.commit()
    db.close()


def set_welcome_photo(token, file_id):
    db = get_conn()
    db.execute('UPDATE child_bots SET welcome_photo=? WHERE bot_token=?', (file_id, token))
    db.commit()
    db.close()


def set_log_chat(token, chat_id):
    db = get_conn()
    db.execute('UPDATE child_bots SET log_chat_id=? WHERE bot_token=?', (str(chat_id), token))
    db.commit()
    db.close()


# ── Users ─────────────────────────────────────────────────────

def register_user(token, user_id, username, first_name, referred_by=None, level1_ref=None):
    db = get_conn()
    try:
        db.execute('INSERT INTO users (bot_token,user_id,username,first_name,referred_by,level1_ref) VALUES (?,?,?,?,?,?)',
                   (token, user_id, username, first_name, referred_by, level1_ref))
        db.commit()
        is_new = True
    except sqlite3.IntegrityError:
        is_new = False

    if is_new:
        bonus  = float(get_setting(token, 'ref_bonus', 10))
        bonus2 = float(get_setting(token, 'ref_bonus_l2', 2))
        if referred_by:
            db.execute('UPDATE users SET balance=balance+?, total_refs=total_refs+1 WHERE bot_token=? AND user_id=?',
                       (bonus, token, referred_by))
            _check_milestone(db, token, referred_by)
        if level1_ref:
            db.execute('UPDATE users SET balance=balance+?, level2_refs=level2_refs+1 WHERE bot_token=? AND user_id=?',
                       (bonus2, token, level1_ref))
        db.commit()

    db.close()
    return is_new


def _check_milestone(db, token, user_id):
    row = db.execute('SELECT total_refs FROM users WHERE bot_token=? AND user_id=?', (token, user_id)).fetchone()
    if not row:
        return
    ms = db.execute('SELECT bonus FROM milestones WHERE bot_token=? AND ref_count=?',
                    (token, row['total_refs'])).fetchone()
    if ms:
        db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?',
                   (ms['bonus'], token, user_id))


def get_user(token, user_id):
    db  = get_conn()
    row = db.execute('SELECT * FROM users WHERE bot_token=? AND user_id=?', (token, user_id)).fetchone()
    db.close()
    return dict(row) if row else None


def get_all_users(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM users WHERE bot_token=?', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def ban_user(token, user_id):
    db = get_conn()
    db.execute('UPDATE users SET is_banned=1 WHERE bot_token=? AND user_id=?', (token, user_id))
    db.commit()
    db.close()


def unban_user(token, user_id):
    db = get_conn()
    db.execute('UPDATE users SET is_banned=0 WHERE bot_token=? AND user_id=?', (token, user_id))
    db.commit()
    db.close()


def add_balance_db(token, user_id, amount):
    db = get_conn()
    db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?', (amount, token, user_id))
    db.commit()
    db.close()


def set_user_wallet(token, user_id, wallet):
    db = get_conn()
    db.execute('UPDATE users SET wallet=?, wallet_locked=1 WHERE bot_token=? AND user_id=?', (wallet, token, user_id))
    db.commit()
    db.close()


def get_leaderboard(token, limit=10):
    db   = get_conn()
    rows = db.execute('SELECT user_id,username,first_name,total_refs,balance FROM users WHERE bot_token=? AND is_banned=0 ORDER BY total_refs DESC LIMIT ?',
                      (token, limit)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_db_stats(token):
    db = get_conn()
    tu = db.execute('SELECT COUNT(*) as n FROM users WHERE bot_token=?', (token,)).fetchone()['n']
    tb = db.execute('SELECT COALESCE(SUM(balance),0) as s FROM users WHERE bot_token=?', (token,)).fetchone()['s']
    tr = db.execute('SELECT COALESCE(SUM(total_refs),0) as s FROM users WHERE bot_token=?', (token,)).fetchone()['s']
    pw = db.execute("SELECT COUNT(*) as n FROM withdrawals WHERE bot_token=? AND status='pending'", (token,)).fetchone()['n']
    pa = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE bot_token=? AND status='approved'", (token,)).fetchone()['s']
    bn = db.execute('SELECT COUNT(*) as n FROM users WHERE bot_token=? AND is_banned=1', (token,)).fetchone()['n']
    tk = db.execute("SELECT COUNT(*) as n FROM tickets WHERE bot_token=? AND status='open'", (token,)).fetchone()['n']
    db.close()
    return {'total_users': tu, 'total_balance': tb, 'total_refs': tr,
            'pending_withdrawals': pw, 'total_paid': pa, 'banned_users': bn, 'open_tickets': tk}


# ── Daily Bonus ───────────────────────────────────────────────

def claim_daily(token, user_id):
    today = datetime.now().strftime('%Y-%m-%d')
    db    = get_conn()
    row   = db.execute('SELECT last_daily, daily_streak FROM users WHERE bot_token=? AND user_id=?',
                       (token, user_id)).fetchone()
    if not row:
        db.close()
        return False, 0, 0

    last   = row['last_daily']
    streak = row['daily_streak'] or 0

    if last == today:
        db.close()
        return False, 0, streak

    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    streak    = (streak + 1) if last == yesterday else 1
    base      = float(get_setting(token, 'daily_bonus', 2))
    bonus     = round(base + (streak - 1) * 0.5, 2)

    db.execute('UPDATE users SET balance=balance+?, last_daily=?, daily_streak=? WHERE bot_token=? AND user_id=?',
               (bonus, today, streak, token, user_id))
    db.commit()
    db.close()
    return True, bonus, streak


# ── Force Channels ────────────────────────────────────────────

def add_channel(token, channel_id, name, invite):
    db = get_conn()
    try:
        db.execute('INSERT INTO force_channels (bot_token,channel_id,channel_name,invite_link) VALUES (?,?,?,?)',
                   (token, str(channel_id), name, invite))
        db.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    db.close()
    return result


def remove_channel(token, channel_id):
    db = get_conn()
    db.execute('DELETE FROM force_channels WHERE bot_token=? AND channel_id=?', (token, str(channel_id)))
    db.commit()
    db.close()


def get_channels(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM force_channels WHERE bot_token=?', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Withdrawals ───────────────────────────────────────────────

def create_withdrawal(token, user_id, amount, method, address):
    db = get_conn()
    db.execute('UPDATE users SET balance=balance-? WHERE bot_token=? AND user_id=?', (amount, token, user_id))
    db.execute('INSERT INTO withdrawals (bot_token,user_id,amount,method,address) VALUES (?,?,?,?,?)',
               (token, user_id, amount, method, address))
    db.commit()
    wid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return wid


def get_pending_withdrawals(token):
    db   = get_conn()
    rows = db.execute('''SELECT w.*, u.username, u.first_name
                         FROM withdrawals w
                         LEFT JOIN users u ON w.bot_token=u.bot_token AND w.user_id=u.user_id
                         WHERE w.bot_token=? AND w.status='pending'
                         ORDER BY w.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def update_withdrawal(wid, status, reason=''):
    db = get_conn()
    db.execute('UPDATE withdrawals SET status=?, reject_reason=? WHERE id=?', (status, reason, wid))
    if status == 'rejected':
        row = db.execute('SELECT bot_token, user_id, amount FROM withdrawals WHERE id=?', (wid,)).fetchone()
        if row:
            db.execute('UPDATE users SET balance=balance+? WHERE bot_token=? AND user_id=?',
                       (row['amount'], row['bot_token'], row['user_id']))
    db.commit()
    db.close()


def get_withdrawal(wid):
    db  = get_conn()
    row = db.execute('SELECT * FROM withdrawals WHERE id=?', (wid,)).fetchone()
    db.close()
    return dict(row) if row else None


# ── Milestones ────────────────────────────────────────────────

def add_milestone(token, ref_count, bonus):
    db = get_conn()
    try:
        db.execute('INSERT INTO milestones (bot_token,ref_count,bonus) VALUES (?,?,?)', (token, ref_count, bonus))
        db.commit()
        result = True
    except Exception:
        result = False
    db.close()
    return result


def get_milestones(token):
    db   = get_conn()
    rows = db.execute('SELECT * FROM milestones WHERE bot_token=? ORDER BY ref_count', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Tickets ───────────────────────────────────────────────────

def create_ticket(token, user_id, message):
    db = get_conn()
    db.execute('INSERT INTO tickets (bot_token,user_id,message) VALUES (?,?,?)', (token, user_id, message))
    db.commit()
    tid = db.execute('SELECT last_insert_rowid() as id').fetchone()['id']
    db.close()
    return tid


def get_open_tickets(token):
    db   = get_conn()
    rows = db.execute('''SELECT t.*, u.username, u.first_name
                         FROM tickets t
                         LEFT JOIN users u ON t.bot_token=u.bot_token AND t.user_id=u.user_id
                         WHERE t.bot_token=? AND t.status='open'
                         ORDER BY t.created_at''', (token,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def reply_ticket(tid, reply_text):
    db = get_conn()
    db.execute("UPDATE tickets SET reply=?, status='closed' WHERE id=?", (reply_text, tid))
    db.commit()
    db.close()
