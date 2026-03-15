import asyncio
import logging
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters
)

logger = logging.getLogger(__name__)

# { bot_token: Application }
running_bots = {}


async def start_child_bot(bot_token: str, bot_name: str):
    """Child bot চালু করো — same as working version"""
    if bot_token in running_bots:
        logger.info(f"Bot {bot_name} already running")
        return True

    try:
        app = Application.builder().token(bot_token).build()

        # ── Refer & Earn handlers ────────────────────────────
        from refer import cmd_start, cb_refer, msg_refer, cb_fj
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CallbackQueryHandler(cb_fj,    pattern=r'^fj:'))
        app.add_handler(CallbackQueryHandler(cb_refer,  pattern=r'^r:'))
        app.add_handler(CallbackQueryHandler(cb_refer,  pattern=r'^wa:'))

        # ── Admin panel handlers ─────────────────────────────
        from adminp import cmd_adminpanel, cb_admin, msg_admin
        app.add_handler(CommandHandler("adminpanel", cmd_adminpanel))
        app.add_handler(CallbackQueryHandler(cb_admin, pattern=r'^ap:'))

        # ── Text/Photo router ────────────────────────────────
        async def on_text(update, context):
            if context.user_data.get('ap_step'):
                await msg_admin(update, context)
            else:
                await msg_refer(update, context)

        async def on_media(update, context):
            if context.user_data.get('ap_step'):
                await msg_admin(update, context)

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
        app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, on_media))

        # ── Start ────────────────────────────────────────────
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        running_bots[bot_token] = app
        logger.info(f"✅ Child Bot started: {bot_name}")
        return True

    except Exception as e:
        logger.error(f"❌ Child Bot failed ({bot_name}): {e}")
        return False


async def stop_child_bot(bot_token: str):
    if bot_token not in running_bots:
        return False
    try:
        app = running_bots[bot_token]
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        del running_bots[bot_token]
        return True
    except Exception as e:
        logger.error(f"Stop error: {e}")
        return False


async def restart_child_bot(bot_token: str, bot_name: str):
    await stop_child_bot(bot_token)
    await asyncio.sleep(1)
    return await start_child_bot(bot_token, bot_name)


def is_bot_running(bot_token: str) -> bool:
    return bot_token in running_bots


def get_running_count() -> int:
    return len(running_bots)
