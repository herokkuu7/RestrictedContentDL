

import os
import shutil
import psutil
import asyncio
from time import time
from aiohttp import web

# Removed pyleaves import
# from pyleaves import Leaves

from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest, FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from helpers.utils import (
    processMediaGroup,
    progressArgs,
    send_media,
    progress_for_pyrogram,
    refresh_progress_message
)

from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download
)

from helpers.msg import (
    getChatMsgID,
    get_file_name,
    get_parsed_msg
)

from config import PyroConf
from logger import LOGGER

# Initialize the bot client
bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=100,
    parse_mode=ParseMode.MARKDOWN,
    max_concurrent_transmissions=1,
    sleep_threshold=30,
)

# Client for user session
user = Client(
    "user_session",
    workers=100,
    session_string=PyroConf.SESSION_STRING,
    max_concurrent_transmissions=1,
    sleep_threshold=30,
)

RUNNING_TASKS = set()
download_semaphore = None
BATCH_STATES = {}  # Stores state for user interactions: {user_id: {'step': '...', 'data': ...}}

# GLOBAL SETTING FOR DESTINATION CHANNEL
DESTINATION_CHAT_ID = None

def track_task(coro):
    task = asyncio.create_task(coro)
    RUNNING_TASKS.add(task)
    def _remove(_):
        RUNNING_TASKS.discard(task)
    task.add_done_callback(_remove)
    return task


@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "👋 **Welcome to Media Downloader Bot!**\n\n"
        "I can grab photos, videos, audio, and documents from any Telegram post.\n"
        "Just send me a link (paste it directly or use `/dl <link>`),\n"
        "or reply to a message with `/dl`.\n\n"
        "**New Feature:**\n"
        "Use `/batch` to clone/download multiple messages easily!\n"
        "Use `/set <channel_id>` to set a custom upload destination.\n\n"
        "ℹ️ Use `/help` to view all commands and examples.\n"
        "🔒 Make sure the user client is part of the chat.\n\n"
        "Ready? Send me a Telegram post link!"
    )

    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Update Channel", url="https://t.me/itsSmartDev")]]
    )
    await message.reply(welcome_text, reply_markup=markup, disable_web_page_preview=True)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **Media Downloader Bot Help**\n\n"
        "➤ **Single Download**\n"
        "   – Just paste a link or use `/dl <link>`.\n\n"
        "➤ **Batch Process (Simple)**\n"
        "   1. Send `/batch`\n"
        "   2. Send the **Start Link**\n"
        "   3. Send the **Number of Messages** (e.g., 100)\n"
        "   The bot will calculate the range and process them.\n\n"
        "➤ **Destination Settings**\n"
        "   – `/set -100xxxx`: Set a channel for uploads.\n"
        "   – `/set none`: Reset to default (upload to this chat).\n"
        "     *Note: Bot must be admin in the target channel.*\n\n"
        "➤ **Requirements**\n"
        "   – Make sure the user client is part of the chat.\n\n"
        "➤ **Management**\n"
        "   – `/killall` : Cancel all running tasks.\n"
        "   – `/logs` : Get log file.\n"
        "   – `/stats` : System status.\n"
    )
    
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Update Channel", url="https://t.me/itsSmartDev")]]
    )
    await message.reply(help_text, reply_markup=markup, disable_web_page_preview=True)

# -------------------------------------------------------------------------------------
# DESTINATION CHANNEL SETTING
# -------------------------------------------------------------------------------------
@bot.on_message(filters.command("set") & filters.private)
async def set_destination(bot: Client, message: Message):
    global DESTINATION_CHAT_ID
    
    if len(message.command) < 2:
        await message.reply(
            "❌ **Usage:** `/set <channel_id>`\n"
            "Example: `/set -100123456789`\n"
            "To reset: `/set none`"
        )
        return

    input_arg = message.command[1]

    if input_arg.lower() == "none":
        DESTINATION_CHAT_ID = None
        await message.reply("✅ **Destination removed.** Files will be sent to this chat.")
        return

    try:
        # Attempt to interpret as integer ID
        try:
            target_id = int(input_arg)
        except ValueError:
            # Fallback: maybe a username?
            chat_obj = await bot.get_chat(input_arg)
            target_id = chat_obj.id

        # Verify bot permissions by sending a test message
        try:
            sent_msg = await bot.send_message(target_id, "✅ **Destination Channel Connected Successfully!**")
            # Optional: delete the test message after a few seconds
            # await asyncio.sleep(5)
            # await sent_msg.delete()
        except Exception as e:
            await message.reply(
                f"❌ **Failed to connect to channel `{target_id}`**.\n\n"
                f"**Error:** `{e}`\n"
                "👉 Make sure the Bot is an **Admin** in that channel with post permissions."
            )
            return

        DESTINATION_CHAT_ID = target_id
        await message.reply(f"✅ **Destination Channel Set!**\nAll downloads will now be uploaded to ID: `{target_id}`")
        LOGGER(__name__).info(f"Destination channel set to {target_id} by user {message.from_user.id}")

    except Exception as e:
        await message.reply(f"❌ **Error:** {str(e)}")


# -------------------------------------------------------------------------------------
# CORE DOWNLOAD LOGIC (With Cloning)
# -------------------------------------------------------------------------------------
async def handle_download(bot: Client, message: Message, post_url: str, silent: bool = False):
    async with download_semaphore:
        if "?" in post_url:
            post_url = post_url.split("?", 1)[0]

        # Determine target chat
        target_chat_id = DESTINATION_CHAT_ID if DESTINATION_CHAT_ID else message.chat.id

        try:
            chat_id, message_id = getChatMsgID(post_url)
            chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
            
            LOGGER(__name__).info(f"Processing URL: {post_url}")

            # --- 1. TRY DIRECT CLONE (Optimization) ---
            # Strategies:
            # A. User -> Destination (Fastest, requires User to be Admin in Dest)
            # B. Bot -> Destination (Fastest, requires Bot to be in Source)
            # C. User -> Bot -> Destination (Relay, requires Source to be Cloneable)
            
            cloned = False
            
            # ATTEMPT A: User Client Direct
            try:
                if chat_message.media_group_id:
                    await user.copy_media_group(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                else:
                    await user.copy_message(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                cloned = True
                LOGGER(__name__).info(f"Directly cloned via User: {post_url}")
            except Exception as e_user:
                LOGGER(__name__).info(f"User direct clone failed: {e_user}")

                # ATTEMPT B: Bot Client Direct
                try:
                    if chat_message.media_group_id:
                        await bot.copy_media_group(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                    else:
                        await bot.copy_message(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                    cloned = True
                    LOGGER(__name__).info(f"Directly cloned via Bot: {post_url}")
                except Exception as e_bot:
                    LOGGER(__name__).info(f"Bot direct clone failed: {e_bot}")

                    # ATTEMPT C: Relay (User -> Bot -> Destination)
                    try:
                        if not bot.me:
                            await bot.get_me()
                        
                        bot_username = bot.me.username
                        LOGGER(__name__).info(f"Attempting Relay Clone via {bot_username}...")

                        if chat_message.media_group_id:
                            # 1. User copies to Bot
                            relayed_msgs = await user.copy_media_group(
                                chat_id=bot_username,
                                from_chat_id=chat_id,
                                message_id=message_id
                            )
                            # 2. Bot copies to Destination
                            if relayed_msgs:
                                await bot.copy_media_group(
                                    chat_id=target_chat_id,
                                    from_chat_id=bot.me.id,
                                    message_id=relayed_msgs[0].id
                                )
                        else:
                            # 1. User copies to Bot
                            relayed_msg = await user.copy_message(
                                chat_id=bot_username,
                                from_chat_id=chat_id,
                                message_id=message_id
                            )
                            # 2. Bot copies to Destination
                            await bot.copy_message(
                                chat_id=target_chat_id,
                                from_chat_id=bot.me.id,
                                message_id=relayed_msg.id
                            )
                            # 3. Cleanup
                            try:
                                await relayed_msg.delete()
                            except:
                                pass

                        cloned = True
                        LOGGER(__name__).info(f"Relay clone success: {post_url}")

                    except Exception as e_relay:
                        LOGGER(__name__).info(f"Relay clone failed: {e_relay}")

            # If any clone attempt worked, exit early
            if cloned:
                await asyncio.sleep(PyroConf.FLOOD_WAIT_DELAY)
                return 
            # ------------------------------------------

            # --- 2. FALLBACK: DOWNLOAD & UPLOAD ---
            # LOGGER(__name__).info("All clone methods failed. Falling back to Download & Upload.")
            
            if chat_message.document or chat_message.video or chat_message.audio:
                file_size = (
                    chat_message.document.file_size
                    if chat_message.document
                    else chat_message.video.file_size
                    if chat_message.video
                    else chat_message.audio.file_size
                )

                if not await fileSizeLimit(
                    file_size, message, "download", user.me.is_premium
                ):
                    return

            parsed_caption = await get_parsed_msg(
                chat_message.caption or "", chat_message.caption_entities
            )
            parsed_text = await get_parsed_msg(
                chat_message.text or "", chat_message.entities
            )

            if chat_message.media_group_id:
                if not await processMediaGroup(chat_message, bot, message, destination_chat_id=target_chat_id):
                    if not silent:
                        await message.reply(
                            "**Could not extract any valid media from the media group.**"
                        )
                return

            elif chat_message.media:
                start_time = time()
                
                # --- NEW LOGIC: Generic Start Message + ID in Progress Header ---
                if not silent:
                    # We send a generic message to initialize the progress bar container
                    progress_message = await message.reply("**⏳ Initializing...**")
                    progress_func = progress_for_pyrogram
                    # Inject the ID into the Action Header string
                    progress_action_str = f"📥 Downloading (ID: {message_id})"
                    prog_args = progressArgs(progress_action_str, progress_message, start_time)
                else:
                    progress_message = None
                    progress_func = None
                    prog_args = None

                filename = get_file_name(message_id, chat_message)
                download_path = get_download_path(message.id, filename)

                media_path = await chat_message.download(
                    file_name=download_path,
                    progress=progress_func, # Use the variable
                    progress_args=prog_args, # Use the variable
                )

                if not media_path or not os.path.exists(media_path):
                    if progress_message: await progress_message.edit("**❌ Download failed: File not saved properly**")
                    return

                file_size = os.path.getsize(media_path)
                if file_size == 0:
                    if progress_message: await progress_message.edit("**❌ Download failed: File is empty**")
                    cleanup_download(media_path)
                    return

                LOGGER(__name__).info(f"Downloaded media: {media_path} (Size: {file_size} bytes)")

                media_type = (
                    "photo"
                    if chat_message.photo
                    else "video"
                    if chat_message.video
                    else "audio"
                    if chat_message.audio
                    else "document"
                )
                
                await send_media(
                    bot,
                    message,
                    media_path,
                    media_type,
                    parsed_caption,
                    progress_message, # Pass None if silent
                    start_time,
                    destination_chat_id=target_chat_id
                )

                cleanup_download(media_path)
                
                # Only delete if we actually sent a status message
                if progress_message:
                    await progress_message.delete()

            elif chat_message.text or chat_message.caption:
                # Send text to target chat
                if target_chat_id != message.chat.id:
                    await bot.send_message(target_chat_id, parsed_text or parsed_caption)
                else:
                    await message.reply(parsed_text or parsed_caption)
            else:
                if not silent:
                    await message.reply("**No media or text found in the post URL.**")

        except (PeerIdInvalid, BadRequest, KeyError):
            if not silent:
                await message.reply(f"**Error processing {post_url}: User client likely not in chat.**")
        except Exception as e:
            error_message = f"**❌ Error at {post_url}: {str(e)}**"
            if not silent:
                await message.reply(error_message)
            LOGGER(__name__).error(e)


@bot.on_message(filters.command("dl") & filters.private)
async def download_media(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command.**")
        return
    post_url = message.command[1]
    # Single download is NOT silent, so we see progress bars
    await track_task(handle_download(bot, message, post_url, silent=False))


# -------------------------------------------------------------------------------------
# NEW /BATCH INTERACTIVE FLOW
# -------------------------------------------------------------------------------------
@bot.on_message(filters.command("batch") & filters.private)
async def batch_command_start(bot: Client, message: Message):
    # Set initial state
    BATCH_STATES[message.from_user.id] = {'step': 'ask_link'}
    await message.reply(
        "🚀 **Batch Mode Initiated**\n\n"
        "Please send the **Start Link** of the first post you want to download."
    )


# Generic Text Handler (Handles both single links AND batch conversation steps)
@bot.on_message(filters.private & ~filters.command(["start", "help", "dl", "batch", "stats", "logs", "killall", "set"]))
async def handle_text_and_states(bot: Client, message: Message):
    # 1. Check if user is in a Batch conversation
    user_id = message.from_user.id
    state = BATCH_STATES.get(user_id)

    if state:
        # --- Step 1: User sent the Link ---
        if state['step'] == 'ask_link':
            if not message.text.startswith("https://t.me/"):
                await message.reply("❌ Invalid link. Please send a valid Telegram post link (e.g., https://t.me/channel/100).")
                return
            
            # Store link and move to next step
            BATCH_STATES[user_id]['start_link'] = message.text
            BATCH_STATES[user_id]['step'] = 'ask_count'
            await message.reply(
                "✅ Link accepted.\n\n"
                "**How many messages** do you want to process starting from there?\n"
                "(Send a number, e.g., `100`)"
            )
            return

        # --- Step 2: User sent the Count ---
        elif state['step'] == 'ask_count':
            if not message.text.isdigit():
                await message.reply("❌ Please send a valid number.")
                return
            
            count = int(message.text)
            start_link = BATCH_STATES[user_id]['start_link']
            
            # Clean up state
            del BATCH_STATES[user_id]
            
            # Execute Batch
            await execute_batch_logic(bot, message, start_link, count)
            return

    # 2. If not in state, treat as a single download link (if it looks like a link)
    if message.text and not message.text.startswith("/"):
        await track_task(handle_download(bot, message, message.text, silent=False))


# Helper to run the batch loop
async def execute_batch_logic(bot: Client, message: Message, start_link: str, count: int):
    try:
        start_chat, start_id = getChatMsgID(start_link)
    except Exception as e:
        return await message.reply(f"**❌ Error parsing start link:\n{e}**")

    # Calculate End ID
    end_id = start_id + count - 1
    
    prefix = start_link.rsplit("/", 1)[0]
    
    loading = await message.reply(
        f"📥 **Starting Batch Process**\n"
        f"From: `{start_id}`\n"
        f"To: `{end_id}`\n"
        f"Total: `{count}` posts"
    )

    downloaded = skipped = failed = 0
    skipped_streak = 0
    batch_tasks = []
    BATCH_SIZE = PyroConf.BATCH_SIZE

    for msg_id in range(start_id, end_id + 1):
        url = f"{prefix}/{msg_id}"
        try:
            # Check if message exists/is empty
            try:
                chat_msg = await user.get_messages(chat_id=start_chat, message_ids=msg_id)
            except FloodWait as e:
                LOGGER(__name__).warning(f"FloodWait while fetching {url}. Sleeping {e.value}s.")
                await asyncio.sleep(e.value)
                chat_msg = await user.get_messages(chat_id=start_chat, message_ids=msg_id)

            if not chat_msg:
                skipped += 1
                skipped_streak += 1
                if skipped_streak >= BATCH_SIZE:
                    await asyncio.sleep(4)
                    skipped_streak = 0
                continue

            has_media = bool(chat_msg.media_group_id or chat_msg.media)
            has_text  = bool(chat_msg.text or chat_msg.caption)
            if not (has_media or has_text):
                skipped += 1
                skipped_streak += 1
                if skipped_streak >= BATCH_SIZE:
                    await asyncio.sleep(4)
                    skipped_streak = 0
                continue
            skipped_streak = 0

            # Spawn task - Enable Silent Mode for Batch to avoid FloodWait!
            # Change silent=False to silent=True if you want completely silent batch
            # But user wants progress bars. If user wants progress bars, we MUST use silent=False
            # BUT we implemented the 25s delay in utils.py so it is SAFE now.
            # So we set silent=False here to show bars as requested.
            task = track_task(handle_download(bot, message, url, silent=False))
            batch_tasks.append(task)

            # Wait if batch size reached
            if len(batch_tasks) >= BATCH_SIZE:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, asyncio.CancelledError):
                        await loading.delete()
                        return await message.reply(
                            f"**❌ Batch canceled** after processing `{downloaded}` posts."
                        )
                    elif isinstance(result, Exception):
                        failed += 1
                        LOGGER(__name__).error(f"Error: {result}")
                    else:
                        downloaded += 1

                batch_tasks.clear()
                # Flood wait to be safe
                await asyncio.sleep(PyroConf.FLOOD_WAIT_DELAY)

        except Exception as e:
            failed += 1
            LOGGER(__name__).error(f"Error at {url}: {e}")

    # Process remaining tasks
    if batch_tasks:
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                failed += 1
            else:
                downloaded += 1

    await loading.delete()
    await message.reply(
        "**✅ Batch Process Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Processed** : `{downloaded}`\n"
        f"⏭️ **Skipped** : `{skipped}`\n"
        f"❌ **Failed** : `{failed}`"
    )


@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    total, used, free = shutil.disk_usage(".")
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
    recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
    
    stats_msg = (
        "**Bot Status**\n\n"
        f"**➜ Uptime:** `{currentTime}`\n"
        f"**➜ Disk Free:** `{free}`\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`"
    )
    await message.reply(stats_msg)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    if os.path.exists("logs.txt"):
        await message.reply_document(document="logs.txt", caption="**Logs**")
    else:
        await message.reply("**Not exists**")


@bot.on_callback_query(filters.regex("^refresh_progress$"))
async def refresh_progress_callback(_, query):
    refreshed = await refresh_progress_message(query.message)
    if refreshed:
        await query.answer("Progress refreshed.")
    else:
        await query.answer("No active progress for this message.", show_alert=True)


@bot.on_message(filters.command("killall") & filters.private)
async def cancel_all_tasks(_, message: Message):
    cancelled = 0
    # Clear state if any
    if message.from_user.id in BATCH_STATES:
        del BATCH_STATES[message.from_user.id]
        
    for task in list(RUNNING_TASKS):
        if not task.done():
            task.cancel()
            cancelled += 1
    await message.reply(f"**Cancelled {cancelled} running task(s).**")


async def initialize():
    global download_semaphore
    download_semaphore = asyncio.Semaphore(PyroConf.MAX_CONCURRENT_DOWNLOADS)


# -------------------------------------------------------------------------------------
# Dummy Web Server for Render
# -------------------------------------------------------------------------------------
async def web_server():
    async def handle(request):
        return web.Response(text="Bot is running!")

    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv('PORT', 8080)))
    await site.start()
    LOGGER(__name__).info(f"Web server started on port {os.getenv('PORT', 8080)}")


# -------------------------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        LOGGER(__name__).info("Bot Started!")
        loop = asyncio.get_event_loop()
        
        # Initialize semaphore
        loop.run_until_complete(initialize())
        
        # Start the User Client
        user.start()
        
        # Start the Dummy Web Server
        loop.run_until_complete(web_server())
        
        # Start the Bot Client
        bot.run()
        
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        LOGGER(__name__).info("Bot Stopped")
