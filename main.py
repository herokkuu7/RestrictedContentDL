import os
import shutil
import psutil
import asyncio
from time import time
from aiohttp import web

from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest, FloodWait, FileReferenceExpired
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
BATCH_STATES = {}  

PIN_PROMPTS = {}


def build_pin_prompt_text(seconds_left: int) -> str:
    return (
        "📌 **Pin First Post?**\n\n"
        "Do you want me to pin the **first post of this batch** after it is uploaded?\n"
        f"⏳ Auto-continue without pinning in **{seconds_left}s**."
    )


async def finalize_pin_prompt(user_id: int, timed_out: bool = False):
    prompt = PIN_PROMPTS.get(user_id)
    if not prompt or prompt.get("done"):
        return

    prompt["done"] = True
    event = prompt["event"]
    if not event.is_set():
        event.set()

    try:
        await prompt["prompt_msg"].edit(
            "⏱️ No selection received in 10 seconds. Proceeding without pinning."
            if timed_out
            else ("✅ Pin enabled for this batch." if prompt["pin_first"] else "➡️ Proceeding without pinning.")
        )
    except Exception:
        pass


async def pin_prompt_countdown(user_id: int):
    prompt = PIN_PROMPTS.get(user_id)
    if not prompt:
        return

    intervals = [7, 4, 1]
    for seconds_left in intervals:
        await asyncio.sleep(3)
        current = PIN_PROMPTS.get(user_id)
        if not current or current.get("done"):
            return
        try:
            await current["prompt_msg"].edit(
                build_pin_prompt_text(seconds_left),
                reply_markup=current["markup"]
            )
        except Exception:
            return

    await asyncio.sleep(1)
    current = PIN_PROMPTS.get(user_id)
    if current and not current.get("done"):
        await finalize_pin_prompt(user_id, timed_out=True)

# GLOBAL SETTING FOR DESTINATION CHANNEL
DESTINATION_CHAT_ID = None


async def resolve_target_chat_id(bot: Client, source_message: Message | None = None):
    if DESTINATION_CHAT_ID:
        return DESTINATION_CHAT_ID
    if source_message:
        return source_message.chat.id
    if not bot.me:
        await bot.get_me()
    return bot.me.id

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
        "   – `/set none`: Reset to default (upload to the bot chat).\n"
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
        await message.reply("✅ **Destination removed.** Files will now be stored in the bot chat.")
        return

    try:
        try:
            target_id = int(input_arg)
        except ValueError:
            chat_obj = await bot.get_chat(input_arg)
            target_id = chat_obj.id

        try:
            sent_msg = await bot.send_message(target_id, "✅ **Destination Channel Connected Successfully!**")
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
# CORE DOWNLOAD LOGIC
# -------------------------------------------------------------------------------------
async def handle_download(bot: Client, message: Message, post_url: str, silent: bool = False, pre_fetched_msg=None, abort_event: asyncio.Event = None):
    # If abort signal is triggered globally, exit instantly.
    if abort_event and abort_event.is_set():
        return "aborted"

    async with download_semaphore:
        if abort_event and abort_event.is_set():
            return "aborted"
            
        if "?" in post_url:
            post_url = post_url.split("?", 1)[0]

        target_chat_id = await resolve_target_chat_id(bot, message)
        progress_message = None

        try:
            chat_id, message_id, thread_id = getChatMsgID(post_url)
            
            if pre_fetched_msg:
                chat_message = pre_fetched_msg
            else:
                chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
            
            LOGGER(__name__).info(f"Processing URL: {post_url}")
            cloned = False
            
            # --- CLONE ATTEMPTS ---
            try:
                if chat_message.media_group_id:
                    await user.copy_media_group(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                else:
                    await user.copy_message(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                cloned = True
                LOGGER(__name__).info(f"Directly cloned via User: {post_url}")
            except FloodWait as e:
                raise e # DO NOT MASK FLOODWAIT!
            except Exception as e_user:
                LOGGER(__name__).info(f"User direct clone failed: {e_user}")

                try:
                    if chat_message.media_group_id:
                        await bot.copy_media_group(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                    else:
                        await bot.copy_message(chat_id=target_chat_id, from_chat_id=chat_id, message_id=message_id)
                    cloned = True
                    LOGGER(__name__).info(f"Directly cloned via Bot: {post_url}")
                except FloodWait as e:
                    raise e
                except Exception as e_bot:
                    LOGGER(__name__).info(f"Bot direct clone failed: {e_bot}")

                    try:
                        if not bot.me:
                            await bot.get_me()
                        bot_username = bot.me.username
                        
                        if chat_message.media_group_id:
                            relayed_msgs = await user.copy_media_group(chat_id=bot_username, from_chat_id=chat_id, message_id=message_id)
                            if relayed_msgs:
                                await bot.copy_media_group(chat_id=target_chat_id, from_chat_id=bot.me.id, message_id=relayed_msgs[0].id)
                        else:
                            relayed_msg = await user.copy_message(chat_id=bot_username, from_chat_id=chat_id, message_id=message_id)
                            await bot.copy_message(chat_id=target_chat_id, from_chat_id=bot.me.id, message_id=relayed_msg.id)
                            try:
                                await relayed_msg.delete()
                            except:
                                pass
                        cloned = True
                        LOGGER(__name__).info(f"Relay clone success: {post_url}")

                    except FloodWait as e:
                        raise e
                    except Exception as e_relay:
                        LOGGER(__name__).info(f"Relay clone failed: {e_relay}")

            if cloned:
                await asyncio.sleep(PyroConf.FLOOD_WAIT_DELAY)
                return "success"

            # --- FALLBACK: DOWNLOAD & UPLOAD ---
            if chat_message.document or chat_message.video or chat_message.audio:
                file_size = (
                    chat_message.document.file_size if chat_message.document
                    else chat_message.video.file_size if chat_message.video
                    else chat_message.audio.file_size
                )
                if not await fileSizeLimit(file_size, message, "download", user.me.is_premium):
                    return "error"

            parsed_caption = await get_parsed_msg(chat_message.caption or "", chat_message.caption_entities)
            parsed_text = await get_parsed_msg(chat_message.text or "", chat_message.entities)

            if chat_message.media_group_id:
                if not await processMediaGroup(chat_message, bot, message, destination_chat_id=target_chat_id):
                    if not silent:
                        await message.reply("**Could not extract any valid media from the media group.**")
                return "success"

            elif chat_message.media:
                start_time = time()
                
                if not silent:
                    progress_message = await message.reply("**⏳ Initializing...**")
                    progress_func = progress_for_pyrogram
                    progress_action_str = f"📥 Downloading (ID: {message_id})"
                    prog_args = progressArgs(progress_action_str, progress_message, start_time)
                else:
                    progress_func = None
                    prog_args = None

                filename = get_file_name(message_id, chat_message)
                download_path = get_download_path(message.id, filename)

                try:
                    media_path = await chat_message.download(
                        file_name=download_path,
                        progress=progress_func,
                        progress_args=prog_args,
                    )
                except FileReferenceExpired:
                    LOGGER(__name__).info(f"File reference expired for {post_url}, refetching message and retrying download once.")
                    chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
                    media_path = await chat_message.download(
                        file_name=download_path,
                        progress=progress_func,
                        progress_args=prog_args,
                    )

                if not media_path or not os.path.exists(media_path):
                    if progress_message: await progress_message.edit("**❌ Download failed: File not saved properly**")
                    return "error"

                file_size = os.path.getsize(media_path)
                if file_size == 0:
                    if progress_message: await progress_message.edit("**❌ Download failed: File is empty**")
                    cleanup_download(media_path)
                    return "error"

                LOGGER(__name__).info(f"Downloaded media: {media_path} (Size: {file_size} bytes)")

                media_type = (
                    "photo" if chat_message.photo
                    else "video" if chat_message.video
                    else "audio" if chat_message.audio
                    else "document"
                )
                
                await send_media(
                    bot, message, media_path, media_type, parsed_caption,
                    progress_message, start_time, destination_chat_id=target_chat_id
                )

                cleanup_download(media_path)
                
                if progress_message:
                    await progress_message.delete()
                    
                return "success"

            elif chat_message.text or chat_message.caption:
                await bot.send_message(target_chat_id, parsed_text or parsed_caption)
                return "success"
            else:
                if not silent:
                    await message.reply("**No media or text found in the post URL.**")
                return "error"

        # --- GLOBAL ERROR HANDLING & ABORT LOGIC ---
        except FloodWait as e:
            if abort_event and not abort_event.is_set():
                abort_event.set() # Trigger global shut down
                await message.reply(f"🚨 **FloodWait Triggered!**\nTelegram requires a wait of `{e.value}` seconds. Process Aborted.")
            if progress_message:
                await progress_message.delete()
            return "aborted"
            
        except (PeerIdInvalid, BadRequest, KeyError):
            if abort_event and abort_event.is_set(): return "aborted"
            err = f"**Error processing {post_url}: User client likely not in chat.**"
            if not silent:
                if progress_message: await progress_message.edit(err)
                else: await message.reply(err)
            return "error"
            
        except Exception as e:
            if "FLOOD_WAIT" in str(e).upper():
                if abort_event and not abort_event.is_set():
                    abort_event.set()
                    await message.reply(f"🚨 **FloodWait Triggered!**\nProcess Aborted.")
                if progress_message:
                    await progress_message.delete()
                return "aborted"
                
            if abort_event and abort_event.is_set(): return "aborted"
            
            error_message = f"**❌ Error at {post_url}: {str(e)}**"
            if not silent:
                if progress_message: await progress_message.edit(error_message)
                else: await message.reply(error_message)
            LOGGER(__name__).error(e)
            return "error"


@bot.on_message(filters.command("dl") & filters.private)
async def download_media_cmd(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command.**")
        return
    post_url = message.command[1]
    
    try:
        await track_task(handle_download(bot, message, post_url, silent=False))
    except FloodWait as e:
        await message.reply(f"🚨 **FloodWait Triggered!**\nTelegram requires a wait of `{e.value}` seconds.")
    except Exception as e:
        if "FLOOD_WAIT" in str(e).upper():
             await message.reply(f"🚨 **FloodWait Triggered!**")


# -------------------------------------------------------------------------------------
# NEW /BATCH INTERACTIVE FLOW
# -------------------------------------------------------------------------------------
@bot.on_message(filters.command("batch") & filters.private)
async def batch_command_start(bot: Client, message: Message):
    BATCH_STATES[message.from_user.id] = {'step': 'ask_link'}
    await message.reply(
        "🚀 **Batch Mode Initiated**\n\n"
        "Please send the **Start Link** of the first post you want to download."
    )


@bot.on_message(filters.private & ~filters.command(["start", "help", "dl", "batch", "stats", "logs", "killall", "set"]))
async def handle_text_and_states(bot: Client, message: Message):
    user_id = message.from_user.id
    state = BATCH_STATES.get(user_id)

    if state:
        if state['step'] == 'ask_link':
            if not message.text.startswith("https://t.me/"):
                await message.reply("❌ Invalid link. Please send a valid Telegram post link (e.g., https://t.me/channel/100).")
                return
            
            BATCH_STATES[user_id]['start_link'] = message.text
            BATCH_STATES[user_id]['step'] = 'ask_count'
            await message.reply(
                "✅ Link accepted.\n\n"
                "**How many messages** do you want to process starting from there?\n"
                "(Send a number, e.g., `100`)"
            )
            return

        elif state['step'] == 'ask_count':
            if not message.text.isdigit():
                await message.reply("❌ Please send a valid number.")
                return
            
            count = int(message.text)
            start_link = BATCH_STATES[user_id]['start_link']
            
            del BATCH_STATES[user_id]

            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes", callback_data=f"pin_decision:yes:{user_id}"),
                InlineKeyboardButton("❌ No", callback_data=f"pin_decision:no:{user_id}"),
            ]])
            prompt_msg = await message.reply(build_pin_prompt_text(10), reply_markup=markup)

            decision_event = asyncio.Event()
            PIN_PROMPTS[user_id] = {
                "event": decision_event,
                "start_link": start_link,
                "count": count,
                "pin_first": False,
                "done": False,
                "prompt_msg": prompt_msg,
                "markup": markup,
            }

            track_task(pin_prompt_countdown(user_id))
            await decision_event.wait()

            prompt = PIN_PROMPTS.pop(user_id, None)
            pin_first = prompt.get("pin_first", False) if prompt else False

            await execute_batch_logic(bot, message, start_link, count, pin_first=pin_first)
            return

    if message.text and not message.text.startswith("/"):
        # Ignore random text in private chat; only Telegram post links should trigger /dl-like behavior.
        if not message.text.startswith("https://t.me/"):
            LOGGER(__name__).info(f"Ignoring non-link private message in idle mode: {message.text[:80]}")
            return
        try:
            await track_task(handle_download(bot, message, message.text, silent=False))
        except FloodWait as e:
            await message.reply(f"🚨 **FloodWait Triggered!**\nWait `{e.value}` seconds.")


# Helper to run the batch loop (NOW HIGHLY OPTIMIZED WITH BULK FETCH)
async def execute_batch_logic(bot: Client, message: Message, start_link: str, count: int, pin_first: bool = False):
    try:
        start_chat, start_id, start_thread_id = getChatMsgID(start_link)
    except Exception as e:
        return await message.reply(f"**❌ Error parsing start link:\n{e}**")

    end_id = start_id + count - 1
    prefix = start_link.rsplit("/", 1)[0]
    
    thread_text = f"\n**Topic/Thread Filter Active**: ID `{start_thread_id}`" if start_thread_id else ""
    loading = await message.reply(
        f"📥 **Starting Batch Process**\n"
        f"From: `{start_id}`\n"
        f"To: `{end_id}`\n"
        f"Total Range Checked: `{count}` posts{thread_text}"
    )

    downloaded = skipped = failed = 0
    batch_tasks = []
    BATCH_SIZE = PyroConf.BATCH_SIZE
    
    abort_event = asyncio.Event() # Shared flag to shut everything down

    all_message_ids = list(range(start_id, end_id + 1))
    chunk_size = 50 # Fetch 50 messages per API call (Instant skipping)

    for i in range(0, len(all_message_ids), chunk_size):
        if abort_event.is_set():
            break
            
        chunk = all_message_ids[i:i+chunk_size]
        
        try:
            # OPTIMIZATION: Fetch in bulk to save API rate limits!
            messages_batch = await user.get_messages(chat_id=start_chat, message_ids=chunk)
        except FloodWait as e:
            await message.reply(f"🚨 **Batch Halted: Read FloodWait Triggered!**\nWait `{e.value}` seconds.")
            abort_event.set()
            break
        except Exception as e:
            if "FLOOD_WAIT" in str(e).upper():
                 await message.reply(f"🚨 **Batch Halted: Read FloodWait Triggered!**")
                 abort_event.set()
                 break
            failed += len(chunk)
            continue

        if getattr(messages_batch, "id", None) is not None:
             messages_batch = [messages_batch]

        for chat_msg in messages_batch:
            if abort_event.is_set():
                break

            if not chat_msg or getattr(chat_msg, 'empty', False):
                skipped += 1
                continue

            if start_thread_id:
                msg_thread = getattr(chat_msg, "message_thread_id", None)
                if msg_thread != start_thread_id:
                    skipped += 1
                    continue

            has_media = bool(chat_msg.media_group_id or chat_msg.media)
            has_text  = bool(chat_msg.text or chat_msg.caption)
            if not (has_media or has_text):
                skipped += 1
                continue

            url = f"{prefix}/{chat_msg.id}"
            task = track_task(handle_download(
                bot, message, url, 
                silent=False, 
                pre_fetched_msg=chat_msg, 
                abort_event=abort_event # Pass the global abort flag
            ))
            batch_tasks.append(task)

            if len(batch_tasks) >= BATCH_SIZE:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                for result in results:
                    if result == "aborted" or abort_event.is_set():
                        pass 
                    elif result == "success":
                        downloaded += 1
                    else:
                        failed += 1
                
                batch_tasks.clear()
                if not abort_event.is_set():
                    await asyncio.sleep(PyroConf.FLOOD_WAIT_DELAY)

    if batch_tasks and not abort_event.is_set():
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        for result in results:
            if result == "aborted" or abort_event.is_set():
                pass
            elif result == "success":
                downloaded += 1
            else:
                failed += 1

    await loading.delete()
    
    completion_text = "**✅ Batch Process Complete!**" if not abort_event.is_set() else "**🛑 Batch Process Stopped (FloodWait)**"
    
    if pin_first:
        try:
            target_chat_id = await resolve_target_chat_id(bot, message)
            await bot.pin_chat_message(target_chat_id, start_id, disable_notification=True)
        except Exception as e:
            LOGGER(__name__).info(f"Could not pin first batch post: {e}")

    await message.reply(
        f"{completion_text}\n"
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


@bot.on_callback_query(filters.regex(r"^pin_decision:(yes|no):(\d+)$"))
async def pin_decision_callback(_, query):
    choice, owner_id_str = query.data.split(":")[1:]
    owner_id = int(owner_id_str)

    if not query.from_user or query.from_user.id != owner_id:
        await query.answer("This prompt is not for you.", show_alert=True)
        return

    prompt = PIN_PROMPTS.get(owner_id)
    if not prompt:
        await query.answer("This decision prompt is no longer active.", show_alert=True)
        return

    if prompt.get("done"):
        await query.answer("Decision already recorded.")
        return

    prompt["pin_first"] = choice == "yes"
    await finalize_pin_prompt(owner_id, timed_out=False)
    await query.answer("Decision saved.")


@bot.on_callback_query(filters.regex("^refresh_progress$"))
async def refresh_progress_callback(_, query):
    refreshed, remaining = await refresh_progress_message(query.message)
    if refreshed:
        await query.answer("Progress refreshed.")
    else:
        if remaining:
            await query.answer(f"Heyy!! Wait for {remaining} sec", show_alert=True)
        else:
            await query.answer("No active progress for this message.", show_alert=True)


@bot.on_message(filters.command("killall") & filters.private)
async def cancel_all_tasks(_, message: Message):
    cancelled = 0
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
        
        loop.run_until_complete(initialize())
        
        user.start()
        
        loop.run_until_complete(web_server())
        
        bot.run()
        
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        LOGGER(__name__).info("Bot Stopped")
