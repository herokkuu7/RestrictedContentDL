import os
import asyncio
import time
import math
from typing import Optional
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for

from pyrogram.parser import Parser
from pyrogram.utils import get_channel_id
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    Voice,
)
from pyrogram.errors import MessageNotModified

from helpers.files import (
    fileSizeLimit,
    cleanup_download,
    get_readable_file_size,
    get_readable_time
)

from helpers.msg import get_parsed_msg
from logger import LOGGER


# Progress bar template
PROGRESS_BAR = """
Percentage: {percentage:.2f}% | {current}/{total}
Speed: {speed}
Elapsed Time: {elapsed_time}
Estimated Time Left: {est_time}
"""

# Cache to limit progress updates
PROGRESS_CACHE = {}
PROGRESS_STATE = {}
PROGRESS_REFRESH_COOLDOWN = {}


def progress_keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔄 Refresh", callback_data="refresh_progress")]]
    )


def build_progress_text(
    current,
    total,
    action,
    template,
    finish,
    unfinish,
    speed_text,
    elapsed_text,
    etl_text
):
    percentage = (current * 100) / total

    bar_len = 20
    filled = int(percentage / 100 * bar_len)
    bar = finish * filled + unfinish * (bar_len - filled)

    current_size = get_readable_file_size(current)
    total_size = get_readable_file_size(total)
    text = template.format(
        percentage=percentage,
        current=current_size,
        total=total_size,
        speed=speed_text,
        elapsed_time=elapsed_text,
        est_time=etl_text
    )

    return f"**{action}**\n{bar}\n{text}"


def get_readable_speed_mb(speed_in_bytes: float) -> str:
    if speed_in_bytes <= 0:
        return "0.00 MB/s"
    speed_mb = speed_in_bytes / (1024 * 1024)
    return f"{speed_mb:.2f} MB/s"


async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)

    stdout, stderr = await proc.communicate()

    try:
        stdout = stdout.decode().strip()
    except Exception:
        stdout = "Unable to decode the response!"

    try:
        stderr = stderr.decode().strip()
    except Exception:
        stderr = "Unable to decode the error!"

    return stdout, stderr, proc.returncode


async def get_media_info(path):
    try:
        result = await cmd_exec([
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", "-show_streams", path,
        ])
    except Exception as e:
        LOGGER(__name__).error(f"Get Media Info: {e}. File: {path}")
        return 0, None, None, None, None

    if result[0] and result[2] == 0:
        try:
            import json
            data = json.loads(result[0])

            fields = data.get("format", {})
            duration = round(float(fields.get("duration", 0)))

            tags = fields.get("tags", {})
            artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
            title = tags.get("title") or tags.get("TITLE") or tags.get("Title")

            width = None
            height = None
            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video":
                    width = stream.get("width")
                    height = stream.get("height")
                    break

            return duration, artist, title, width, height
        except Exception as e:
            LOGGER(__name__).error(f"Error parsing media info: {e}")
            return 0, None, None, None, None

    return 0, None, None, None, None


async def get_video_thumbnail(video_file, duration):
    os.makedirs("Assets", exist_ok=True)
    output = os.path.join("Assets", "video_thumb.jpg")

    if duration is None:
        duration = (await get_media_info(video_file))[0]

    if not duration:
        duration = 3

    duration //= 2

    if os.path.exists(output):
        try:
            os.remove(output)
        except Exception:
            pass

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", str(duration), "-i", video_file,
        "-vframes", "1", "-q:v", "2",
        "-y", output,
    ]

    try:
        _, err, code = await wait_for(cmd_exec(cmd), timeout=60)
        if code != 0 or not os.path.exists(output):
            LOGGER(__name__).warning(f"Thumbnail generation failed: {err}")
            return None
    except Exception as e:
        LOGGER(__name__).warning(f"Thumbnail generation error: {e}")
        return None

    return output


def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time, PROGRESS_BAR, "▓", "░")


async def progress_for_pyrogram(
    current,
    total,
    action,
    message,
    start_time,
    template,
    finish,
    unfinish
):
    now = time.time()

    is_download = "Download" in action
    size_mb = total / (1024 * 1024)
    if is_download:
        interval = 25 if size_mb < 500 else 20
    else:
        interval = 5 if size_mb < 500 else 10

    state = PROGRESS_STATE.get(message.id)
    if not state:
        state = {
            "start_time": start_time,
            "last_time": now,
            "last_current": current,
            "template": template,
            "finish": finish,
            "unfinish": unfinish,
        }
    PROGRESS_STATE[message.id] = {
        "current": current,
        "total": total,
        "action": action,
        "start_time": state["start_time"],
        "last_time": state["last_time"],
        "last_current": state["last_current"],
        "template": template,
        "finish": finish,
        "unfinish": unfinish,
    }

    last_update = PROGRESS_CACHE.get(message.id, 0)
    if current != total and (now - last_update) < interval:
        return

    PROGRESS_CACHE[message.id] = now

    elapsed_time = now - PROGRESS_STATE[message.id]["start_time"]
    if elapsed_time <= 0:
        elapsed_time = 0.1

    delta_time = now - PROGRESS_STATE[message.id]["last_time"]
    if delta_time <= 0:
        delta_time = 0.1
    delta_bytes = current - PROGRESS_STATE[message.id]["last_current"]
    if delta_bytes < 0:
        delta_bytes = 0

    speed_value = delta_bytes / delta_time if delta_bytes else (current / elapsed_time)
    speed_text = get_readable_speed_mb(speed_value)

    remaining_bytes = total - current
    if speed_value > 0:
        etl_seconds = remaining_bytes / speed_value
        etl_text = get_readable_time(int(etl_seconds))
    else:
        etl_text = "0s"
    elapsed_text = get_readable_time(int(elapsed_time))

    text = build_progress_text(
        current=current,
        total=total,
        action=action,
        template=template,
        finish=finish,
        unfinish=unfinish,
        speed_text=speed_text,
        elapsed_text=elapsed_text,
        etl_text=etl_text
    )

    try:
        await message.edit(text, reply_markup=progress_keyboard())
    except MessageNotModified:
        pass
    except Exception as e:
        LOGGER(__name__).error(f"Progress Error: {e}")

    if current == total:
        PROGRESS_CACHE.pop(message.id, None)
        PROGRESS_STATE.pop(message.id, None)
        PROGRESS_REFRESH_COOLDOWN.pop(message.id, None)
    else:
        PROGRESS_STATE[message.id]["last_time"] = now
        PROGRESS_STATE[message.id]["last_current"] = current


async def refresh_progress_message(message):
    state = PROGRESS_STATE.get(message.id)
    if not state:
        return False, 0

    now = time.time()
    last_refresh = PROGRESS_REFRESH_COOLDOWN.get(message.id, 0)
    if (now - last_refresh) < 4:
        remaining = int(math.ceil(4 - (now - last_refresh)))
        return False, remaining

    PROGRESS_REFRESH_COOLDOWN[message.id] = now
    PROGRESS_CACHE[message.id] = 0

    elapsed_time = now - state["start_time"]
    if elapsed_time <= 0:
        elapsed_time = 0.1

    delta_time = now - state.get("last_time", now)
    if delta_time <= 0:
        delta_time = 0.1
    delta_bytes = state["current"] - state.get("last_current", state["current"])
    if delta_bytes < 0:
        delta_bytes = 0

    speed_value = delta_bytes / delta_time if delta_bytes else (state["current"] / elapsed_time)
    speed_text = get_readable_speed_mb(speed_value)

    remaining_bytes = state["total"] - state["current"]
    if speed_value > 0:
        etl_seconds = remaining_bytes / speed_value
        etl_text = get_readable_time(int(etl_seconds))
    else:
        etl_text = "0s"
    elapsed_text = get_readable_time(int(elapsed_time))

    text = build_progress_text(
        current=state["current"],
        total=state["total"],
        action=state["action"],
        template=state["template"],
        finish=state["finish"],
        unfinish=state["unfinish"],
        speed_text=speed_text,
        elapsed_text=elapsed_text,
        etl_text=etl_text
    )
    try:
        await message.edit(text, reply_markup=progress_keyboard())
    except MessageNotModified:
        return True, 0
    except Exception as e:
        LOGGER(__name__).error(f"Progress Refresh Error: {e}")
        return False, 0

    state["last_time"] = now
    state["last_current"] = state["current"]

    return True, 0


async def send_media(
    bot,
    message,
    media_path,
    media_type,
    caption,
    progress_message,
    start_time,
    destination_chat_id=None
):
    file_size = os.path.getsize(media_path)
    target_chat_id = destination_chat_id or message.chat.id

    if not await fileSizeLimit(file_size, message, "upload"):
        return

    if progress_message:
        progress_args = progressArgs("📥 Uploading Progress", progress_message, start_time)
        progress_func = progress_for_pyrogram
    else:
        progress_args = None
        progress_func = None

    send_kwargs = {
        "caption": caption or "",
        "progress": progress_func,
        "progress_args": progress_args
    }

    try:
        if media_type == "photo":
            await bot.send_photo(target_chat_id, media_path, **send_kwargs)

        elif media_type == "video":
            duration, _, _, width, height = await get_media_info(media_path)
            width = width or 640
            height = height or 480
            thumb = await get_video_thumbnail(media_path, duration)

            await bot.send_video(
                target_chat_id,
                media_path,
                duration=duration,
                width=width,
                height=height,
                thumb=thumb,
                supports_streaming=True,
                **send_kwargs
            )

        elif media_type == "audio":
            duration, artist, title, _, _ = await get_media_info(media_path)
            await bot.send_audio(
                target_chat_id,
                media_path,
                duration=duration,
                performer=artist,
                title=title,
                **send_kwargs
            )

        elif media_type == "document":
            await bot.send_document(target_chat_id, media_path, **send_kwargs)

    except Exception as e:
        LOGGER(__name__).error(f"Error sending media: {e}")


async def download_single_media(msg, progress_message, start_time):
    try:
        media_path = await msg.download(
            progress=progress_for_pyrogram,
            progress_args=progressArgs(
                "📥 Downloading Progress",
                progress_message,
                start_time
            )
        )

        parsed_caption = await get_parsed_msg(
            msg.caption or "",
            msg.caption_entities
        )

        if msg.photo:
            return "success", media_path, InputMediaPhoto(media_path, parsed_caption)
        if msg.video:
            return "success", media_path, InputMediaVideo(media_path, parsed_caption)
        if msg.document:
            return "success", media_path, InputMediaDocument(media_path, parsed_caption)
        if msg.audio:
            return "success", media_path, InputMediaAudio(media_path, parsed_caption)

    except Exception as e:
        LOGGER(__name__).info(f"Error downloading media: {e}")
        return "error", None, None

    return "skip", None, None


async def processMediaGroup(chat_message, bot, message, destination_chat_id=None):
    media_group_messages = await chat_message.get_media_group()

    valid_media = []
    temp_paths = []
    invalid_paths = []

    target_chat_id = destination_chat_id or message.chat.id
    start_time = time.time()

    progress_message = await message.reply(
        f"📥 Downloading media group... ({len(media_group_messages)} files)"
    )

    download_tasks = [
        download_single_media(msg, progress_message, start_time)
        for msg in media_group_messages
        if msg.photo or msg.video or msg.document or msg.audio
    ]

    results = await asyncio.gather(*download_tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            continue

        status, media_path, media_obj = result
        if status == "success":
            temp_paths.append(media_path)
            valid_media.append(media_obj)

    if valid_media:
        try:
            await bot.send_media_group(target_chat_id, valid_media)
            await progress_message.delete()
        except Exception:
            for media in valid_media:
                try:
                    if isinstance(media, InputMediaPhoto):
                        await bot.send_photo(target_chat_id, media.media, media.caption)
                    elif isinstance(media, InputMediaVideo):
                        await bot.send_video(target_chat_id, media.media, caption=media.caption)
                    elif isinstance(media, InputMediaDocument):
                        await bot.send_document(target_chat_id, media.media, caption=media.caption)
                    elif isinstance(media, InputMediaAudio):
                        await bot.send_audio(target_chat_id, media.media, caption=media.caption)
                except Exception:
                    pass

        for path in temp_paths + invalid_paths:
            cleanup_download(path)

        return True

    await progress_message.delete()
    for path in invalid_paths:
        cleanup_download(path)

    return False
