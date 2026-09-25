"""Regression tests for the /batch "pin the first post?" prompt feature.

These tests never touch the network.  `pyrogram.Client` is replaced by an
in-memory fake before `main.py` is imported, so the real handler functions
(the same ones Pyrogram would call) are executed end to end:

    /batch -> start link -> count -> pin prompt -> decision -> batch -> pin

Run with:

    python3 tests/test_pin_feature.py
"""

import asyncio
import os
import sys
import traceback

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# --- environment required by config.py --------------------------------
os.environ.setdefault("BOT_TOKEN", "123456:TESTTOKEN")
os.environ.setdefault("SESSION_STRING", "test-session-string")
os.environ.setdefault("FLOOD_WAIT_DELAY", "0")
os.environ.setdefault("BATCH_SIZE", "2")

import pyrogram  # noqa: E402


# ----------------------------------------------------------------------
# Fake Telegram layer
# ----------------------------------------------------------------------
USER_ID = 424242
CHANNEL_ID = -1009999
BOT_USERNAME = "test_bot"
BOT_ID = 777000
OUTGOING = []          # every message the bot sends / edits, for assertions
REGISTERED = []        # handlers registered through the decorators

# Which chat each id really lives in.  A USER session sending to its OWN id
# lands in Saved Messages - not in the chat with the bot - which is exactly the
# trap that produces 400 MESSAGE_ID_INVALID when the pin is attempted.
CHAT_MESSAGES = {}


class PinRefused(Exception):
    """Stands in for Telegram's 400 MESSAGE_ID_INVALID."""


def logical_chat(role, chat_id):
    if isinstance(chat_id, str):
        return "botchat" if chat_id == BOT_USERNAME else "str:%s" % chat_id
    if chat_id == USER_ID:
        return "botchat" if role == "bot" else "saved"
    if chat_id == BOT_ID:
        return "botchat"
    return "chat:%s" % chat_id


def register(role, chat_id, msg_id):
    key = logical_chat(role, chat_id)
    CHAT_MESSAGES.setdefault(key, {})[msg_id] = True
    return key


class FakeUser(object):
    def __init__(self, uid):
        self.id = uid


class FakeChat(object):
    def __init__(self, cid):
        self.id = cid


class EmptyMessage(object):
    """Telegram returns an empty message when an id does not exist in a chat."""

    def __init__(self, mid):
        self.id = mid
        self.empty = True


class SentMessage(object):
    """A message that now exists in a chat."""

    def __init__(self, msg_id, chat_id):
        self.id = msg_id
        self.chat = FakeChat(chat_id)


class OutgoingMessage(SentMessage):
    _next_id = 1000

    def __init__(self, text, reply_markup=None, chat_id=USER_ID, register_it=True):
        OutgoingMessage._next_id += 1
        SentMessage.__init__(self, OutgoingMessage._next_id, chat_id)
        self.text = text
        self.reply_markup = reply_markup
        self.edits = []
        self.deleted = False
        OUTGOING.append(self)
        if register_it:
            register("bot", chat_id, self.id)

    async def edit(self, text, reply_markup=None, **kwargs):
        self.text = text
        self.reply_markup = reply_markup
        self.edits.append(text)
        return self

    async def delete(self):
        self.deleted = True
        for msgs in CHAT_MESSAGES.values():
            msgs.pop(self.id, None)
        return True

    async def reply(self, text, **kwargs):
        return OutgoingMessage(text, chat_id=self.chat.id)


class IncomingMessage(object):
    """A message sent to the bot by the user."""

    _next_id = 500

    def __init__(self, text, user_id=USER_ID, chat_id=None, message_id=None):
        IncomingMessage._next_id += 1
        self.id = message_id or IncomingMessage._next_id
        self.text = text
        self.caption = None
        self.from_user = FakeUser(user_id)
        self.chat = FakeChat(chat_id if chat_id is not None else user_id)
        self.command = [text.split()[0].lstrip("/")] if text.startswith("/") else []
        self.replies = []

    async def reply(self, text, reply_markup=None, **kwargs):
        msg = OutgoingMessage(text, reply_markup=reply_markup, chat_id=self.chat.id)
        self.replies.append(msg)
        return msg

    async def reply_document(self, *args, **kwargs):
        return OutgoingMessage("document")

    async def delete(self):
        return True


class FakeChatMessage(object):
    """A message fetched from the source channel (pre-fetched by the batch)."""

    def __init__(self, mid):
        self.id = mid
        self.empty = False
        self.media = "video"          # truthy -> treated as downloadable
        self.media_group_id = None
        self.text = None
        self.caption = None
        self.message_thread_id = None
        self.photo = None
        self.video = None
        self.audio = None
        self.document = None


class CallbackQuery(object):
    def __init__(self, data, user_id=USER_ID):
        self.data = data
        self.from_user = FakeUser(user_id)
        self.message = OutgoingMessage("prompt")
        self.answers = []

    async def answer(self, text=None, show_alert=False, **kwargs):
        self.answers.append((text, show_alert))


class FakeClient(object):
    """Drop-in replacement for pyrogram.Client."""

    def __init__(self, name, **kwargs):
        self.name = name
        self.role = "bot" if "bot" in name else "user"
        self.kwargs = kwargs
        self.me = FakeUser(BOT_ID if self.role == "bot" else USER_ID)
        self.me.username = BOT_USERNAME if self.role == "bot" else "tester"
        self.pins = []
        self.unpins = []
        self.pin_error = None
        self.copied = []
        self.created = []
        self.gate = None
        self.fetch_error = None
        self._sent = 0

    # handler registration ------------------------------------------------
    def _record(self, kind):
        def decorator(func):
            REGISTERED.append((kind, func))
            return func
        return decorator

    def on_message(self, *args, **kwargs):
        return self._record("message")

    def on_callback_query(self, *args, **kwargs):
        return self._record("callback_query")

    # pyrogram API used by the bot ---------------------------------------
    def _deliver(self, chat_id, source_id=None):
        self._sent += 1
        new_id = (9000 + source_id) if source_id is not None else (7000 + self._sent)
        register(self.role, chat_id, new_id)
        self.created.append(new_id)
        return SentMessage(new_id, chat_id)

    async def get_me(self):
        return self.me

    async def get_messages(self, chat_id, message_ids):
        if self.fetch_error:
            raise self.fetch_error
        single = not isinstance(message_ids, (list, tuple))
        ids = [message_ids] if single else list(message_ids)
        known = CHAT_MESSAGES.get(logical_chat("bot", chat_id), {}) if self.role == "bot" else None
        out = []
        for mid in ids:
            if known is not None and mid not in known:
                out.append(EmptyMessage(mid))
            else:
                out.append(FakeChatMessage(mid))
        return out[0] if single else out

    async def copy_message(self, chat_id, from_chat_id, message_id, **kwargs):
        if self.gate is not None:
            await self.gate.wait()
        self.copied.append((chat_id, from_chat_id, message_id))
        return self._deliver(chat_id, source_id=message_id)

    async def copy_media_group(self, chat_id, from_chat_id, message_id, **kwargs):
        if self.gate is not None:
            await self.gate.wait()
        self.copied.append((chat_id, from_chat_id, message_id))
        return [self._deliver(chat_id, source_id=message_id)]

    async def send_message(self, chat_id, text, **kwargs):
        return self._deliver(chat_id)

    async def send_photo(self, chat_id, *a, **kwargs):
        return self._deliver(chat_id)

    async def send_video(self, chat_id, *a, **kwargs):
        return self._deliver(chat_id)

    async def send_audio(self, chat_id, *a, **kwargs):
        return self._deliver(chat_id)

    async def send_document(self, chat_id, *a, **kwargs):
        return self._deliver(chat_id)

    async def send_media_group(self, chat_id, media, **kwargs):
        return [self._deliver(chat_id) for _ in media]

    async def pin_chat_message(self, chat_id, message_id, disable_notification=False, **kwargs):
        if self.pin_error:
            raise self.pin_error
        key = logical_chat("bot", chat_id)
        if message_id not in CHAT_MESSAGES.get(key, {}):
            raise PinRefused(
                "[400 MESSAGE_ID_INVALID] message %s does not exist in this chat (%s)"
                % (message_id, key)
            )
        self.pins.append((chat_id, message_id))
        return OutgoingMessage("pinned")

    async def unpin_chat_message(self, chat_id, message_id=None, **kwargs):
        self.unpins.append((chat_id, message_id))
        return True


# Install the fake *before* main.py imports `Client`.
pyrogram.Client = FakeClient

import main  # noqa: E402


# ----------------------------------------------------------------------
# Test helpers
# ----------------------------------------------------------------------
def outgoing_texts():
    """Every text the bot has shown, including earlier edits of a message."""
    texts = []
    for m in OUTGOING:
        texts.append(m.text or "")
        texts.extend(m.edits)
    return texts


def bot_said(*needles):
    haystack = " \n ".join(outgoing_texts()).lower()
    return all(n.lower() in haystack for n in needles)


async def drain_tasks():
    for task in list(main.RUNNING_TASKS):
        if not task.done():
            task.cancel()
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def reset(bot, user_client):
    main.BATCH_STATES.clear()
    main.PIN_PROMPTS.clear()
    main.DESTINATION_CHAT_ID = None
    del OUTGOING[:]
    CHAT_MESSAGES.clear()
    bot.pins = []
    bot.unpins = []
    bot.pin_error = None
    bot.copied = []
    user_client.copied = []
    user_client.created = []
    user_client.gate = None
    user_client.fetch_error = None


async def start_prompt(bot, user_client, count=2, start_id=100):
    """Run /batch + link + count; return the still-running handler task."""
    await main.batch_command_start(bot, IncomingMessage("/batch"))
    await main.handle_text_and_states(
        bot, IncomingMessage("https://t.me/testchan/%d" % start_id)
    )
    runner = asyncio.create_task(
        main.handle_text_and_states(bot, IncomingMessage(str(count)))
    )
    # let the handler create the prompt and start waiting
    for _ in range(50):
        await asyncio.sleep(0.01)
        if main.PIN_PROMPTS.get(USER_ID):
            break
    return runner


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
async def test_prompt_appears_and_blocks_batch():
    """The prompt shows up before any upload happens."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        prompt = main.PIN_PROMPTS.get(USER_ID)
        assert prompt, "no pin prompt state was created for the user"
        markup = prompt["prompt_msg"].reply_markup
        datas = [b.callback_data for row in markup.inline_keyboard for b in row]
        assert any("pin_decision:yes" in d for d in datas), datas
        assert any("pin_decision:no" in d for d in datas), datas
        assert user_client.copied == [], "batch started before the user decided"

        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:no:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins == [], "pinned although the user said no"
    finally:
        await drain_tasks()


async def test_yes_pins_first_post_once():
    """Choosing Yes pins the first uploaded post of the batch, exactly once."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client, count=4, start_id=100)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins == [(USER_ID, 9100)], bot.pins
        assert user_client.copied, "nothing was uploaded"
    finally:
        await drain_tasks()


async def test_pin_targets_destination_channel():
    """When a destination channel is set, the pin must target that channel."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    main.DESTINATION_CHAT_ID = CHANNEL_ID
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins == [(CHANNEL_ID, 9100)], bot.pins
    finally:
        await drain_tasks()


async def test_prompt_window_is_not_too_short():
    """A 10 second window is unworkable; the user needs a real chance to answer."""
    window = getattr(main, "PIN_PROMPT_TIMEOUT", 10)
    assert window >= 30, "pin prompt window is only %ss" % window


async def test_timeout_starts_batch_without_pinning():
    """No answer -> the batch still runs, and the user is told."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    original = getattr(main, "PIN_PROMPT_TIMEOUT", None)
    main.PIN_PROMPT_TIMEOUT = 0.3
    try:
        runner = await start_prompt(bot, user_client)
        await asyncio.wait_for(runner, timeout=8)
        assert bot.pins == [], "pinned even though nobody answered"
        assert user_client.copied, "batch did not run after the timeout"
        assert bot_said("without pinning"), outgoing_texts()
    finally:
        if original is not None:
            main.PIN_PROMPT_TIMEOUT = original
        await drain_tasks()


async def test_typing_yes_is_accepted():
    """Users often reply with text instead of tapping the button."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.handle_text_and_states(bot, IncomingMessage("yes"))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins == [(USER_ID, 9100)], bot.pins
    finally:
        await drain_tasks()


async def test_late_yes_still_pins():
    """A click that arrives after the auto-continue must still enable pinning."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    original = getattr(main, "PIN_PROMPT_TIMEOUT", None)
    main.PIN_PROMPT_TIMEOUT = 0.3
    gate = asyncio.Event()
    try:
        runner = await start_prompt(bot, user_client, count=2)
        user_client.gate = gate          # hold the batch mid-upload
        # wait until the prompt auto-continued and the batch is running
        for _ in range(200):
            await asyncio.sleep(0.01)
            if bot_said("starting batch process"):
                break
        assert main.PIN_PROMPTS.get(USER_ID), "prompt vanished before the batch ended"
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        gate.set()
        user_client.gate = None
        await asyncio.wait_for(runner, timeout=8)
        assert bot.pins == [(USER_ID, 9100)], (
            "late Yes was ignored - pin prompt answered after the timeout"
        )
    finally:
        gate.set()
        user_client.gate = None
        if original is not None:
            main.PIN_PROMPT_TIMEOUT = original
        await drain_tasks()


async def test_killall_does_not_hang_pending_batch():
    """/killall while the prompt is open must not leave the batch stuck forever."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.cancel_all_tasks(bot, IncomingMessage("/killall"))
        await asyncio.wait_for(runner, timeout=3)
    finally:
        await drain_tasks()


async def test_new_batch_cancels_pending_prompt():
    """Starting /batch again must release the previous prompt instead of hanging."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    first = await start_prompt(bot, user_client)
    try:
        await main.batch_command_start(bot, IncomingMessage("/batch"))
        await asyncio.wait_for(first, timeout=3)
    finally:
        await drain_tasks()


async def test_pin_failure_is_reported_to_user():
    """If Telegram refuses the pin, the user must be told instead of silence."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    bot.pin_error = Exception("CHAT_ADMIN_REQUIRED")
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot_said("admin"), outgoing_texts()
    finally:
        await drain_tasks()


async def test_prompt_closed_after_batch():
    """Once the batch is over the prompt must not accept further decisions."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:no:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert main.PIN_PROMPTS.get(USER_ID) is None, "prompt state leaked after the batch"

        query = CallbackQuery("pin_decision:yes:%d" % USER_ID)
        await main.pin_decision_callback(bot, query)
        assert query.answers and query.answers[0][0], "stale click answered with nothing"
        assert len(bot.pins) == 0
    finally:
        await drain_tasks()


async def test_no_channel_delivers_to_bot_chat():
    """With no /set channel, content must land in the bot chat, never Saved Messages."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:no:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert user_client.copied, "nothing was copied"
        for target, _from_chat, _mid in user_client.copied:
            assert target != USER_ID, (
                "user client uploaded to its own id (= Saved Messages) instead of the bot chat"
            )
            assert target in ("test_bot", bot.me.id), target
    finally:
        await drain_tasks()


async def test_pin_targets_bot_chat_message():
    """The pin addresses the chat the first post was delivered to, using its id there."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins == [(USER_ID, 9100)], bot.pins
        assert user_client.created, "nothing was delivered"
        assert bot.pins[0][1] == user_client.created[0], (
            "pinned id %s is not the id the bot chat received (%s)"
            % (bot.pins[0][1], user_client.created[0])
        )
    finally:
        await drain_tasks()


async def test_error_messages_are_deleted_after_ttl():
    """Error replies are shown first, then removed automatically."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    original = main.PyroConf.ERROR_MESSAGE_TTL
    main.PyroConf.ERROR_MESSAGE_TTL = 0.2
    try:
        await main.batch_command_start(bot, IncomingMessage("/batch"))
        await main.handle_text_and_states(bot, IncomingMessage("not-a-link"))
        err = OUTGOING[-1]
        assert "invalid link" in (err.text or "").lower(), err.text
        assert not err.deleted, "error vanished before the user could read it"
        await asyncio.sleep(0.6)
        assert err.deleted, "error message was not deleted after ERROR_MESSAGE_TTL"
    finally:
        main.PyroConf.ERROR_MESSAGE_TTL = original
        await drain_tasks()


async def test_batch_read_failure_is_reported_then_cleaned():
    """A chunk that cannot be read is reported, and that report self-destructs."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    original = main.PyroConf.ERROR_MESSAGE_TTL
    main.PyroConf.ERROR_MESSAGE_TTL = 0.2
    user_client.fetch_error = Exception("CHANNEL_INVALID")
    try:
        runner = await start_prompt(bot, user_client)
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:no:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        errors = [m for m in OUTGOING if "could not read" in (m.text or "").lower()]
        assert errors, "the read failure was not reported to the user"
        await asyncio.sleep(0.6)
        assert any(m.deleted for m in errors), "error report was not cleaned up"
    finally:
        main.PyroConf.ERROR_MESSAGE_TTL = original
        user_client.fetch_error = None
        await drain_tasks()


async def test_pin_failure_reported_only_once():
    """A failed pin is reported once, not again at every chunk boundary."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    bot.pin_error = Exception("MESSAGE_ID_INVALID")
    runner = await start_prompt(bot, user_client, count=4, start_id=100)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        warnings = [m for m in OUTGOING if "could not pin" in (m.text or "").lower()]
        assert len(warnings) == 1, "pin failure reported %d time(s)" % len(warnings)
        assert bot.pins == [], "a failed pin must not look like success"
    finally:
        await drain_tasks()


async def test_stale_self_destination_never_pins_into_the_void():
    """A destination equal to the user's own id is Saved Messages - must not be used.

    This is the live failure: the user client hands its copy to Saved Messages,
    so the first delivered id does not exist in the chat being pinned and
    Telegram answers 400 MESSAGE_ID_INVALID.
    """
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    main.DESTINATION_CHAT_ID = USER_ID          # stale /set <own user id>
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)

        assert "saved" not in CHAT_MESSAGES, (
            "a copy was delivered to Saved Messages: %s" % list(CHAT_MESSAGES)
        )
        assert bot.pins, "the first post was not pinned"
        assert not bot_said("could not pin"), outgoing_texts()
        assert not bot_said("MESSAGE_ID_INVALID"), outgoing_texts()
    finally:
        main.DESTINATION_CHAT_ID = None
        await drain_tasks()


async def test_pinned_id_exists_in_pin_chat():
    """The pinned id must be a message that really lives in the pinned chat."""
    bot, user_client = main.bot, main.user
    reset(bot, user_client)
    runner = await start_prompt(bot, user_client)
    try:
        await main.pin_decision_callback(bot, CallbackQuery("pin_decision:yes:%d" % USER_ID))
        await asyncio.wait_for(runner, timeout=5)
        assert bot.pins, "nothing was pinned"
        pin_chat, pin_id = bot.pins[0]
        key = logical_chat("bot", pin_chat)
        assert pin_id in CHAT_MESSAGES.get(key, {}), (
            "pinned %s but that id is not in %s (has %s)"
            % (pin_id, key, sorted(CHAT_MESSAGES.get(key, {})))
        )
    finally:
        await drain_tasks()


TESTS = [
    test_prompt_appears_and_blocks_batch,
    test_yes_pins_first_post_once,
    test_pin_targets_destination_channel,
    test_prompt_window_is_not_too_short,
    test_timeout_starts_batch_without_pinning,
    test_typing_yes_is_accepted,
    test_late_yes_still_pins,
    test_killall_does_not_hang_pending_batch,
    test_new_batch_cancels_pending_prompt,
    test_pin_failure_is_reported_to_user,
    test_pin_failure_reported_only_once,
    test_stale_self_destination_never_pins_into_the_void,
    test_pinned_id_exists_in_pin_chat,
    test_prompt_closed_after_batch,
    test_no_channel_delivers_to_bot_chat,
    test_pin_targets_bot_chat_message,
    test_error_messages_are_deleted_after_ttl,
    test_batch_read_failure_is_reported_then_cleaned,
]


async def main_runner():
    await main.initialize()          # normally done in __main__ before polling
    passed, failed = 0, 0
    for test in TESTS:
        name = test.__name__
        try:
            await test()
        except Exception as exc:
            failed += 1
            print("FAIL  %s\n      %s: %s" % (name, type(exc).__name__, exc))
            if os.getenv("PIN_TEST_TRACE"):
                traceback.print_exc()
        else:
            passed += 1
            print("PASS  %s" % name)
    print("\n%d passed, %d failed of %d" % (passed, failed, len(TESTS)))
    return 1 if failed else 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    code = loop.run_until_complete(main_runner())
    loop.close()
    sys.exit(code)
