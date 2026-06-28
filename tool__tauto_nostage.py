"""
tool__tauto_nostage.py  v23
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Luồng hoạt động:
  1. Forward bài vào Saved Messages
  2. /done* / /xdone / /zdone  → tool xếp sequence trong memory
  3. Gõ tên kênh / tap lệnh   → forward thẳng ra kênh đích
  4. Auto reset, sẵn sàng batch tiếp

Tính năng nền:
  • Auto sync folder mỗi 1h
  • Auto remove kênh chết mỗi 6h + on-the-fly
  • Anti-flood: retry 6 lần, global flood gate đúng
  • Im lặng: không spam confirm

Fixes v21 (so với v20):
  [FLOOD-TOPIC] resolve_forward_topic: retry FloodWait — flood vài giây không còn làm mất map
  [FLOOD-TOPIC] ensure_topic_detected: chờ + retry detect trước khi xếp/auto-forward
  [FLOOD-TOPIC] update_menu: gọi ensure_topic_detected thay vì timeout 15s rồi bỏ topic
  [FLOOD-TOPIC] _detect: topic_checked chỉ set khi xong; retry khi flood

Fixes v22 (so với v21):
  [DEDUP]    _topic_detect_started: chỉ 1 task _detect / batch (tránh 30+ API calls)
  [DEDUP]    update_menu debounce (_menu_gen): chỉ 1 menu task chạy khi batch ngừng
  [DEDUP]    ensure_topic_detected: chờ _detect, fallback 1 lần qua lock — không spam API
  [SPEED]    _start_forward: load_ads nền, báo "chạy nền" ngay không chờ

Fixes v23 (so với v22):
  [MAP-FIX]  topic cache key (src_id, top_id) — tránh map nhầm kênh cùng topic id
  [MAP-FIX]  không cache title giả khi API fail lúc flood — chờ retry thay vì map sai
  [MAP-FIX]  update_menu bind đúng slot — không lấy nhầm slot mới khi đang chờ
  [MAP-FIX]  verify_slot_topic trước khi auto-map — xác nhận lại từ bài đầu batch
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import copy
import os
import json
import random
import re as _re_cmd
import time
import traceback
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait,
    ChannelInvalid,
    ChannelPrivate,
    ChatWriteForbidden,
    PeerIdInvalid,
    UserBannedInChannel,
    ChatAdminRequired,
)

load_dotenv()

API_ID            = int(os.getenv("API_ID"))
API_HASH          = os.getenv("API_HASH")
INTERMEDIATE_CHAT = int(os.getenv("INTERMEDIATE_CHAT"))
ADS_CHAT          = int(os.getenv("ADS_CHAT"))
SAVED_MESSAGES    = "me"
CHANNELS_FILE     = "channels.json"
FOLDERS_FILE      = "folders.json"
FAILED_FILE       = "failed_msgs.json"
RR_FILE           = "topic_rr.json"

FOLDER_SYNC_INTERVAL_SEC = 3600
DEAD_CHECK_INTERVAL_SEC  = 6 * 3600

# [FIX-5] concurrent 2, delay 1.5s
FWD_BASE_DELAY_SEC          = 1.5
FWD_JITTER_SEC              = 0.5
FWD_MAX_RETRY               = 6
FWD_BETWEEN_CHANNELS_SEC    = 2.0
FWD_MAX_CONCURRENT_CHANNELS = 2

# ── Batch forwarding (từ asmtoki) ──────────────────────────
FWD_BATCH_SIZE      = 50
FWD_BATCH_MIN_DELAY = 1.0
FWD_GLOBAL_RATE     = 10.0
FWD_GLOBAL_BURST    = 15

TOPIC_DETECT_MAX_WAIT_SEC = 45

DEAD_CHANNEL_ERRORS = (
    ChannelInvalid,
    ChannelPrivate,
    PeerIdInvalid,
    UserBannedInChannel,
)

SKIP_NOT_DEAD_ERRORS = (
    ChatWriteForbidden,
    ChatAdminRequired,
)


class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate     = rate
        self.capacity = capacity
        self.tokens   = capacity
        self.last     = time.monotonic()
        self.lock     = asyncio.Lock()

    async def acquire(self, n: float = 1.0):
        async with self.lock:
            now     = time.monotonic()
            elapsed = now - self.last
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last   = now
            if self.tokens >= n:
                self.tokens -= n
                return
            need = n - self.tokens
            wait = need / self.rate
            self.tokens = 0
            self.last   = now + wait
        await asyncio.sleep(wait)


_global_bucket: "TokenBucket | None" = None

app = Client("test_session", api_id=API_ID, api_hash=API_HASH)

fwd_lock             = asyncio.Lock()
_channels_write_lock = asyncio.Lock()

_flood_gate  = asyncio.Lock()
_flood_until = 0.0
_topic_resolve_lock = asyncio.Lock()


def log(tag, msg):
    print(f"[{tag}] {msg}")


async def flood_wait_globally(seconds: float, source: str = ""):
    global _flood_until
    async with _flood_gate:
        now       = time.monotonic()
        remaining = _flood_until - now
        if remaining > 0:
            log("FLOOD", f"[{source}] gate đang chờ {remaining:.0f}s — xếp hàng")
            await asyncio.sleep(remaining)
            return
        _flood_until = time.monotonic() + seconds
        log("FLOOD", f"[{source}] FloodWait toàn cục {seconds:.0f}s")
        await asyncio.sleep(seconds)
        _flood_until = 0.0


def make_slot():
    return {
        "content_msgs":      [],
        "seen_media_groups": set(),
        "ads_msgs":          [],
        "ads_chat_id":       None,
        "ads_index":         0,
        "final_sequence":    [],
        "menu_msg_id":       None,
        "waiting":           True,
        "awaiting_channel":  False,
        "channel_commands":  {},
        "topic_id":          None,
        "topic_title":       None,
        "topic_src_id":      None,
        "topic_checked":     False,
        "all_mode":          False,
        "_album_pending":         0,
        "total_media_count":      0,
        "_topic_detect_started":  False,
        "_menu_gen":              0,
    }

state = {
    "my_id":    None,
    "slots":    [make_slot()],
    "checking": False,
}

def active_slot():
    if not state["slots"]:
        log("WARN", "active_slot() called on empty slots — tạo mới")
        state["slots"].append(make_slot())
    return state["slots"][-1]

def waiting_slot():
    for s in state["slots"]:
        if s["awaiting_channel"]:
            return s
    return None

def reset_slot(slot):
    slot["content_msgs"].clear()
    slot["seen_media_groups"].clear()
    slot["ads_index"]        = 0
    slot["ads_msgs"]         = []
    slot["final_sequence"]   = []
    slot["menu_msg_id"]      = None
    slot["waiting"]          = True
    slot["awaiting_channel"] = False
    slot["channel_commands"] = {}
    slot["topic_id"]         = None
    slot["topic_title"]      = None
    slot["topic_src_id"]     = None
    slot["topic_checked"]    = False
    slot["all_mode"]         = False
    slot["_album_pending"]        = 0
    slot["total_media_count"]     = 0
    slot["_topic_detect_started"] = False
    slot["_menu_gen"]             = 0
    slot.pop("_topic_event", None)

def reset_state():
    reset_slot(active_slot())
    state["slots"] = [s for s in state["slots"] if s["waiting"] or s["awaiting_channel"]]
    if not state["slots"]:
        state["slots"].append(make_slot())
    log("RESET", "State đã reset")


IM_SEND_MAX_RETRY = 6
IM_SEND_MAX_WAIT  = 300
IM_SEND_SPACING   = 0.4

im_send_lock     = asyncio.Lock()
_last_im_send_ts = 0.0

async def _im_spacing():
    global _last_im_send_ts
    now = time.monotonic()
    gap = now - _last_im_send_ts
    if gap < IM_SEND_SPACING:
        await asyncio.sleep(IM_SEND_SPACING - gap)
    _last_im_send_ts = time.monotonic()

async def robust_send(text, chat_id=None,
                      max_retries=IM_SEND_MAX_RETRY,
                      max_wait=IM_SEND_MAX_WAIT):
    target   = INTERMEDIATE_CHAT if chat_id is None else chat_id
    use_lock = (target == INTERMEDIATE_CHAT)

    async def _do():
        for attempt in range(max_retries):
            try:
                if use_lock:
                    await _im_spacing()
                return await app.send_message(target, text)
            except FloodWait as e:
                wait = min(e.value + 1, max_wait)
                log("FLOOD", f"robust_send FloodWait {wait}s — retry {attempt+1}/{max_retries}")
                await asyncio.sleep(wait)
            except Exception as e:
                log("WARN", f"robust_send fail: {type(e).__name__}: {e}")
                return None
        log("ERROR", f"robust_send bỏ cuộc — MẤT MSG: {text[:60]!r}")
        return None

    if use_lock:
        async with im_send_lock:
            return await _do()
    return await _do()

async def robust_edit(chat_id, msg_id, text,
                      max_retries=IM_SEND_MAX_RETRY,
                      max_wait=IM_SEND_MAX_WAIT):
    use_lock = (chat_id == INTERMEDIATE_CHAT)

    async def _do():
        for attempt in range(max_retries):
            try:
                if use_lock:
                    await _im_spacing()
                await app.edit_message_text(chat_id, msg_id, text)
                return True
            except FloodWait as e:
                wait = min(e.value + 1, max_wait)
                log("FLOOD", f"robust_edit FloodWait {wait}s — retry {attempt+1}/{max_retries}")
                await asyncio.sleep(wait)
            except Exception as e:
                log("WARN", f"robust_edit fail: {type(e).__name__}: {e}")
                return False
        return False

    if use_lock:
        async with im_send_lock:
            return await _do()
    return await _do()

async def safe_send(text):
    await robust_send(text)


_channels_cache = None

def load_channels():
    global _channels_cache
    if _channels_cache is not None:
        return list(_channels_cache)
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            _channels_cache = json.load(f)
            return list(_channels_cache)
    _channels_cache = []
    return []

def save_channels(channels):
    global _channels_cache
    _channels_cache = list(channels)
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)

async def remove_dead_channel(chat_id):
    async with _channels_write_lock:
        channels = load_channels()
        new_list = [ch for ch in channels if str(ch.get("id")) != str(chat_id)]
        if len(new_list) == len(channels):
            return None
        removed = next((ch for ch in channels if str(ch.get("id")) == str(chat_id)), None)
        save_channels(new_list)
        return (removed or {}).get("title", str(chat_id))

def get_match_key(title: str) -> str:
    parts = title.strip().split(None, 1)
    if len(parts) >= 2:
        return parts[1].strip().lower()
    return (parts[0] if parts else "").lower()

def find_channels(query: str):
    q = query.lower().strip()
    if not q:
        return []
    result = []
    for ch in load_channels():
        alias     = ch.get("alias", "").lower().strip()
        title     = ch.get("title", "") or ""
        match_key = get_match_key(title)
        if alias:
            if q in alias:
                result.append(ch)
        else:
            if match_key and q in match_key:
                result.append(ch)
    return result


_topic_title_cache = {}


def _is_valid_topic_title(title) -> bool:
    if not title:
        return False
    t = str(title).strip()
    if not t or t == "General":
        return False
    if t.startswith("topic "):
        return False
    return True


def _set_slot_topic(slot, src_id, top_id, title, source=""):
    slot["topic_src_id"]  = src_id
    slot["topic_id"]      = top_id
    slot["topic_title"]   = title
    log("TOPIC", f"{source} topic='{title}' id={top_id} src={src_id}")


async def resolve_forward_topic(client, saved_msg_id):
    from pyrogram.raw import functions as fn, types as tt
    from pyrogram import utils as ut
    for attempt in range(FWD_MAX_RETRY):
        try:
            while True:
                remain = _flood_until - time.monotonic()
                if remain <= 0:
                    break
                log("FLOOD", f"resolve_forward_topic chờ gate {remain:.0f}s")
                await asyncio.sleep(remain)

            raw  = await client.invoke(fn.messages.GetMessages(id=[tt.InputMessageID(id=saved_msg_id)]))
            rm   = raw.messages[0]
            fwd  = getattr(rm, "fwd_from", None)
            sp   = getattr(fwd, "saved_from_peer", None)
            smid = getattr(fwd, "saved_from_msg_id", None)
            if not sp or not smid:
                return (None, None, None)
            src_id = ut.get_peer_id(sp)
            ipc    = await client.resolve_peer(src_id)
            if not hasattr(ipc, "channel_id"):
                return (src_id, None, None)
            inch = tt.InputChannel(channel_id=ipc.channel_id, access_hash=ipc.access_hash)
            og   = await client.invoke(fn.channels.GetMessages(channel=inch, id=[tt.InputMessageID(id=smid)]))
            omsg = og.messages[0]
            rt   = getattr(omsg, "reply_to", None)
            if rt is not None and getattr(rt, "forum_topic", False):
                top_id = (getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None))
            else:
                top_id = 1

            cache_key = (src_id, top_id)
            title = _topic_title_cache.get(cache_key)
            if title is None:
                try:
                    ft    = await client.invoke(fn.channels.GetForumTopicsByID(channel=inch, topics=[top_id]))
                    title = (ft.topics[0].title if getattr(ft, "topics", None) else None)
                except FloodWait:
                    raise
                except Exception as exc:
                    log("WARN", f"GetForumTopicsByID fail src={src_id} topic={top_id}: {exc}")
                    title = None
                if _is_valid_topic_title(title):
                    _topic_title_cache[cache_key] = title
                else:
                    title = None

            if not _is_valid_topic_title(title):
                return (src_id, top_id, None)
            return (src_id, top_id, title)

        except FloodWait as e:
            wait = e.value + 2
            log("FLOOD", f"resolve_forward_topic FloodWait {wait}s — retry {attempt+1}/{FWD_MAX_RETRY}")
            await flood_wait_globally(wait, source="topic_resolve")
            continue
        except Exception as e:
            if attempt < FWD_MAX_RETRY - 1:
                backoff = 2 * (attempt + 1)
                log("WARN", f"resolve_forward_topic attempt {attempt+1}: {type(e).__name__}: {e} — retry {backoff}s")
                await asyncio.sleep(backoff)
                continue
            log("WARN", f"resolve_forward_topic: {type(e).__name__}: {e}")
            return (None, None, None)
    return (None, None, None)


async def ensure_topic_detected(slot, client=None, max_wait=TOPIC_DETECT_MAX_WAIT_SEC):
    """Chờ _detect duy nhất của batch. Không gọi API song song."""
    if _is_valid_topic_title(slot.get("topic_title")) and slot.get("topic_src_id"):
        return True
    client = client or app

    ev = slot.get("_topic_event")
    if ev is not None and not ev.is_set():
        try:
            await asyncio.wait_for(ev.wait(), timeout=max_wait)
        except asyncio.TimeoutError:
            log("TOPIC", f"ensure_topic_detected: chờ _detect timeout {max_wait}s")

    if _is_valid_topic_title(slot.get("topic_title")) and slot.get("topic_src_id"):
        return True

    return await verify_slot_topic(slot, client, source="ensure_topic_detected")


async def verify_slot_topic(slot, client=None, source="verify"):
    """Xác nhận topic từ bài đầu batch — tránh map nhầm do cache/flood."""
    if not slot.get("content_msgs"):
        return False
    client = client or app
    msg_id = slot["content_msgs"][0]

    async with _topic_resolve_lock:
        src_id, top_id, top_title = await resolve_forward_topic(client, msg_id)
        if not _is_valid_topic_title(top_title) or top_id is None:
            log("TOPIC", f"{source}: chưa có topic hợp lệ cho msg={msg_id}")
            return False

        cur_title = slot.get("topic_title")
        cur_src   = slot.get("topic_src_id")
        if cur_title == top_title and cur_src == src_id:
            return True

        if cur_title and cur_title != top_title:
            log("TOPIC", f"{source}: sửa topic {cur_title!r} → {top_title!r} (src={src_id})")

        _set_slot_topic(slot, src_id, top_id, top_title, source=source)
        ev = slot.get("_topic_event")
        if ev is not None and not ev.is_set():
            ev.set()
        return True


TOPIC_MAP_TXT = "topic_map.txt"

TOPIC_MAP_TEMPLATE = (
    "# ===== MAP TOPIC -> KÊNH (sửa tay file này) =====\n"
    "# Mỗi dòng 1 mapping:   tên_topic = tên_kênh\n"
    "# Dòng # là ghi chú. Sửa xong lưu là dùng được ngay.\n"
    "#\n"
    "# Ví dụ:\n"
    "# vitamin = pro\n"
    "# real    = real\n"
    "#\n"
    "# ----- Cấu hình xếp bài -----\n"
    "# @xepbai = on\n"
    "# @xepbaiwhite =\n"
)

def ensure_topic_map_txt():
    if not os.path.exists(TOPIC_MAP_TXT):
        try:
            with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
                f.write(TOPIC_MAP_TEMPLATE)
            log("MAP", f"Tạo file mẫu {TOPIC_MAP_TXT}")
        except Exception as e:
            log("WARN", f"ensure_topic_map_txt: {e}")

def _read_topic_lines():
    out = []
    if not os.path.exists(TOPIC_MAP_TXT):
        return out
    try:
        with open(TOPIC_MAP_TXT, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if "#" in s:
                    s = s.split("#", 1)[0].strip()
                if not s:
                    continue
                sep = "=" if "=" in s else (":" if ":" in s else None)
                if not sep:
                    continue
                left, _, right = s.partition(sep)
                out.append((left.strip(), right.strip()))
    except Exception as e:
        log("WARN", f"_read_topic_lines: {e}")
    return out

def load_topic_txt():
    return [(l, r.lstrip("/")) for (l, r) in _read_topic_lines()
            if l and r and not l.startswith("@")]

def get_xepbai_mode():
    val = "on"
    for l, r in _read_topic_lines():
        if l.lower() == "@xepbai":
            val = "off" if r.strip().lower() == "off" else "on"
    return val

def get_xepbai_whitelist():
    wl   = set()
    seen = False
    for l, r in _read_topic_lines():
        if l.lower() == "@xepbaiwhite":
            wl   = {x.strip().lower().lstrip("/") for x in r.replace(" ", ",").split(",") if x.strip()}
            seen = True
    return wl if seen else set()

def set_topic_directive(key, value):
    ensure_topic_map_txt()
    try:
        with open(TOPIC_MAP_TXT, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception:
        lines = []
    kept = []
    for ln in lines:
        s  = ln.strip()
        cs = s.split("#", 1)[0].strip() if "#" in s else s
        if "=" in cs and cs.partition("=")[0].strip().lower() == key.lower():
            continue
        kept.append(ln)
    kept.append(f"{key} = {value}")
    with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(kept) + "\n")

def find_cmds_for_topic_title(title):
    if not title:
        return []
    t   = title.strip().lower()
    out = []
    for topic, cmd in load_topic_txt():
        if topic.strip().lower() == t:
            if cmd.lower() not in [c.lower() for c in out]:
                out.append(cmd)
    return out

def resolve_channels_by_cmd(cmd_key):
    cmd_key = (cmd_key or "").strip().lower().lstrip("/")
    if not cmd_key:
        return []
    groups = {}
    for ch in load_channels():
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        k     = get_cmd_key(title, alias)
        if k:
            groups.setdefault(k, []).append(ch)
    return groups.get(cmd_key, [])

def all_channel_cmds():
    groups = {}
    for ch in load_channels():
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        k     = get_cmd_key(title, alias)
        if k:
            groups.setdefault(k, []).append(ch)
    return sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))

def gen_topic_map_txt():
    existing    = load_topic_txt()
    mapped_cmds = {c.lower() for _, c in existing}
    groups      = all_channel_cmds()
    lines       = [TOPIC_MAP_TEMPLATE.rstrip("\n"), ""]
    if existing:
        lines.append("# ===== ĐÃ MAP =====")
        for topic, cmd in existing:
            lines.append(f"{topic} = {cmd}")
        lines.append("")
    lines.append("# ===== ĐIỀN TÊN TOPIC VÀO TRƯỚC DẤU = =====")
    n_new = 0
    for cmd, chs in groups:
        if cmd.lower() in mapped_cmds:
            continue
        titles = ", ".join((c.get("title") or "") for c in chs)
        lines.append(f" = {cmd}    # {titles}")
        n_new += 1
    with open(TOPIC_MAP_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return len(existing), n_new


def load_topic_rr() -> dict:
    if os.path.exists(RR_FILE):
        try:
            with open(RR_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_topic_rr(data: dict):
    with open(RR_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def pick_next_rr(topic_title: str, cmds: list) -> str:
    if not cmds:
        return ""
    if len(cmds) == 1:
        return cmds[0]
    rr   = load_topic_rr()
    key  = (topic_title or "").strip().lower()
    prev = rr.get(key, -1)
    idx  = (prev + 1) % len(cmds)
    rr[key] = idx
    save_topic_rr(rr)
    return cmds[idx]


RESERVED_CMDS = {
    "add", "addf", "addchan", "addfolder",
    "list", "listchan",
    "del", "delchan",
    "alias", "aliaschan",
    "check", "checkchan",
    "clean", "cleanchan",
    "skip", "next", "help",
    "xdone", "zdone",
    "map", "unmap", "mapgen",
    "xepbai", "xepbaiwhite",
    "all",
    "done", "done1", "done2", "done3", "done4", "done5",
    "done6", "done7", "done8", "done9", "done10",
}

_CMD_OK = _re_cmd.compile(r"^[a-z0-9_]+$")

def _is_junk_word(w: str) -> bool:
    return not w or w.isdigit() or not any(c.isalnum() for c in w)

def _strip_junk(w: str) -> str:
    i, j = 0, len(w)
    while i < j and not w[i].isalnum():
        i += 1
    while j > i and not w[j-1].isalnum():
        j -= 1
    return w[i:j]

def get_cmd_key(title: str, alias: str = "") -> str:
    if alias and alias.strip():
        return _strip_junk(alias.strip()).lower()
    parts = (title or "").strip().split()
    if len(parts) <= 1:
        return _strip_junk(parts[0]).lower() if parts else ""
    rest = parts[1:]
    while rest and _is_junk_word(rest[-1]):
        rest.pop()
    if not rest:
        return ""
    return _strip_junk(rest[-1]).lower()

def build_channel_commands(channels):
    if not channels:
        return "  (Chưa có kênh — dùng /add <link/id> để thêm)", {}
    groups = {}
    for ch in channels:
        alias = (ch.get("alias") or "").strip().lower()
        title = (ch.get("title") or "").strip()
        key   = get_cmd_key(title, alias)
        if not key:
            continue
        groups.setdefault(key, []).append(ch)
    sorted_keys = sorted(groups.keys(), key=lambda k: (-len(groups[k]), k))
    lines   = []
    cmd_map = {}
    for key in sorted_keys:
        chs       = groups[key]
        titles    = ", ".join((ch.get("title") or "").strip() for ch in chs)
        clickable = bool(_CMD_OK.match(key)) and key not in RESERVED_CMDS
        if clickable:
            cmd_map[key] = chs
            lines.append(f"/{key}   ({titles})")
        else:
            lines.append(f"• {key}   ({titles})")
    return "\n\n".join(lines), cmd_map


def load_folders():
    if os.path.exists(FOLDERS_FILE):
        try:
            with open(FOLDERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_folders(folders):
    with open(FOLDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(folders, f, ensure_ascii=False, indent=2)

def remember_folder(slug, title=""):
    folders = load_folders()
    for fd in folders:
        if fd.get("slug") == slug:
            if title and not fd.get("title"):
                fd["title"] = title
                save_folders(folders)
            return False
    folders.append({"slug": slug, "title": title or slug, "added_at": int(time.time())})
    save_folders(folders)
    return True


def load_failed():
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_failed(items):
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

def record_failed(target_id, target_title, items):
    if not items:
        return
    data = load_failed()
    data.append({
        "target_id":    target_id,
        "target_title": target_title,
        "items":        items,
        "ts":           int(time.time()),
    })
    save_failed(data)


async def load_ads_into(slot):
    ads     = []
    chat_id = slot.get("ads_chat_id") or ADS_CHAT
    try:
        async for msg in app.get_chat_history(chat_id, limit=200):
            if not msg.empty and not msg.service:
                ads.append(msg.id)
        ads.reverse()
        slot["ads_msgs"]    = ads
        slot["ads_chat_id"] = chat_id
        log("ADS", f"Load xong {len(ads)} ads")
    except Exception as e:
        log("ERROR", f"load_ads: {e}")

async def load_ads():
    await load_ads_into(active_slot())


def get_menu(n_content):
    slot      = active_slot()
    ads_count = max(1, len(slot["ads_msgs"]))
    best      = max(1, round(n_content / ads_count))
    media_cnt = slot.get("total_media_count", 0)
    media_str = f" / {media_cnt} media" if media_cnt > 0 else ""
    lines     = [f"📥 Đã nhận {n_content} bài{media_str}\n━━━━━━━━━━━━━━━"]
    for i in range(1, 11):
        mark = " ✅" if i == best else ""
        lines.append(f"/done{i} — {i} content/ads{mark}")
    lines += [
        "━━━━━━━━━━━━━━━",
        f"➡️ Gợi ý: /done{best}",
        "━━━━━━━━━━━━━━━",
        "/xdone — nhét hết ads (có thể 2 ads liền)",
        "/zdone — ads trước, content sau",
    ]
    return "\n".join(lines)


def schedule_update_menu(n):
    """Debounce: mỗi bài mới tăng gen — chỉ task gen mới nhất được chạy."""
    slot = active_slot()
    slot["_menu_gen"] = slot.get("_menu_gen", 0) + 1
    gen = slot["_menu_gen"]
    asyncio.ensure_future(update_menu(slot, n, gen))


async def update_menu(slot, n, gen=None):
    await asyncio.sleep(2)
    if gen is not None and gen != slot.get("_menu_gen"):
        return
    if not slot.get("waiting"):
        return

    for _ in range(30):
        if slot.get("_album_pending", 0) <= 0:
            break
        await asyncio.sleep(0.5)

    if len(slot["content_msgs"]) != n or not slot["waiting"]:
        return

    if gen is not None and gen != slot.get("_menu_gen"):
        return

    if slot.get("all_mode"):
        log("ALL", f"/all → up {n} bài lên tất cả kênh")
        await do_all_forward(slot)
        return

    # Xác nhận topic từ bài đầu batch trước khi auto-map
    topic_ok = await ensure_topic_detected(slot, app)
    if not topic_ok:
        log("TOPIC", "Chưa xác nhận được topic — chờ thêm hoặc chọn tay /done")
        text = get_menu(n)
        if slot.get("menu_msg_id"):
            ok = await robust_edit(INTERMEDIATE_CHAT, slot["menu_msg_id"], text)
            if ok:
                return
            slot["menu_msg_id"] = None
        m = await robust_send(text)
        if m is not None:
            slot["menu_msg_id"] = m.id
        return

    if len(slot["content_msgs"]) != n or not slot.get("waiting"):
        return

    mode         = get_xepbai_mode()
    mapped_cmds  = find_cmds_for_topic_title(slot.get("topic_title"))
    whitelist    = get_xepbai_whitelist()
    force_manual = any(c.lower() in whitelist for c in mapped_cmds)
    topic_auto   = bool(mapped_cmds) and not force_manual
    log("XEPBAI", f"mode={mode} topic={slot.get('topic_title')!r} src={slot.get('topic_src_id')} "
                  f"mapped={mapped_cmds} force_manual={force_manual} topic_auto={topic_auto}")

    if (mode == "off" and not force_manual) or topic_auto:
        if not slot["ads_msgs"]:
            await load_ads()
        ads_count = max(1, len(slot["ads_msgs"]))
        best      = max(1, round(n / ads_count))
        reason    = f"topic map ({slot.get('topic_title')!r})" if topic_auto else "xepbai=off"
        log("XEPBAI", f"{reason} → tự xếp /done{best}")
        media_cnt = slot.get("total_media_count", 0)
        media_hint = f"{n} bài / {media_cnt} media" if media_cnt else f"{n} bài"
        asyncio.ensure_future(safe_send(f"⏳ {media_hint} — đang xếp & forward..."))
        await build_sequence(best)
        return

    text = get_menu(n)
    if slot.get("menu_msg_id"):
        ok = await robust_edit(INTERMEDIATE_CHAT, slot["menu_msg_id"], text)
        if ok:
            return
        slot["menu_msg_id"] = None

    if len(slot["content_msgs"]) != n or not slot["waiting"]:
        return

    m = await robust_send(text)
    if m is not None:
        slot["menu_msg_id"] = m.id
    else:
        log("ERROR", f"update_menu: không gửi được menu n={n}")


async def raw_forward(from_peer, to_peer, ids: list):
    from pyrogram.raw import functions
    await app.invoke(
        functions.messages.ForwardMessages(
            from_peer   = from_peer,
            to_peer     = to_peer,
            id          = ids,
            drop_author = True,
            random_id   = [random.randint(0, 2**63) for _ in ids],
            silent      = False,
        )
    )
async def _try_batch_raw(
    from_peer, to_peer, ids: list,
    bucket: TokenBucket, last_ts: list,
):
    if not ids:
        return [], [], None

    while True:
        elapsed = time.monotonic() - last_ts[0]
        gap     = FWD_BATCH_MIN_DELAY + random.uniform(0, 0.3)
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)

        await bucket.acquire(1)
        last_ts[0] = time.monotonic()

        while True:
            remain = _flood_until - time.monotonic()
            if remain <= 0:
                break
            log("FLOOD", f"Chờ flood gate {remain:.0f}s — batch {len(ids)} ids")
            await asyncio.sleep(remain)

        try:
            await raw_forward(from_peer, to_peer, ids)
            return list(ids), [], None

        except FloodWait as e:
            wait = e.value + 3
            log("FLOOD", f"FloodWait {wait}s (batch={len(ids)}) → global wait")
            await flood_wait_globally(wait, source="batch_raw")
            continue

        except DEAD_CHANNEL_ERRORS as e:
            err = f"{type(e).__name__}: {str(e)[:80]}"
            log("DEAD", f"Dead channel trong batch: {err}")
            return [], list(ids), err

        except SKIP_NOT_DEAD_ERRORS as e:
            log("SKIP", f"Skip channel ({type(e).__name__}) trong batch")
            return [], list(ids), f"SKIP:{type(e).__name__}"

        except Exception as e:
            if len(ids) > 1:
                log("WARN", f"batch={len(ids)} lỗi ({type(e).__name__}) → split đôi retry")
                mid = len(ids) // 2
                lok, lf, ld = await _try_batch_raw(from_peer, to_peer, ids[:mid], bucket, last_ts)
                if ld:
                    return lok, lf + ids[mid:], ld
                rok, rf, rd = await _try_batch_raw(from_peer, to_peer, ids[mid:], bucket, last_ts)
                return lok + rok, lf + rf, rd
            log("ERROR", f"id={ids[0]} fail: {type(e).__name__}: {str(e)[:100]}")
            return [], list(ids), None


async def forward_sequence_to_channel(target_id, sequence):
    to_peer    = await app.resolve_peer(target_id)
    peer_cache = {}

    async def get_peer(chat_id):
        if chat_id not in peer_cache:
            peer_cache[chat_id] = await app.resolve_peer(chat_id)
        return peer_cache[chat_id]

    bucket  = TokenBucket(FWD_GLOBAL_RATE, FWD_GLOBAL_BURST)
    last_ts = [0.0]

    expanded     = []
    seen_groups  = set()
    failed_items = []
    dead_reason  = None

    for seq_item in sequence:
        src_chat, msg_id = seq_item
        last_err   = None
        item_done  = False

        for attempt in range(FWD_MAX_RETRY):
            while True:
                remain = _flood_until - time.monotonic()
                if remain <= 0:
                    break
                log("FLOOD", f"Flood gate {remain:.0f}s — expand {src_chat}/{msg_id}")
                await asyncio.sleep(remain)

            try:
                msg = await app.get_messages(src_chat, msg_id)

                if msg.empty:
                    if attempt < FWD_MAX_RETRY - 1:
                        log("WARN", f"msg empty attempt {attempt+1}/{FWD_MAX_RETRY}, retry 3s: "
                                    f"{src_chat}/{msg_id}")
                        await asyncio.sleep(3)
                        last_err = Exception("msg.empty")
                        continue
                    log("WARN", f"msg empty sau {FWD_MAX_RETRY} lần → skip: {src_chat}/{msg_id}")
                    item_done = True
                    break

                if msg.media_group_id:
                    key = (src_chat, msg.media_group_id)
                    if key in seen_groups:
                        item_done = True
                        break
                    seen_groups.add(key)
                    album = await app.get_media_group(src_chat, msg_id)
                    ids   = [m.id for m in album]
                    if ids:
                        expanded.append((src_chat, ids, seq_item))
                    item_done = True
                    break
                else:
                    expanded.append((src_chat, [msg_id], seq_item))
                    item_done = True
                    break

            except FloodWait as e:
                wait = e.value + 3
                log("FLOOD", f"FloodWait {wait}s — get_messages {src_chat}/{msg_id}")
                await flood_wait_globally(wait, source=f"expand ch={target_id}")
                last_err = e

            except DEAD_CHANNEL_ERRORS as dead_exc:
                dead_reason    = f"{type(dead_exc).__name__}: {str(dead_exc)[:80]}"
                log("DEAD", f"Kênh {target_id} chết khi expand: {dead_reason}")
                already_expanded = {ex[2] for ex in expanded}
                remaining = [
                    s for s in sequence
                    if s not in already_expanded and s != seq_item
                ]
                return 0, failed_items + [seq_item] + remaining, dead_reason

            except SKIP_NOT_DEAD_ERRORS as skip_exc:
                log("SKIP", f"Skip kênh {target_id} ({type(skip_exc).__name__})")
                return 0, [], None

            except Exception as e:
                last_err = e
                backoff  = 2 * (attempt + 1) + random.uniform(0, 1.5)
                log("WARN", f"expand {src_chat}/{msg_id} attempt {attempt+1}/{FWD_MAX_RETRY}: "
                            f"{type(e).__name__}: {e} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

        if not item_done:
            failed_items.append(seq_item)
            log("ERROR", f"Không expand {src_chat}/{msg_id} sau {FWD_MAX_RETRY} lần: {last_err}")

    groups: list = []
    for src_chat, ids, orig in expanded:
        if groups and groups[-1][0] == src_chat:
            groups[-1][1].append((ids, orig))
        else:
            groups.append((src_chat, [(ids, orig)]))

    sent_count = 0

    for g_idx, (src_chat, items) in enumerate(groups):
        try:
            from_peer = await get_peer(src_chat)
        except DEAD_CHANNEL_ERRORS as dead_exc:
            dead_reason = f"{type(dead_exc).__name__}: {str(dead_exc)[:80]}"
            leftover = [orig for _, orig in items]
            for _, remaining_items in groups[g_idx + 1:]:
                leftover += [orig for _, orig in remaining_items]
            return sent_count, failed_items + leftover, dead_reason

        all_ids    = [mid for ids, _orig in items for mid in ids]
        id_to_orig = {mid: orig for ids, orig in items for mid in ids}

        for batch_start in range(0, len(all_ids), FWD_BATCH_SIZE):
            batch = all_ids[batch_start : batch_start + FWD_BATCH_SIZE]

            ok_ids, fail_ids, dead_err = await _try_batch_raw(
                from_peer, to_peer, batch, bucket, last_ts
            )

            sent_count += len(ok_ids)
            log("FWD", f"  batch×{len(batch)} (ok={len(ok_ids)} fail={len(fail_ids)}) "
                       f"src={src_chat} → ch={target_id}")

            if dead_err:
                is_dead     = not dead_err.startswith("SKIP:")
                dead_reason = dead_err if is_dead else None
                remaining_ids  = set(fail_ids) | set(all_ids[batch_start + FWD_BATCH_SIZE:])
                leftover_origs: list = []
                seen_origs: set = set()
                for mid in remaining_ids:
                    orig = id_to_orig.get(mid)
                    if orig and id(orig) not in seen_origs:
                        leftover_origs.append(orig)
                        seen_origs.add(id(orig))
                for _, remaining_items in groups[g_idx + 1:]:
                    for _, orig in remaining_items:
                        if id(orig) not in seen_origs:
                            leftover_origs.append(orig)
                            seen_origs.add(id(orig))
                return sent_count, failed_items + leftover_origs, dead_reason

            if fail_ids:
                added_origs: set = set()
                for fid in fail_ids:
                    orig = id_to_orig.get(fid)
                    if orig and id(orig) not in added_origs:
                        failed_items.append(orig)
                        added_origs.add(id(orig))

    if failed_items:
        log("FWD", f"⚠️ ch={target_id}: {len(failed_items)} items fail sau retry")

    return sent_count, failed_items, dead_reason


async def build_sequence(content_per_ads=1, mode="normal"):
    slot         = active_slot()
    contents     = slot["content_msgs"]
    n            = len(contents)
    ads_chat     = slot["ads_chat_id"] or ADS_CHAT
    content_chat = SAVED_MESSAGES
    media_cnt    = slot.get("total_media_count", 0)
    media_str    = f"{n} bài / {media_cnt} media" if media_cnt else f"{n} bài"

    log("BUILD", f"mode={mode} n={n} media={media_cnt} ads={len(slot['ads_msgs'])} cpa={content_per_ads}")

    if not slot["ads_msgs"]:
        log("BUILD", "Không có ads — forward content không xen ads")
        sequence = [(content_chat, mid) for mid in contents]
        slot["final_sequence"]   = sequence
        slot["awaiting_channel"] = True
        slot["waiting"]          = False
        channels            = load_channels()
        chan_lines, cmd_map = build_channel_commands(channels)
        slot["channel_commands"] = cmd_map
        await safe_send(
            f"⚠️ Không có ads — forward {media_str} không xen ads.\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📡 Tap lệnh để forward:\n{chan_lines}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"/skip — bỏ qua"
        )
        return

    ads_queue = list(slot["ads_msgs"][slot["ads_index"]:])
    sequence  = []
    ads_used  = 0

    def take_ads(count):
        nonlocal ads_used
        taken = []
        for _ in range(count):
            if not ads_queue:
                break
            aid = ads_queue.pop(0)
            slot["ads_index"] += 1
            taken.append((ads_chat, aid))
            ads_used += 1
        return taken

    if mode == "normal":
        total_ads = len(ads_queue)
        if total_ads == 0 or n <= 1:
            for mid in contents:
                sequence.append((content_chat, mid))
        else:
            groups      = total_ads + 1
            base, extra = divmod(n, groups)
            sizes       = [base + (1 if g < extra else 0) for g in range(groups)]
            idx         = 0
            for g, size in enumerate(sizes):
                for _ in range(size):
                    sequence.append((content_chat, contents[idx]))
                    idx += 1
                if g < groups - 1:
                    sequence.extend(take_ads(1))

    elif mode == "xdone":
        slots_between = n - 1
        if slots_between <= 0:
            for mid in contents:
                sequence.append((content_chat, mid))
        else:
            total       = len(ads_queue)
            base, extra = divmod(total, slots_between)
            for i, mid in enumerate(contents):
                sequence.append((content_chat, mid))
                if i < n - 1:
                    sequence.extend(take_ads(base + (1 if i < extra else 0)))

    elif mode == "zdone":
        total       = len(ads_queue)
        base, extra = divmod(total, n)
        for i, mid in enumerate(contents):
            sequence.extend(take_ads(base + (1 if i < extra else 0)))
            sequence.append((content_chat, mid))

    log("BUILD", f"Xếp xong len(seq)={len(sequence)} ads_used={ads_used}")

    if not sequence:
        await safe_send("⚠️ Sequence rỗng.")
        return

    slot["final_sequence"]   = sequence
    slot["awaiting_channel"] = True
    slot["waiting"]          = False

    if not await verify_slot_topic(slot, app, source="build_sequence"):
        await safe_send(
            f"⚠️ Chưa xác nhận được topic (có thể đang flood) — batch {media_str}.\n"
            f"Chờ vài giây rồi gõ /done hoặc chọn kênh tay."
        )
        slot["awaiting_channel"] = True
        channels            = load_channels()
        chan_lines, cmd_map = build_channel_commands(channels)
        slot["channel_commands"] = cmd_map
        return

    mapped_cmds = find_cmds_for_topic_title(slot.get("topic_title"))

    if len(mapped_cmds) >= 2:
        picked_cmd = pick_next_rr(slot.get("topic_title", ""), mapped_cmds)
        rr_state   = load_topic_rr()
        rr_key     = (slot.get("topic_title") or "").strip().lower()
        rr_idx     = rr_state.get(rr_key, 0)
        rr_display = f"{rr_idx + 1}/{len(mapped_cmds)}"

        results = resolve_channels_by_cmd(picked_cmd)
        if results:
            names = ", ".join(ch["title"] for ch in results)
            await safe_send(
                f"🔄 Topic '{slot.get('topic_title')}' (lượt {rr_display}) → /{picked_cmd}\n"
                f"✅ Xếp {media_str} + {ads_used} ads → tự forward tới {len(results)} kênh: {names}"
            )
            await _start_forward(slot, results, f"/{picked_cmd}")
            return
        for fallback_cmd in mapped_cmds:
            if fallback_cmd == picked_cmd:
                continue
            results = resolve_channels_by_cmd(fallback_cmd)
            if results:
                names = ", ".join(ch["title"] for ch in results)
                await safe_send(
                    f"⚠️ /{picked_cmd} không có kênh → fallback /{fallback_cmd}\n"
                    f"✅ Xếp {media_str} + {ads_used} ads → tự forward tới {len(results)} kênh: {names}"
                )
                await _start_forward(slot, results, f"/{fallback_cmd}")
                return
        cmd_map = {c: resolve_channels_by_cmd(c) for c in mapped_cmds if resolve_channels_by_cmd(c)}
        slot["channel_commands"] = cmd_map
        opts = "\n\n".join(f"/{c}" for c in mapped_cmds)
        await safe_send(
            f"⚠️ Topic '{slot.get('topic_title')}' — tất cả kênh đều không resolve được.\n"
            f"✅ Đã xếp {media_str} + {ads_used} ads. Vui lòng chọn tay:\n\n{opts}\n\n/skip — bỏ qua"
        )
        return

    if len(mapped_cmds) == 1:
        mapped_cmd = mapped_cmds[0]
        results    = resolve_channels_by_cmd(mapped_cmd)
        if results:
            names = ", ".join(ch["title"] for ch in results)
            await safe_send(
                f"🎯 Topic '{slot.get('topic_title')}' → /{mapped_cmd}\n"
                f"✅ Xếp {media_str} + {ads_used} ads → tự forward tới {len(results)} kênh: {names}"
            )
            await _start_forward(slot, results, f"/{mapped_cmd}")
            return
        await safe_send(
            f"⚠️ Topic '{slot.get('topic_title')}' map tới /{mapped_cmd} "
            f"nhưng không có kênh nào khớp (xem /list). Chọn tay:"
        )

    channels            = load_channels()
    chan_lines, cmd_map = build_channel_commands(channels)
    slot["channel_commands"] = cmd_map

    hint = ""
    if slot.get("topic_title") and not mapped_cmds:
        hint = (f"\n━━━━━━━━━━━━━━━\n"
                f"ℹ️ Topic: '{slot.get('topic_title')}' — chưa map.\n"
                f"Mở {TOPIC_MAP_TXT}, thêm: {slot.get('topic_title')} = <tenkenh>")

    await safe_send(
        f"✅ Xếp xong: {media_str} + {ads_used} ads\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📡 Tap lệnh để forward:\n{chan_lines}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"/skip — bỏ qua"
        f"{hint}"
    )
async def do_all_forward(slot):
    seq = [(SAVED_MESSAGES, mid) for mid in slot["content_msgs"]]
    slot["all_mode"] = False
    if not seq:
        return
    channels = load_channels()
    if not channels:
        await safe_send("⚠️ /all: chưa có kênh nào.")
        reset_slot(slot)
        return
    slot["final_sequence"]   = seq
    slot["awaiting_channel"] = True
    slot["waiting"]          = False
    media_cnt = slot.get("total_media_count", 0)
    media_str = f"{len(seq)} bài / {media_cnt} media" if media_cnt else f"{len(seq)} bài"
    await safe_send(f"📦 /all: up {media_str} → TẤT CẢ {len(channels)} kênh (không ads, ẩn tên).")
    await _start_forward(slot, channels, "/all")


async def do_forward_job(slot, results):
    sequence    = slot["final_sequence"]
    content_n   = len(slot["content_msgs"])
    content_med = slot.get("total_media_count", 0)

    ok_ids   = set()
    err_ids  = set()
    dead_ids = set()

    dead_killed  = []
    fail_recap   = []
    ch_media_ok: dict = {}
    sem = asyncio.Semaphore(FWD_MAX_CONCURRENT_CHANNELS)

    async def _fwd_one(ch):
        ch_id    = ch["id"]
        ch_label = f"{ch['title']} (id={ch_id})"

        async with sem:
            try:
                seq_copy = copy.copy(sequence)
                sent, failed_items, dead_reason = await forward_sequence_to_channel(ch_id, seq_copy)

                if dead_reason:
                    dead_ids.add(ch_id)
                    removed = await remove_dead_channel(ch_id)
                    if removed:
                        dead_killed.append(removed)
                    if failed_items:
                        record_failed(ch_id, ch["title"], failed_items)
                        fail_recap.append((ch["title"], len(failed_items)))
                    log("DEAD", f"💀 {ch_label} — CHẾT, lưu {len(failed_items)} bài | {dead_reason}")
                else:
                    ok_ids.add(ch_id)
                    ch_media_ok[ch_id] = sent
                    log("FWD", f"✓ {ch_label} — OK {sent} media, fail={len(failed_items)}")
                    if failed_items:
                        record_failed(ch_id, ch["title"], failed_items)
                        fail_recap.append((ch["title"], len(failed_items)))

                await asyncio.sleep(FWD_BETWEEN_CHANNELS_SEC + random.uniform(0, 1.0))

            except Exception as e:
                err_ids.add(ch_id)
                log("ERROR", f"forward to {ch_label}: {e}\n{traceback.format_exc()}")

    async with fwd_lock:
        await asyncio.gather(*[_fwd_one(ch) for ch in results])

    total_ok_media = max(ch_media_ok.values()) if ch_media_ok else 0
    media_recv_str = f"{content_n} bài / {content_med} media" if content_med else f"{content_n} bài"

    lines = [
        f"✅ Xong! Forward → {len(results)} kênh",
        f"  📥 Nhận: {media_recv_str}",
        f"  📤 Forward: {total_ok_media} media/kênh",
    ]
    if ok_ids:
        ok_names = [ch["title"] for ch in results if ch["id"] in ok_ids]
        lines.append(f"  ✓ OK ({len(ok_ids)}): {', '.join(ok_names)}")
    if dead_killed:
        lines.append(f"  💀 Kênh chết (đã xóa): {', '.join(dead_killed)}")
    if err_ids:
        err_names = [ch["title"] for ch in results if ch["id"] in err_ids]
        lines.append(f"  ❌ Lỗi: {', '.join(err_names)}")
    if fail_recap:
        detail = ", ".join(f"{t}({n} bài)" for t, n in fail_recap)
        lines.append(f"  ⚠️ Bài lưu retry: {detail}")

    try:
        await safe_send("\n".join(lines))
    finally:
        reset_slot(slot)
        if slot in state["slots"]:
            state["slots"].remove(slot)
        if not state["slots"]:
            new_s = make_slot()
            state["slots"].append(new_s)
            asyncio.ensure_future(load_ads_into(new_s))
        if not any(s["awaiting_channel"] for s in state["slots"]):
            await safe_send("✨ Sẵn sàng! Forward bài mới vào Saved Messages.")


async def _start_forward(slot, results, query_display: str = ""):
    if not results:
        await safe_send(f"❌ Không tìm thấy kênh khớp '{query_display}'.\nDùng /list để xem hoặc gõ lại.")
        return
    if not slot or not slot["final_sequence"]:
        await safe_send("⚠️ Không có batch nào đang chờ forward.")
        return
    names = ", ".join(ch["title"] for ch in results)
    if len(names) > 300:
        names = names[:300] + f"… (+{len(results)} kênh)"
    content_n   = len(slot["content_msgs"])
    content_med = slot.get("total_media_count", 0)
    media_str   = f"{content_n} bài / {content_med} media" if content_med else f"{content_n} bài"
    slot["awaiting_channel"] = False
    await safe_send(
        f"📡 Forward → {len(results)} kênh: {names}\n"
        f"📦 {media_str} — {len(slot['final_sequence'])} seq items\n"
        f"▶️ Chạy nền — bạn có thể forward bài mới ngay!"
    )
    new_s = make_slot()
    state["slots"].append(new_s)
    asyncio.ensure_future(load_ads_into(new_s))
    asyncio.ensure_future(do_forward_job(slot, results))

async def cmd_select_channel(query: str):
    slot    = waiting_slot()
    results = find_channels(query)
    await _start_forward(slot, results, query)

async def cmd_select_by_cmd(slot, cmd_key: str):
    cmd_map = slot.get("channel_commands") or {}
    results = cmd_map.get(cmd_key, [])
    await _start_forward(slot, results, f"/{cmd_key}")


async def _fetch_folder_chats(slug):
    from pyrogram.raw import functions as raw_fn
    result = await app.invoke(raw_fn.chatlists.CheckChatlistInvite(slug=slug))
    return getattr(result, "title", slug) or slug, getattr(result, "chats", [])

async def cmd_addfolder(link: str, silent: bool = False, remember: bool = True):
    import re as _re
    match = _re.search(r"addlist/([A-Za-z0-9_+=-]+)", link.strip())
    if not match:
        if not silent:
            await safe_send("❌ Link folder không hợp lệ.\nĐịnh dạng: https://t.me/addlist/xxxxx")
        return 0
    slug = match.group(1)
    if not silent:
        await safe_send("⏳ Đang đọc folder...")
    try:
        folder_title, chats = await _fetch_folder_chats(slug)
    except Exception as e:
        if not silent:
            await safe_send(f"❌ Không đọc được folder: {e}")
        log("ERROR", f"addfolder({slug}): {e}")
        return 0
    if not chats:
        if not silent:
            await safe_send("⚠️ Folder trống.")
        if remember:
            remember_folder(slug, folder_title)
        return 0
    channels       = load_channels()
    added, skipped = [], []
    for chat in chats:
        title    = getattr(chat, "title", "") or ""
        username = getattr(chat, "username", "") or ""
        raw_id   = getattr(chat, "id", None)
        if not raw_id or not title:
            continue
        tg_id = int(f"-100{raw_id}") if raw_id > 0 else raw_id
        if any(str(ch["id"]) == str(tg_id) for ch in channels):
            skipped.append(title)
            continue
        channels.append({"id": tg_id, "title": title, "username": username, "alias": ""})
        added.append(f"✅ #{len(channels)}. {title}" + (f" (@{username})" if username else ""))
    save_channels(channels)
    if remember:
        remember_folder(slug, folder_title)
    if not silent:
        out = [f"📁 Folder: {folder_title}"]
        if added:
            out.append(f"✅ Thêm {len(added)} kênh:")
            out.extend(added)
        if skipped:
            out.append(f"⚠️ Bỏ qua {len(skipped)} (đã có): {', '.join(skipped)}")
        if added:
            out.append("💡 /alias <số> <tên> để đặt tên tắt")
        out.append("🔄 Folder đã lưu — kênh mới sẽ tự sync mỗi 1h")
        await safe_send("\n".join(out))
    else:
        if added:
            log("FOLDER-SYNC", f"+{len(added)} kênh mới từ folder '{folder_title}'")
    return len(added)


async def cmd_addchan(raw: str):
    lines_raw    = [l.strip() for l in raw.splitlines() if l.strip()]
    if not lines_raw:
        await safe_send("❌ Không có link/id nào.")
        return
    folder_links = [l for l in lines_raw if "addlist" in l]
    identifiers  = [l for l in lines_raw if "addlist" not in l]
    for fl in folder_links:
        await cmd_addfolder(fl)
    if not identifiers:
        return
    channels               = load_channels()
    added, skipped, failed = [], [], []
    await safe_send(f"⏳ Đang xử lý {len(identifiers)} kênh...")
    for ident in identifiers:
        try:
            chat = await app.get_chat(ident)
            if any(str(ch["id"]) == str(chat.id) for ch in channels):
                skipped.append(f"⚠️ {chat.title} (đã có)")
                continue
            channels.append({"id": chat.id, "title": chat.title or "", "username": chat.username or "", "alias": ""})
            added.append(f"✅ #{len(channels)}. {chat.title}  (@{chat.username or 'private'})")
        except Exception as e:
            failed.append(f"❌ {ident}  → {e}")
        await asyncio.sleep(0.3)
    save_channels(channels)
    out = []
    if added:
        out.append(f"✅ Đã thêm {len(added)} kênh:")
        out.extend(added)
    if skipped:
        out.append(f"⚠️ Bỏ qua: {len(skipped)}")
        out.extend(skipped)
    if failed:
        out.append(f"❌ Thất bại {len(failed)}:")
        out.extend(failed)
    if added:
        out.append("💡 /alias <số> <tên> để đặt tên tắt")
    await safe_send("\n".join(out))


async def _probe_channel(ch):
    last_err = None
    for attempt in range(3):
        try:
            chat = await app.get_chat(ch["id"])
            return ("alive", chat)
        except FloodWait as e:
            wait = e.value + 2
            log("CHECK", f"FloodWait {wait}s khi check '{ch.get('title','?')}' — retry {attempt+1}/3")
            await asyncio.sleep(wait)
            last_err = e
        except DEAD_CHANNEL_ERRORS as e:
            return ("dead", f"{type(e).__name__}: {str(e)[:80]}")
        except SKIP_NOT_DEAD_ERRORS as e:
            return ("unknown", f"{type(e).__name__}: {str(e)[:80]}")
        except Exception as e:
            return ("unknown", f"{type(e).__name__}: {str(e)[:80]}")
    return ("unknown", f"FloodWait persistent: {last_err}")

async def cmd_checkchan(auto_clean: bool = False, silent: bool = False):
    channels = load_channels()
    if not channels:
        if not silent:
            await safe_send("📭 Chưa có kênh nào để check.")
        return 0
    total      = len(channels)
    status_msg = None
    if not silent:
        status_msg = await robust_send(f"🔍 Đang check {total} kênh... (0/{total})")
    alive, dead, unknown = [], [], []
    for i, ch in enumerate(channels):
        status, payload = await _probe_channel(ch)
        if status == "alive":
            chat = payload
            if chat.title:
                ch["title"] = chat.title
            ch["username"] = chat.username or ""
            alive.append(ch)
            log("CHECK", f"✓ {ch['title']}")
        elif status == "dead":
            dead.append({"ch": ch, "err": payload})
            log("CHECK", f"✗ DEAD {ch.get('title','?')} → {payload}")
        else:
            unknown.append({"ch": ch, "err": payload})
            log("CHECK", f"? UNKNOWN {ch.get('title','?')} → {payload} — GIỮ LẠI")
        if not silent and status_msg and ((i + 1) % 5 == 0 or i == total - 1):
            await robust_edit(
                INTERMEDIATE_CHAT, status_msg.id,
                f"🔍 Đang check... ({i+1}/{total})\n"
                f"✅ {len(alive)}   ❌ {len(dead)}   ❓ {len(unknown)}"
            )
        await asyncio.sleep(0.5)
    keep = alive + [item["ch"] for item in unknown]
    if auto_clean:
        save_channels(keep)
    else:
        save_channels(keep + [item["ch"] for item in dead])
    if silent:
        return len(dead)
    lines = [
        f"📊 Kết quả check {total} kênh:",
        "━━━━━━━━━━━━━━━",
        f"✅ Hoạt động:   {len(alive)}",
        f"❌ Chết:        {len(dead)}",
        f"❓ Không rõ:    {len(unknown)}  (FloodWait/network — giữ lại)",
    ]
    if dead:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("🪦 Kênh chết (sẽ xóa nếu /clean):")
        for item in dead:
            ch   = item["ch"]
            name = ch.get("title") or str(ch.get("id"))
            lines.append(f"  • {name}")
            lines.append(f"     └ {item['err']}")
    if unknown:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("❓ Không xác định (KHÔNG xóa — thử /check lại sau):")
        for item in unknown[:10]:
            ch   = item["ch"]
            name = ch.get("title") or str(ch.get("id"))
            lines.append(f"  • {name}  ({item['err'].split(':')[0]})")
        if len(unknown) > 10:
            lines.append(f"  ... và {len(unknown)-10} kênh khác")
    if auto_clean and dead:
        lines += ["━━━━━━━━━━━━━━━", f"🗑️ Đã xóa {len(dead)} kênh chết."]
    elif dead:
        lines += ["━━━━━━━━━━━━━━━", "💡 /clean — xóa các kênh chết ra khỏi danh sách"]
    elif not unknown:
        lines += ["━━━━━━━━━━━━━━━", "🎉 Tất cả kênh đều hoạt động!"]
    final = "\n".join(lines)
    if status_msg:
        ok = await robust_edit(INTERMEDIATE_CHAT, status_msg.id, final)
        if not ok:
            await robust_send(final)
    else:
        await robust_send(final)
    return len(dead)


async def task_auto_sync_folders():
    await asyncio.sleep(30)
    while True:
        folders = load_folders()
        if folders:
            log("AUTO-SYNC", f"Bắt đầu sync {len(folders)} folder...")
            total_added = 0
            for fd in folders:
                slug = fd.get("slug")
                if not slug:
                    continue
                try:
                    added = await cmd_addfolder(f"https://t.me/addlist/{slug}", silent=True, remember=False)
                    total_added += added or 0
                except Exception as e:
                    log("AUTO-SYNC", f"folder {slug}: {type(e).__name__}: {e}")
                await asyncio.sleep(2)
            if total_added > 0:
                await safe_send(f"🔄 Auto-sync: thêm {total_added} kênh mới từ folder đã lưu.")
            log("AUTO-SYNC", f"Hoàn tất — {'thêm ' + str(total_added) if total_added else 'không có kênh mới'}")
        await asyncio.sleep(FOLDER_SYNC_INTERVAL_SEC)

async def task_auto_clean_dead():
    await asyncio.sleep(300)
    while True:
        await asyncio.sleep(DEAD_CHECK_INTERVAL_SEC)
        if state.get("checking"):
            log("AUTO-CLEAN", "Bỏ lượt — đang có check khác chạy")
            continue
        def _user_busy():
            return any(
                s.get("content_msgs") or s.get("awaiting_channel") or s.get("all_mode")
                for s in state["slots"]
            )
        waited = 0
        while _user_busy() and waited < 600:
            await asyncio.sleep(60)
            waited += 60
        if _user_busy():
            log("AUTO-CLEAN", "Bỏ lượt — user vẫn đang bận")
            continue
        state["checking"] = True
        try:
            dead_count = await cmd_checkchan(auto_clean=True, silent=True)
            if dead_count and dead_count > 0:
                await safe_send(f"🧹 Auto-clean: đã xóa {dead_count} kênh chết.")
                log("AUTO-CLEAN", f"Hoàn tất — xóa {dead_count} kênh chết")
            else:
                log("AUTO-CLEAN", "Hoàn tất — không có kênh chết")
        except Exception as e:
            log("AUTO-CLEAN", f"Lỗi: {type(e).__name__}: {e}")
        finally:
            state["checking"] = False


COMMAND_ALIASES = {
    "/addchan":   "/add",
    "/addfolder": "/addf",
    "/listchan":  "/list",
    "/delchan":   "/del",
    "/aliaschan": "/alias",
    "/checkchan": "/check",
    "/cleanchan": "/clean",
}

def normalize_command(text: str):
    if not text.startswith("/"):
        return text
    for old, new in COMMAND_ALIASES.items():
        if text == old:
            return new
        if text.startswith(old + " ") or text.startswith(old + "\n"):
            return new + text[len(old):]
    return text
@app.on_message()
async def handler(client, msg: Message):

    if msg.chat.id == state["my_id"]:
        slot = active_slot()

        if not msg.forward_date or not slot["waiting"]:
            return

        if msg.forward_from_chat and msg.forward_from_chat.id == ADS_CHAT:
            slot["ads_chat_id"] = ADS_CHAT
            await load_ads_into(slot)
            return

        is_first = not slot.get("_topic_detect_started") and not slot.get("all_mode")
        if is_first:
            slot["_topic_detect_started"] = True
            slot["_topic_event"] = asyncio.Event()

            async def _detect(saved_msg_id, ev, target_slot):
                try:
                    async with _topic_resolve_lock:
                        for attempt in range(FWD_MAX_RETRY):
                            try:
                                while True:
                                    remain = _flood_until - time.monotonic()
                                    if remain <= 0:
                                        break
                                    await asyncio.sleep(remain)
                                src_id, top_id, top_title = await resolve_forward_topic(client, saved_msg_id)
                                if top_id is not None and _is_valid_topic_title(top_title):
                                    _set_slot_topic(target_slot, src_id, top_id, top_title, source="Batch")
                                    break
                                if attempt < FWD_MAX_RETRY - 1:
                                    await asyncio.sleep(2 * (attempt + 1))
                            except FloodWait as e:
                                wait = e.value + 2
                                log("FLOOD", f"_detect FloodWait {wait}s — retry {attempt+1}/{FWD_MAX_RETRY}")
                                await flood_wait_globally(wait, source="detect")
                                continue
                            except Exception as e:
                                if attempt < FWD_MAX_RETRY - 1:
                                    log("WARN", f"_detect attempt {attempt+1}: {type(e).__name__}: {e}")
                                    await asyncio.sleep(2 * (attempt + 1))
                                    continue
                                log("TOPIC", f"Không detect được topic: {e}")
                                break
                        else:
                            log("TOPIC", "Không detect được topic sau hết retry")
                finally:
                    target_slot["topic_checked"] = True
                    ev.set()

            asyncio.ensure_future(_detect(msg.id, slot["_topic_event"], slot))
        elif "_topic_event" not in slot:
            slot["_topic_event"] = asyncio.Event()
            slot["_topic_event"].set()

        if msg.media_group_id:
            gid = msg.media_group_id
            if gid in slot["seen_media_groups"]:
                return
            slot["seen_media_groups"].add(gid)
            slot["_album_pending"] = slot.get("_album_pending", 0) + 1
            album = None
            try:
                for attempt, delay in enumerate([0.5, 1.0, 2.0]):
                    await asyncio.sleep(delay)
                    try:
                        album = await client.get_media_group(SAVED_MESSAGES, msg.id)
                        if album and len(album) >= 1:
                            break
                        album = None
                    except FloodWait as e:
                        wait = e.value + 2
                        log("FLOOD", f"get_media_group FloodWait {wait}s")
                        await flood_wait_globally(wait, source="album")
                    except Exception as e:
                        log("WARN", f"get_media_group attempt {attempt+1}: {e}")
                if not album:
                    log("ERROR", f"Bỏ album {gid} — không fetch được sau 3 lần")
                    return
                slot["content_msgs"].append(album[0].id)
                slot["total_media_count"] = slot.get("total_media_count", 0) + len(album)
                log("MSG", f"Album {gid} ({len(album)} items) — "
                           f"bài #{len(slot['content_msgs'])} | "
                           f"total_media={slot['total_media_count']}")
            except Exception as e:
                log("ERROR", f"Album {gid} exception: {e}")
                return
            finally:
                slot["_album_pending"] = max(0, slot.get("_album_pending", 0) - 1)
                if not album:
                    slot["seen_media_groups"].discard(gid)
        else:
            slot["content_msgs"].append(msg.id)
            has_media = bool(msg.media)
            if has_media:
                slot["total_media_count"] = slot.get("total_media_count", 0) + 1
            log("MSG", f"Bài #{len(slot['content_msgs'])} id={msg.id}"
                       f"{' (media)' if has_media else ' (text)'}"
                       f" | total_media={slot.get('total_media_count', 0)}")

        schedule_update_menu(len(slot["content_msgs"]))
        return

    if msg.chat.id != INTERMEDIATE_CHAT:
        return

    text_raw = (msg.text or "")
    text     = text_raw.strip()
    if not text:
        return

    text     = normalize_command(text)
    text_raw = normalize_command(text_raw)

    if text == "/all":
        reset_slot(active_slot())
        active_slot()["all_mode"] = True
        n_ch = len(load_channels())
        await safe_send(
            f"📦 Chế độ /all ĐÃ BẬT (dùng 1 lần).\n"
            f"➡️ Forward bài vào Saved Messages → tool tự up lên TẤT CẢ {n_ch} kênh.\n"
            f"Gõ /next để huỷ nếu đổi ý."
        )
        return

    if text == "/xepbaiwhite" or text.startswith("/xepbaiwhite "):
        arg = text[len("/xepbaiwhite"):].strip()
        if not arg:
            wl = get_xepbai_whitelist()
            await safe_send(
                "⭐ Whitelist (luôn hiện nút /done dù đang OFF):\n"
                + (", ".join(sorted(wl)) if wl else "  (trống)")
                + "\n━━━━━━━━━━━━━━━\n"
                  "Dùng: /xepbaiwhite pro,real   (hoặc /xepbaiwhite clear để xoá)"
            )
            return
        if arg.lower() == "clear":
            set_topic_directive("@xepbaiwhite", "")
            await safe_send("⭐ Đã xoá whitelist.")
            return
        items = [x.strip().lstrip("/").lower()
                 for x in arg.replace(" ", ",").split(",") if x.strip()]
        set_topic_directive("@xepbaiwhite", ", ".join(items))
        await safe_send("⭐ Whitelist: " + (", ".join(items) if items else "(trống)")
                        + "\n→ Các nhóm này luôn hiện nút /done kể cả khi /xepbai off.")
        return

    if text == "/xepbai" or text.startswith("/xepbai "):
        arg = text[len("/xepbai"):].strip().lower()
        if arg in ("on", "off"):
            set_topic_directive("@xepbai", arg)
            if arg == "on":
                await safe_send("🟢 /xepbai ON — forward xong sẽ hiện nút /done để tự chọn.")
            else:
                await safe_send(
                    "🔴 /xepbai OFF — forward xong tool TỰ xếp theo gợi ý.\n"
                    "(Các nhóm trong /xepbaiwhite vẫn hiện nút bình thường.)"
                )
            return
        mode = get_xepbai_mode()
        wl   = get_xepbai_whitelist()
        await safe_send(
            f"⚙️ /xepbai đang: {mode.upper()}\n"
            f"⭐ Whitelist: {', '.join(sorted(wl)) if wl else '(trống)'}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"Dùng: /xepbai on  |  /xepbai off"
        )
        return

    if text == "/mapgen" or text == "/map gen":
        n_kept, n_new = gen_topic_map_txt()
        groups        = all_channel_cmds()
        lines         = [
            f"🧩 Đã ghi {TOPIC_MAP_TXT}: giữ {n_kept} dòng đã map, thêm {n_new} kênh chưa map.",
            "Mở file đó, điền tên topic vào TRƯỚC dấu =",
            "━━━━━━━━━━━━━━━",
            f"📡 Các nhóm kênh ({len(groups)}):",
        ]
        for cmd, chs in groups:
            titles = ", ".join((c.get("title") or "") for c in chs)
            lines.append(f"  /{cmd}  →  {titles}")
        await safe_send("\n".join(lines))
        return

    if text == "/map" or text.startswith("/map "):
        entries = load_topic_txt()
        if not entries:
            await safe_send(
                f"📭 Chưa map topic nào.\n"
                f"➡️ Gõ /mapgen để tool in sẵn danh sách kênh vào {TOPIC_MAP_TXT}.\n"
                f"Mỗi dòng:  tên_topic = tên_kênh"
            )
        else:
            lines = [f"🗺️ Mapping topic → kênh ({TOPIC_MAP_TXT}):"]
            for topic, cmd in entries:
                lines.append(f"  • {topic} → /{cmd}")
            lines += ["━━━━━━━━━━━━━━━", f"Sửa/thêm/xoá: mở file {TOPIC_MAP_TXT}. /mapgen để bổ sung kênh mới."]
            await safe_send("\n".join(lines))
        return

    if text.startswith("/add ") or text.startswith("/add\n") or text == "/add":
        raw = text_raw[4:].strip()
        if not raw:
            await safe_send("❌ Dùng:\n/add @kenh1\n@kenh2\nhttps://t.me/+xxx\n-100123456")
            return
        await cmd_addchan(raw)
        return

    if text.startswith("/addf"):
        lnk = text[5:].strip()
        if not lnk:
            await safe_send("❌ Dùng: /addf https://t.me/addlist/xxxxx")
            return
        await cmd_addfolder(lnk)
        return

    if text == "/list":
        channels = load_channels()
        if not channels:
            await safe_send("📭 Chưa có kênh nào.\n/add <link hoặc id> để thêm.")
            return
        lines = ["📋 Danh sách kênh:"]
        for i, ch in enumerate(channels):
            alias = f"  [{ch['alias']}]" if ch.get("alias") else ""
            lines.append(
                f"{i+1}. {ch['title']}{alias}\n"
                f"   @{ch.get('username') or 'private'}  |  ID: {ch['id']}"
            )
        folders = load_folders()
        if folders:
            lines.append("━━━━━━━━━━━━━━━")
            lines.append(f"📁 Folder auto-sync ({len(folders)}):")
            for fd in folders:
                lines.append(f"  • {fd.get('title') or fd.get('slug')}")
        await safe_send("\n".join(lines))
        return

    if text.startswith("/del "):
        try:
            idx      = int(text[5:].strip()) - 1
            channels = load_channels()
            if 0 <= idx < len(channels):
                removed = channels.pop(idx)
                save_channels(channels)
                await safe_send(f"🗑️ Đã xóa: {removed['title']}")
            else:
                await safe_send("❌ Số thứ tự không hợp lệ.")
        except ValueError:
            await safe_send("❌ Dùng: /del <số thứ tự>")
        return

    if text.startswith("/alias "):
        parts = text[7:].split(None, 1)
        if len(parts) == 2:
            try:
                idx      = int(parts[0]) - 1
                alias    = parts[1].strip()
                channels = load_channels()
                if 0 <= idx < len(channels):
                    channels[idx]["alias"] = alias
                    save_channels(channels)
                    await safe_send(f"✏️ Alias '{alias}' → {channels[idx]['title']}")
                else:
                    await safe_send("❌ Số thứ tự không hợp lệ.")
            except ValueError:
                await safe_send("❌ Dùng: /alias <số> <tên tắt>")
        return

    if text == "/check" or text == "/clean":
        if state.get("checking"):
            await safe_send("⏳ Đang check rồi, đợi lượt này xong đã nhé.")
            return
        auto_clean = (text == "/clean")
        async def _bg_check():
            state["checking"] = True
            try:
                await cmd_checkchan(auto_clean=auto_clean)
            except Exception as e:
                log("ERROR", f"check nền lỗi: {type(e).__name__}: {e}")
                await safe_send(f"❌ Check lỗi: {type(e).__name__}")
            finally:
                state["checking"] = False
        asyncio.ensure_future(_bg_check())
        await safe_send("🔍 Bắt đầu check ở nền — bạn vẫn forward bài bình thường.")
        return

    if waiting_slot():
        if text == "/skip":
            ws = waiting_slot()
            if ws and ws in state["slots"]:
                state["slots"].remove(ws)
            if not state["slots"]:
                new_s = make_slot()
                state["slots"].append(new_s)
                await load_ads_into(new_s)
            await safe_send("⏭️ Đã bỏ qua. Sẵn sàng nhận bài mới!")
            return
        if text.startswith("/") and len(text) > 1 and " " not in text and "\n" not in text:
            ws        = waiting_slot()
            candidate = text[1:].lower()
            if ws and candidate in (ws.get("channel_commands") or {}):
                await cmd_select_by_cmd(ws, candidate)
                return
        if text and not text.startswith("/"):
            await cmd_select_channel(text)
            return

    if text == "/skip":
        await safe_send(
            "⏭️ /skip chỉ dùng khi đang đợi chọn kênh (sau /done).\n"
            "Muốn bỏ batch đang gom dở thì gõ /next."
        )
        return

    if text == "/next":
        reset_state()
        await load_ads()
        await safe_send("🔄 Reset xong! Forward bài mới vào Saved Messages.")
        return

    if text == "/xdone":
        if not active_slot()["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.")
            return
        if not active_slot()["ads_msgs"]:
            await load_ads()
        await build_sequence(mode="xdone")
        return

    if text == "/zdone":
        if not active_slot()["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.")
            return
        if not active_slot()["ads_msgs"]:
            await load_ads()
        await build_sequence(mode="zdone")
        return

    if text.startswith("/done"):
        num = text[5:]
        try:
            cpa = int(num) if num else 1
        except ValueError:
            cpa = 1
        if not active_slot()["content_msgs"]:
            await safe_send("⚠️ Chưa có bài nào.")
            return
        if not active_slot()["ads_msgs"]:
            await load_ads()
        await build_sequence(cpa)
        return

    if text == "/help":
        await safe_send(
            "📖 Hướng dẫn v23\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚀 Flow:\n"
            "  1. Forward bài vào Saved Messages\n"
            "  2. /done* / /xdone / /zdone → tool xếp sequence\n"
            "  3. Gõ tên kênh → forward thẳng\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📡 Quản lý kênh:\n"
            "  /add <link/id>     thêm kênh\n"
            "  /addf <link>       thêm folder (tự sync mỗi 1h)\n"
            "  /list              xem danh sách\n"
            "  /del <số>          xóa kênh\n"
            "  /alias <số> <tên>  đặt tên tắt\n"
            "  /check             check kênh sống/chết\n"
            "  /clean             check + xóa kênh chết\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🗺️ Auto theo topic forum:\n"
            "  /mapgen            in sẵn danh sách kênh vào topic_map.txt\n"
            "  /map               xem mapping đang có\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚙️ Xếp bài:\n"
            "  /xepbai on/off     hiện/ẩn nút /done\n"
            "  /xepbaiwhite a,b   nhóm kênh luôn hiện nút dù off\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📋 Mode xếp:\n"
            "  /done1 ~ /done10   N content / 1 ads\n"
            "  /xdone             ads xen đều giữa bài\n"
            "  /zdone             ads trước, content sau\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📢 Lệnh khác:\n"
            "  /all    up nguyên bài lên TẤT CẢ kênh\n"
            "  /next   reset batch hiện tại\n"
            "  /skip   bỏ qua batch đang chờ chọn kênh\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 Tự động nền:\n"
            "  • Sync folder mỗi 1h\n"
            "  • Check + xóa kênh chết mỗi 6h\n"
            "  • Anti-flood: retry 6 lần, global flood gate\n"
            "  • Topic detect retry khi flood — không mất map\n"
            "✨ Emoji premium giữ nguyên (drop_author)!"
        )
        return


async def main():
    global _global_bucket
    _global_bucket = TokenBucket(FWD_GLOBAL_RATE, FWD_GLOBAL_BURST)
    log("CONFIG", f"INTERMEDIATE={INTERMEDIATE_CHAT} | ADS_CHAT={ADS_CHAT}")
    ensure_topic_map_txt()
    await app.start()

    me = await app.get_me()
    state["my_id"] = me.id
    log("START", f"Userbot chạy | my_id={me.id}")

    log("START", "Đang cache dialogs...")
    try:
        count = 0
        async for _ in app.get_dialogs():
            count += 1
        log("START", f"Cache xong {count} dialogs")
    except Exception as e:
        log("WARN", f"get_dialogs: {e}")

    try:
        await app.get_chat(INTERMEDIATE_CHAT)
        log("START", "Resolved INTERMEDIATE_CHAT ✓")
    except Exception as e:
        log("ERROR", f"Không resolve được INTERMEDIATE_CHAT: {e}")
        return

    try:
        await app.get_chat(ADS_CHAT)
        log("START", "Resolved ADS_CHAT ✓")
    except Exception as e:
        log("WARN", f"ADS_CHAT chưa cache: {e}")

    await load_ads()

    n_folders  = len(load_folders())
    n_channels = len(load_channels())
    await safe_send(
        "🤖 Userbot v23 đã khởi động!\n"
        f"📡 {n_channels} kênh • 📁 {n_folders} folder auto-sync\n"
        "➡️ Forward bài vào Saved Messages → /done* → nhập tên kênh.\n"
        "Gõ /help để xem hướng dẫn."
    )

    asyncio.ensure_future(task_auto_sync_folders())
    asyncio.ensure_future(task_auto_clean_dead())

    log("START", "📡 Đang lắng nghe + auto-tasks chạy nền...")
    await asyncio.Event().wait()

app.run(main())
