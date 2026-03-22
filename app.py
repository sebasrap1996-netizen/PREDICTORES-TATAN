"""
PREDICTORES TATAN — Aviator Bookmaker Scraping Manager
v5 — Correct SFS2X flow: Handshake→wait→Login→wait→Extensions
     Proper frame classification. No false crashes.
"""

import os, json, uuid, time, struct, zlib, base64, re, threading, logging
from datetime import datetime, timezone
from collections import defaultdict
from queue import Queue, Empty

import websocket
from flask import (
    Flask, render_template, request, jsonify, Response, stream_with_context
)

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("tatan")
logging.getLogger("websocket").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
bookmakers: dict = {}
crashes: dict = defaultdict(list)
ws_connections: dict = {}
ws_threads: dict = {}
sse_subscribers: dict = defaultdict(list)
connection_status: dict = {}
debug_frames: dict = defaultdict(list)
MAX_CRASHES = 1000
MAX_DBG = 200

def _load_config():
    global bookmakers, crashes
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                d = json.load(f)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                crashes[k] = v
            log.info("Loaded %d bookmakers", len(bookmakers))
        except Exception as e:
            log.error("Load fail: %s", e)

def _save_config():
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"bookmakers": bookmakers, "crashes": dict(crashes)},
                      f, indent=2, default=str)
    except Exception as e:
        log.error("Save fail: %s", e)

# ---------------------------------------------------------------------------
# SFS2X Decoder
# ---------------------------------------------------------------------------
class SFS2XDecoder:
    def __init__(self, data):
        self.data = data; self.pos = 0
    def remaining(self): return len(self.data) - self.pos
    def read_bytes(self, n):
        if self.pos + n > len(self.data): raise ValueError(f"EOF {n}@{self.pos}")
        r = self.data[self.pos:self.pos+n]; self.pos += n; return r
    def read_byte(self): return self.read_bytes(1)[0]
    def read_short(self): return struct.unpack(">h", self.read_bytes(2))[0]
    def read_ushort(self): return struct.unpack(">H", self.read_bytes(2))[0]
    def read_int(self): return struct.unpack(">i", self.read_bytes(4))[0]
    def read_long(self): return struct.unpack(">q", self.read_bytes(8))[0]
    def read_float(self): return struct.unpack(">f", self.read_bytes(4))[0]
    def read_double(self): return struct.unpack(">d", self.read_bytes(8))[0]
    def read_utf(self):
        n = self.read_ushort()
        return self.read_bytes(n).decode("utf-8", errors="replace")
    def read_value(self):
        t = self.read_byte(); return self._typed(t)
    def _typed(self, t):
        if t == 0: return None
        if t == 1: return self.read_byte() != 0
        if t == 2: return self.read_byte()
        if t == 3: return self.read_short()
        if t == 4: return self.read_int()
        if t == 5: return self.read_long()
        if t == 6: return round(self.read_float(), 4)
        if t == 7: return round(self.read_double(), 4)
        if t == 8: return self.read_utf()
        if t == 9:  n = self.read_short(); return [self.read_byte()!=0 for _ in range(n)]
        if t == 10: n = self.read_int(); return self.read_bytes(n)
        if t == 11: n = self.read_short(); return [self.read_short() for _ in range(n)]
        if t == 12: n = self.read_short(); return [self.read_int() for _ in range(n)]
        if t == 13: n = self.read_short(); return [self.read_long() for _ in range(n)]
        if t == 14: n = self.read_short(); return [round(self.read_float(),4) for _ in range(n)]
        if t == 15: n = self.read_short(); return [round(self.read_double(),4) for _ in range(n)]
        if t == 16: n = self.read_short(); return [self.read_utf() for _ in range(n)]
        if t == 17: n = self.read_short(); return [self.read_value() for _ in range(n)]
        if t == 18:
            n = self.read_short(); o = {}
            for _ in range(n): k = self.read_utf(); o[k] = self.read_value()
            return o
        if t == 19:
            c = self.read_utf(); n = self.read_short(); o = {"__class__": c}
            for _ in range(n): k = self.read_utf(); o[k] = self.read_value()
            return o
        return None

def _try_sfs_decode(raw):
    payloads = [("raw", raw)]
    try: payloads.append(("zlib", zlib.decompress(raw)))
    except: pass
    if raw and raw[0] == 0x80:
        off = 1
        if len(raw) > off + 1:
            if raw[off] & 0x40: off += 4
            else: off += 2
            payloads.append(("sfs_hdr", raw[off:]))
            try: payloads.append(("sfs_hdr_zlib", zlib.decompress(raw[off:])))
            except: pass
    for skip in range(1, min(9, len(raw))):
        payloads.append((f"skip{skip}", raw[skip:]))
    for label, data in payloads:
        if len(data) < 2: continue
        try:
            dec = SFS2XDecoder(data)
            result = dec.read_value()
            if isinstance(result, (dict, list)) and len(result) > 0:
                return result, label
        except: continue
    return None, None

# ---------------------------------------------------------------------------
# SFS2X Frame Classification
# Based on REAL Spribe protocol analysis:
#   msg1: {c:0, a:0} = Handshake request
#   msg2: {c:0, a:1} = Login request (NOTE: c=0, NOT c=1!)
#   msg3: {c:1, a:13} = Extension request
#   Server game data: {c:1, a:13, p:{c:"<handler>", ...}} = Extension response
# ---------------------------------------------------------------------------

# SFS protocol keys — NEVER game data
PROTOCOL_KEYS = {
    "ct", "ms", "tk", "aph",           # handshake response
    "zn", "un", "pw",                   # login params
    "rn", "ri", "rc", "uc", "id",      # room/user IDs
    "xt", "pi",                         # extension name, ping
}

# Spribe game multiplier keys — SOLO para crashes, no ticks en vuelo
GAME_KEYS = {
    "odd", "odds", "crash", "result",
    "coefficient", "crashValue", "endK", "bust",
    "coef", "coeff",
    "crashMultiplier", "finalMultiplier", "roundResult",
}

# Spribe extension command names que llevan el resultado final del crash
GAME_COMMANDS = {
    "N",                   # new round result (crash value)
    "F",                   # finish / crash
    "R",                   # round result
    "end",
    "finish",
    "currentbetsresult",
    "aviator.crash",
    "game.result",
    "crash",
    "roundEnd",
    "roundend",
    "gameend",
    "gameEnd",
}

# Comandos de estado/tick que NO son crashes — ignorar multiplicadores de estos
TICK_COMMANDS = {
    "x",           # tick del multiplicador en vuelo
    "X",
    "tick",
    "update",
    "updateCurrentCashOuts",
    "currentCashOuts",
    "B",           # bet info
    "T",           # timer/tick
    "S",           # state update (durante el vuelo)
}

# Spribe extension command names for game state (not crashes, but useful)
STATE_COMMANDS = {
    "currentBetsInfoHandler",
    "gameStateHandler",
    "statsHandler",
    "S",                   # state update
    "T",                   # tick/timer
    "B",                   # bet info
    "C",                   # cashout
}


def classify_frame(obj):
    """
    Classify SFS2X frame. Returns (type_str, is_game_data).
    """
    if not isinstance(obj, dict):
        return ("unknown", False)

    c = obj.get("c")
    a = obj.get("a")
    p = obj.get("p", {})

    # Handshake: {c:0, a:0} request or response with ct/ms/tk
    if c == 0 and a == 0:
        return ("handshake_req", False)
    if c == 0 and isinstance(p, dict) and ("ct" in p or "ms" in p or "tk" in p):
        return ("handshake_resp", False)

    # Login: {c:0, a:1} (Spribe uses c=0 for login, not c=1!)
    if c == 0 and a == 1:
        return ("login_req", False)

    # Login response from server
    if c == 1 and a == 0:
        return ("login_resp", False)

    # Room join
    if c == 4:
        return ("room_join", False)

    # Extension: {c:1, a:13} — THIS IS WHERE GAME DATA LIVES
    if a == 13:
        if isinstance(p, dict):
            cmd = p.get("c", "")
            if isinstance(cmd, str):
                cmd_lower = cmd.lower().strip()
                # Check if it's a known game result command
                for gc in GAME_COMMANDS:
                    if gc.lower() == cmd_lower:
                        return (f"ext_game:{cmd}", True)
                # Known state commands
                for sc in STATE_COMMANDS:
                    if sc.lower() == cmd_lower:
                        return (f"ext_state:{cmd}", True)
                # Unknown extension — still might have game data
                return (f"ext:{cmd}", True)
        return ("extension", True)

    # Non-extension frames: still scan for game data if they contain game keys
    # (some bookmakers use non-standard a values)
    if isinstance(p, dict) and any(k in p for k in GAME_KEYS):
        return (f"sfs_c{c}_a{a}_gamedata", True)

    # Ping/pong
    if c == 7:
        return ("ping", False)

    return (f"sfs_c{c}_a{a}", False)


# ---------------------------------------------------------------------------
# Multiplier extraction — ONLY from game data
# ---------------------------------------------------------------------------

def extract_game_mults(obj, label=""):
    """
    Extract crash multipliers from an extension response.
    Only looks in the 'p' (params) sub-object of extension frames.
    Ignora comandos de tick/vuelo — solo captura crashes reales.
    """
    if not isinstance(obj, dict):
        return []

    p = obj.get("p")
    if not isinstance(p, dict):
        return []

    # The game data is inside p.p (params.params) for extension responses
    inner_p = p.get("p")
    cmd = p.get("c", "")

    # Ignorar comandos de tick — son el multiplicador subiendo, no el crash
    if isinstance(cmd, str) and cmd.lower() in {c.lower() for c in TICK_COMMANDS}:
        return []

    results = []

    # Search in inner params first (p.p)
    if isinstance(inner_p, dict):
        results.extend(_scan_game_obj(inner_p, label, cmd))

    # Also search common Spribe subkeys: r, data, result, res, round
    for subkey in ("r", "data", "result", "res", "round", "game", "info"):
        sub = p.get(subkey) if isinstance(p, dict) else None
        if isinstance(sub, dict):
            results.extend(_scan_game_obj(sub, label, cmd))
        elif isinstance(sub, list):
            for item in sub:
                if isinstance(item, dict):
                    results.extend(_scan_game_obj(item, label, cmd))

    # Also search in p directly (some formats put data at top level of p)
    if not results:
        results.extend(_scan_game_obj(p, label, cmd))

    # Also search the full object
    if not results:
        results.extend(_scan_game_obj(obj, label, cmd))

    return results


def _scan_game_obj(obj, label="", cmd="", depth=0):
    """Recursively scan for game multiplier keys, skipping protocol keys."""
    if depth > 8 or not isinstance(obj, dict):
        return []

    results = []
    for key, val in obj.items():
        if key in PROTOCOL_KEYS:
            continue

        if key in GAME_KEYS:
            # Spribe sends multipliers as integers in x100 format:
            # odd=234 means 2.34x, odd=150 means 1.50x
            X100_INT_KEYS = {"odd", "odds", "k", "coef", "coeff",
                             "crashValue", "endK", "crashMultiplier",
                             "finalMultiplier", "coefficient"}
            if key in X100_INT_KEYS and isinstance(val, int):
                if val < 100:
                    continue  # < 1.00x, not a valid crash multiplier
                parsed_val = round(val / 100.0, 2)
                log.debug("[%s] x100 convert: key='%s' raw=%d -> %.2f", label, key, val, parsed_val)
            else:
                parsed_val = val
            m = _parse_mult(parsed_val)
            if m is not None:
                results.append(m)
                log.info("[%s] FOUND mult=%.2fx key='%s' raw_val=%s cmd='%s'", label, m, key, val, cmd)

        elif isinstance(val, dict):
            results.extend(_scan_game_obj(val, label, cmd, depth+1))
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    results.extend(_scan_game_obj(item, label, cmd, depth+1))
                elif isinstance(item, (int, float)):
                    # Arrays of multiplier values (e.g., history)
                    if key in GAME_KEYS:
                        m = _parse_mult(item)
                        if m is not None:
                            results.append(m)

    return results


def _parse_mult(val):
    """Parse a value as crash multiplier. Very strict filtering."""
    if not isinstance(val, (int, float)):
        # Try string
        if isinstance(val, str):
            try: val = float(val)
            except: return None
        else:
            return None

    fv = float(val)

    # Valid crash multiplier range: 1.00x to 9999.99x
    if 1.00 <= fv <= 9999.99:
        return round(fv, 2)

    return None


# ---------------------------------------------------------------------------
# Process frames
# ---------------------------------------------------------------------------
def process_binary(raw, bm_id, bm_name, decoder_type):
    label = bm_name or bm_id
    dbg = {
        "time": datetime.now(timezone.utc).isoformat(),
        "size": len(raw), "type": "BIN",
        "hex": raw[:100].hex(),
        "ascii": raw[:100].decode("ascii", errors="replace"),
        "decoded": None, "method": None, "frame_type": None, "mults": [],
    }

    mults = []

    # SFS2X
    if decoder_type in ("auto", "sfs"):
        parsed, method = _try_sfs_decode(raw)
        if parsed:
            dbg["decoded"] = _sj(parsed)[:1500]
            dbg["method"] = f"sfs2x/{method}"

            ftype, is_game = classify_frame(parsed) if isinstance(parsed, dict) else ("other", False)
            dbg["frame_type"] = ftype

            log.info("[%s] SFS2X(%s) → %s (game=%s): %s",
                     label, method, ftype, is_game, _sj(parsed)[:600])

            if is_game:
                mults = extract_game_mults(parsed, label)
            else:
                log.debug("[%s] Skipping non-game frame: %s", label, ftype)

    # MsgPack fallback
    if not mults and decoder_type in ("auto", "msgpack"):
        for payload in _variants(raw):
            try:
                import msgpack
                obj = msgpack.unpackb(payload, raw=False)
                if obj and isinstance(obj, dict):
                    dbg["decoded"] = _sj(obj)[:1500]
                    dbg["method"] = "msgpack"
                    ftype, is_game = classify_frame(obj)
                    dbg["frame_type"] = ftype
                    if is_game:
                        mults = extract_game_mults(obj, label)
                    if mults: break
            except: continue

    # JSON in binary
    if not mults:
        for payload in _variants(raw):
            try:
                txt = payload.decode("utf-8", errors="strict")
                for m in re.finditer(r'[{\[].*?[}\]]', txt, re.DOTALL):
                    try:
                        obj = json.loads(m.group())
                        if isinstance(obj, dict):
                            ftype, is_game = classify_frame(obj)
                            if is_game:
                                ms = extract_game_mults(obj, label)
                                if ms:
                                    dbg["method"] = "json_embed"
                                    dbg["frame_type"] = ftype
                                    mults.extend(ms)
                    except: continue
            except: continue

    seen = set(); unique = []
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)

    dbg["mults"] = unique
    _store_dbg(bm_id, dbg)
    return unique


def process_text(text, bm_id, bm_name):
    label = bm_name or bm_id
    dbg = {
        "time": datetime.now(timezone.utc).isoformat(),
        "size": len(text), "type": "TEXT",
        "text_preview": text[:600],
        "decoded": None, "method": None, "frame_type": None, "mults": [],
    }

    mults = []
    try:
        obj = json.loads(text)
        dbg["decoded"] = _sj(obj)[:1500]
        dbg["method"] = "json"
        if isinstance(obj, dict):
            ftype, is_game = classify_frame(obj)
            dbg["frame_type"] = ftype
            if is_game:
                mults = extract_game_mults(obj, label)
        log.info("[%s] JSON: %s", label, _sj(obj)[:600])
    except: pass

    if not mults:
        for line in text.split('\n'):
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    ftype, is_game = classify_frame(obj)
                    if is_game:
                        ms = extract_game_mults(obj, label)
                        if ms: mults.extend(ms)
            except: continue

    seen = set(); unique = []
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)
    dbg["mults"] = unique
    _store_dbg(bm_id, dbg)
    return unique


def _store_dbg(bm_id, dbg):
    debug_frames[bm_id].append(dbg)
    if len(debug_frames[bm_id]) > MAX_DBG:
        debug_frames[bm_id] = debug_frames[bm_id][-MAX_DBG:]

def _variants(raw):
    yield raw
    try: yield zlib.decompress(raw)
    except: pass

def _sj(obj):
    try: return json.dumps(obj, default=str, ensure_ascii=False)
    except: return str(obj)

# ---------------------------------------------------------------------------
# WebSocket Client — waits for server response between each message
# ---------------------------------------------------------------------------
def _b64_to_bytes(s):
    if not s or not s.strip(): return b""
    try: return base64.b64decode(s)
    except: return b""

def _start_ws(bm_id):
    bm = bookmakers.get(bm_id)
    if not bm: return
    ws_url = bm.get("ws_url", "").strip()
    if not ws_url:
        connection_status[bm_id] = "error"; return

    msg1 = _b64_to_bytes(bm.get("msg1_b64", ""))
    msg2 = _b64_to_bytes(bm.get("msg2_b64", ""))
    msg3 = _b64_to_bytes(bm.get("msg3_b64", ""))
    decoder_type = bm.get("decoder_type", "auto").lower()
    name = bm.get("name", bm_id)
    connection_status[bm_id] = "connecting"

    log.info("=" * 60)
    log.info("[%s] CONNECTING to %s", name, ws_url)
    log.info("[%s] msg1=%d bytes, msg2=%d bytes, msg3=%d bytes", name, len(msg1), len(msg2), len(msg3))
    log.info("=" * 60)

    # State machine for auth flow
    auth_state = {"step": "wait_connect", "handshake_done": False, "login_done": False}
    stats = {"sent": 0, "recv_bin": 0, "recv_txt": 0}

    def send_msg(ws, msg, idx, label_type):
        if not msg: return
        try:
            ws.send(msg, opcode=websocket.ABNF.OPCODE_BINARY)
            stats["sent"] += 1
            log.info("[%s] → SENT msg%d (%s, %d bytes)", name, idx, label_type, len(msg))
        except Exception as e:
            log.error("[%s] ✗ SEND FAIL msg%d: %s", name, idx, e)

    def on_open(ws):
        log.info("[%s] ✓ CONNECTED", name)
        connection_status[bm_id] = "connected"
        auth_state["step"] = "send_handshake"
        # Send msg1 (Handshake)
        send_msg(ws, msg1, 1, "Handshake")
        auth_state["step"] = "wait_handshake_resp"

    def on_message(ws, message):
        if isinstance(message, str):
            stats["recv_txt"] += 1
            log.info("[%s] ← TXT #%d (%d chars): %s",
                     name, stats["recv_txt"], len(message), message[:500])
            mults = process_text(message, bm_id, name)
            for m in mults:
                _record_crash(bm_id, name, m)
            return

        stats["recv_bin"] += 1
        log.info("[%s] ← BIN #%d (%d bytes): %s",
                 name, stats["recv_bin"], len(message), message[:80].hex())

        # Decode frame
        parsed, method = _try_sfs_decode(message)
        ftype = "unknown"
        is_game = False

        if parsed and isinstance(parsed, dict):
            ftype, is_game = classify_frame(parsed)
            log.info("[%s] Frame: %s (game=%s) decoded=%s",
                     name, ftype, is_game, _sj(parsed)[:600])

        # Store in debug
        dbg = {
            "time": datetime.now(timezone.utc).isoformat(),
            "size": len(message), "type": "BIN",
            "hex": message[:100].hex(),
            "ascii": message[:100].decode("ascii", errors="replace"),
            "decoded": _sj(parsed)[:1500] if parsed else None,
            "method": f"sfs2x/{method}" if method else None,
            "frame_type": ftype,
            "mults": [],
        }

        # AUTH STATE MACHINE
        step = auth_state["step"]

        if step == "wait_handshake_resp" and ftype == "handshake_resp":
            log.info("[%s] ✓ Handshake response received. Sending Login (msg2)...", name)
            auth_state["handshake_done"] = True
            auth_state["step"] = "wait_login_resp"
            # Small delay then send Login
            time.sleep(0.3)
            send_msg(ws, msg2, 2, "Login")

        elif step == "wait_login_resp" and ftype in ("login_resp", "room_join"):
            log.info("[%s] ✓ Login/Room response received. Sending Extension (msg3)...", name)
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            time.sleep(0.3)
            send_msg(ws, msg3, 3, "Extension:currentBets")
            # Process any game data that arrived with the login response
            if is_game and parsed:
                mults = extract_game_mults(parsed, name)
                dbg["mults"] = mults
                for m in mults:
                    _record_crash(bm_id, name, m)

        elif step == "wait_login_resp" and "ext" in ftype:
            # Sometimes login response is followed immediately by extensions
            log.info("[%s] ✓ Got extension before explicit login resp, auth seems ok", name)
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            # Still send msg3
            if not auth_state.get("msg3_sent"):
                auth_state["msg3_sent"] = True
                time.sleep(0.3)
                send_msg(ws, msg3, 3, "Extension:currentBets")
            # Process game data
            if is_game:
                mults = extract_game_mults(parsed, name)
                dbg["mults"] = mults
                for m in mults:
                    _record_crash(bm_id, name, m)

        elif auth_state["step"] == "authenticated" and is_game:
            # NORMAL GAME DATA FLOW
            mults = extract_game_mults(parsed, name)
            dbg["mults"] = mults
            for m in mults:
                _record_crash(bm_id, name, m)

        elif auth_state["step"] == "authenticated":
            # Non-game frame after auth (ping, room update, etc.)
            log.debug("[%s] Non-game frame post-auth: %s", name, ftype)

        else:
            # Handle case where server sends multiple frames in handshake
            # or unexpected order
            if not auth_state["handshake_done"] and ftype != "handshake_resp":
                log.info("[%s] Non-handshake frame during handshake phase: %s", name, ftype)
                # Maybe the handshake was implicit, try sending login
                if msg2:
                    log.info("[%s] Attempting Login anyway...", name)
                    auth_state["handshake_done"] = True
                    auth_state["step"] = "wait_login_resp"
                    time.sleep(0.3)
                    send_msg(ws, msg2, 2, "Login")

        _store_dbg(bm_id, dbg)

    def on_error(ws, error):
        log.error("[%s] ✗ ERROR: %s", name, error)
        connection_status[bm_id] = "error"

    def on_close(ws, code, msg):
        log.info("[%s] CLOSED code=%s | handshake=%s login=%s step=%s | sent=%d bin=%d txt=%d",
                 name, code,
                 auth_state["handshake_done"], auth_state["login_done"], auth_state["step"],
                 stats["sent"], stats["recv_bin"], stats["recv_txt"])
        connection_status[bm_id] = "disconnected"
        if bm_id in bookmakers and bookmakers[bm_id].get("active", False):
            log.info("[%s] Reconnect in 5s...", name)
            time.sleep(5)
            _start_ws(bm_id)

    ws_app = websocket.WebSocketApp(
        ws_url, on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close,
    )
    ws_connections[bm_id] = ws_app
    t = threading.Thread(target=ws_app.run_forever,
                         kwargs={"ping_interval": 20, "ping_timeout": 10}, daemon=True)
    t.start()
    ws_threads[bm_id] = t


def _record_crash(bm_id, bm_name, multiplier):
    # Dedup: don't record same multiplier within 2 seconds
    recent = crashes.get(bm_id, [])
    if recent:
        last = recent[-1]
        if last["multiplier"] == multiplier:
            try:
                last_t = datetime.fromisoformat(last["timestamp"])
                now = datetime.now(timezone.utc)
                if (now - last_t).total_seconds() < 2:
                    return  # Skip duplicate
            except: pass

    entry = {
        "multiplier": multiplier,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "id": str(uuid.uuid4())[:8],
    }
    crashes[bm_id].append(entry)
    if len(crashes[bm_id]) > MAX_CRASHES:
        crashes[bm_id] = crashes[bm_id][-MAX_CRASHES:]
    for q in sse_subscribers.get(bm_id, []):
        try: q.put_nowait(entry)
        except: pass
    log.info("[%s] ★★★ CRASH RECORDED: %.2fx ★★★", bm_name, multiplier)
    if len(crashes[bm_id]) % 5 == 0:
        _save_config()

def _stop_ws(bm_id):
    ws = ws_connections.pop(bm_id, None)
    if ws:
        try: ws.close()
        except: pass
    ws_threads.pop(bm_id, None)
    connection_status[bm_id] = "disconnected"

# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
@app.route("/")
def index(): return render_template("admin.html")

@app.route("/api/aviator/bookmakers", methods=["GET"])
def list_bm():
    r = []
    for bid, bm in bookmakers.items():
        item = {**bm, "id": bid}
        item["connection_status"] = connection_status.get(bid, "disconnected")
        item["crash_count"] = len(crashes.get(bid, []))
        r.append(item)
    return jsonify(r)

@app.route("/api/aviator/bookmakers", methods=["POST"])
def create_bm():
    d = request.get_json(force=True)
    bid = str(uuid.uuid4())[:12]
    bm = {
        "name": d.get("name", "Sin Nombre"),
        "description": d.get("description", ""),
        "ws_url": d.get("ws_url", ""),
        "msg1_b64": d.get("msg1_b64", ""),
        "msg2_b64": d.get("msg2_b64", ""),
        "msg3_b64": d.get("msg3_b64", ""),
        "decoder_type": d.get("decoder_type", "auto"),
        "active": d.get("active", True),
        "recommended": d.get("recommended", False),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    bookmakers[bid] = bm; _save_config()
    if bm["active"]: _start_ws(bid)
    return jsonify({"id": bid, **bm}), 201

@app.route("/api/aviator/bookmakers/<bid>", methods=["PUT"])
def update_bm(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    d = request.get_json(force=True); bm = bookmakers[bid]
    for k in ("name","description","ws_url","msg1_b64","msg2_b64","msg3_b64",
              "decoder_type","active","recommended"):
        if k in d: bm[k] = d[k]
    bm["updated_at"] = datetime.now(timezone.utc).isoformat()
    bookmakers[bid] = bm; _save_config(); _stop_ws(bid)
    if bm.get("active", False): _start_ws(bid)
    return jsonify({"id": bid, **bm})

@app.route("/api/aviator/bookmakers/<bid>", methods=["DELETE"])
def delete_bm(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    _stop_ws(bid); del bookmakers[bid]
    crashes.pop(bid, None); debug_frames.pop(bid, None); _save_config()
    return jsonify({"deleted": bid})

@app.route("/api/aviator/bookmakers/<bid>/crashes", methods=["GET"])
def get_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    limit = request.args.get("limit", 100, type=int)
    return jsonify(crashes.get(bid, [])[-limit:])

@app.route("/api/aviator/bookmakers/<bid>/crashes/stream")
def stream_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    q = Queue(maxsize=100); sse_subscribers[bid].append(q)
    def gen():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                try:
                    e = q.get(timeout=30)
                    yield f"data: {json.dumps(e)}\n\n"
                except Empty:
                    yield ": hb\n\n"
        except GeneratorExit: pass
        finally:
            try: sse_subscribers[bid].remove(q)
            except: pass
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no","Connection":"keep-alive"})

@app.route("/api/aviator/bookmakers/<bid>/debug")
def get_debug(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    return jsonify({
        "bookmaker": bid, "name": bookmakers[bid].get("name"),
        "connection_status": connection_status.get(bid, "disconnected"),
        "frame_count": len(debug_frames.get(bid, [])),
        "crash_count": len(crashes.get(bid, [])),
        "frames": debug_frames.get(bid, [])[-30:],
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "ok", "bookmakers": len(bookmakers),
        "active": sum(1 for s in connection_status.values() if s == "connected"),
        "crashes": sum(len(v) for v in crashes.values()),
    })

@app.route("/api/aviator/bookmakers/<bid>/ingest", methods=["POST"])
def ingest_crash(bid):
    """
    Endpoint para el DOM scraper (Playwright).
    Recibe: {"multiplier": 2.34, "source": "dom_scraper", "timestamp": "..."}
    """
    if bid not in bookmakers:
        return jsonify({"error": "Not found"}), 404
    d = request.get_json(force=True)
    mult = d.get("multiplier")
    if not isinstance(mult, (int, float)) or not (1.00 <= float(mult) <= 9999.99):
        return jsonify({"error": "Invalid multiplier"}), 400
    source = d.get("source", "unknown")
    name = bookmakers[bid].get("name", bid)
    log.info("[%s] INGEST from %s: %.2fx", name, source, mult)
    _record_crash(bid, name, float(mult))
    return jsonify({"ok": True, "multiplier": mult}), 201

# ---------------------------------------------------------------------------
_load_config()
def _autostart():
    time.sleep(2)
    for bid, bm in list(bookmakers.items()):
        if bm.get("active", False):
            log.info("Auto-start: %s", bm.get("name", bid))
            _start_ws(bid); time.sleep(1)
threading.Thread(target=_autostart, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
