"""
PREDICTORES TATAN — Aviator Bookmaker Scraping Manager
v8.11 — Fix congelamiento de crashes en vivo para 1win:
  Reconexión proactiva de 120s deshabilitada para spribegaming64.click (1win).
  En 1win esa reconexión creaba sesión duplicada → server_disconnect:dr1 cada ~65s
  → gap sin datos. Solo se activa para spribegaming.com que tiene timeout real de 150s.
  Mantiene todos los fixes v8.10.
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

@app.after_request
def _add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def _cors_preflight(path):
    return "", 204

# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------
_DATA_DIR    = "/data" if os.path.isdir("/data") else os.path.dirname(__file__) or "."
CONFIG_PATH  = os.path.join(_DATA_DIR, "config.json")
CRASHES_PATH = os.path.join(_DATA_DIR, "crashes.json")
bookmakers: dict = {}
crashes: dict = defaultdict(list)
ws_connections: dict = {}
ws_threads: dict = {}
sse_subscribers: dict = defaultdict(list)
connection_status: dict = {}
debug_frames: dict = defaultdict(list)
MAX_CRASHES  = 1000
MAX_DBG      = 200
_bm_counter: int = 0
_num_to_bid: dict = {}
ws_generation: dict = {}
ws_reconnect_attempts: dict = {}   # contador de reintentos por bookmaker
ws_all_connections: dict = {}      # lista de TODAS las ws vivas (para matar zombies)
ws_login_retries: dict = {}        # v8.8: contador de reintentos de login por bookmaker
_save_event = threading.Event()    # thread-safe flag para guardar crashes en batch
_save_lock = threading.Lock()
_crashes_lock = threading.Lock()   # v8.12: evita duplicados por race condition entre hilos WS

def _load_config():
    """Carga bookmakers + crashes del disco."""
    global bookmakers
    env_json = os.environ.get("BOOKMAKERS_JSON", "").strip()
    if env_json:
        try:
            d = json.loads(env_json)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                if isinstance(v, list): crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from env var", len(bookmakers))
        except Exception as e:
            log.error("BOOKMAKERS_JSON fail: %s", e)
    if not bookmakers and os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                d = json.load(f)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                if isinstance(v, list): crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from config.json", len(bookmakers))
        except Exception as e:
            log.error("config.json fail: %s", e)
    # crashes.json tiene prioridad (se guarda más frecuente)
    if os.path.exists(CRASHES_PATH):
        try:
            with open(CRASHES_PATH) as f:
                saved = json.load(f)
            for k, v in saved.items():
                if isinstance(v, list) and len(v) > 0:
                    crashes[k] = v
            log.info("Restored %d crashes from disk", sum(len(v) for v in crashes.values()))
        except Exception as e:
            log.error("crashes.json fail: %s", e)
    total = sum(len(v) for v in crashes.values())
    log.info("★ READY: %d bookmakers, %d crashes ★", len(bookmakers), total)

def _save_config():
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"bookmakers": bookmakers, "crashes": {}}, f, indent=2, default=str)
    except Exception as e:
        log.error("config save fail: %s", e)

def _save_crashes():
    with _save_lock:
        try:
            data = {k: v[-MAX_CRASHES:] for k, v in crashes.items() if v}
            with open(CRASHES_PATH, "w") as f:
                json.dump(data, f, default=str)
        except Exception as e:
            log.error("crashes save fail: %s", e)

def _rebuild_num_index():
    global _bm_counter, _num_to_bid
    _num_to_bid = {}; max_num = 0
    for bid, bm in bookmakers.items():
        nid = bm.get("num_id")
        if isinstance(nid, int):
            _num_to_bid[nid] = bid
            if nid > max_num: max_num = nid
    for bid, bm in bookmakers.items():
        if not isinstance(bm.get("num_id"), int):
            max_num += 1; bm["num_id"] = max_num; _num_to_bid[max_num] = bid
    _bm_counter = max_num

# ---------------------------------------------------------------------------
# SFS2X Decoder — EXACTO de v6
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
    if raw and raw[0] == 0x80:
        off = 1
        if len(raw) > off + 1:
            if raw[off] & 0x40: off += 4
            else: off += 2
            payloads.append(("sfs_hdr", raw[off:]))
            try: payloads.append(("sfs_hdr_zlib", zlib.decompress(raw[off:])))
            except: pass
    if raw and raw[0] == 0xa0 and len(raw) > 3:
        candidate = raw[3:]
        try: payloads.append(("a0_hdr_zlib", zlib.decompress(candidate)))
        except: pass
        if len(raw) > 4:
            try: payloads.append(("a0_hdr4_zlib", zlib.decompress(raw[4:])))
            except: pass
    try: payloads.append(("zlib", zlib.decompress(raw)))
    except: pass
    for i in range(1, min(16, len(raw) - 2)):
        if raw[i] == 0x78 and raw[i+1] in (0x9c, 0xda, 0x01, 0x5e):
            try: payloads.append((f"zlib_at{i}", zlib.decompress(raw[i:])))
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
# Frame Classification — EXACTO de v6 (esto FUNCIONABA)
# ---------------------------------------------------------------------------
PROTOCOL_KEYS = {
    "ct", "ms", "tk", "aph",
    "zn", "un", "pw",
    "rn", "ri", "rc", "uc", "id",
    "xt", "pi",
    "cashouts", "openBetsCount", "winAmount", "betId", "player_id",
}

GAME_KEYS = {
    "odd", "odds", "crash", "result",
    "coefficient", "crashValue", "endK", "bust",
    "coef", "coeff",
    "crashMultiplier", "finalMultiplier", "roundResult",
    "maxMultiplier",
    "crashX",
}

GAME_COMMANDS = {
    "N", "F", "R",
    "end", "finish",
    "currentbetsresult",
    "aviator.crash",
    "game.result",
    "crash",
    "roundEnd", "roundend",
    "gameend", "gameEnd",
    "roundChartInfo",
}

TICK_COMMANDS = {
    "x", "X",
    "tick", "update",
    "updateCurrentCashOuts", "currentCashOuts", "cashouts",
    "B", "T", "S",
    # Spribe: frames con listas de jugadores/cashouts — NO son crashes
    "updateCurrentBets", "currentBetsInfo",
    "onlinePlayers",
}

STATE_COMMANDS = {
    "currentBetsInfoHandler",
    "gameStateHandler",
    "statsHandler",
    "S", "T", "B", "C",
    # Spribe: cambio de estado de ronda (betting phase, no crash)
    "changeState",
    # Spribe: historial al conectar — NO registrar como crashes en vivo
    "init",
}


def classify_frame(obj):
    """EXACTO de v6 — no tocar."""
    if not isinstance(obj, dict):
        return ("unknown", False)
    c = obj.get("c"); a = obj.get("a"); p = obj.get("p", {})
    # v8.4: handshake_resp ANTES que handshake_req — ambos tienen c=0,a=0
    # El servidor envía p={ct,ms,tk} en su respuesta; el cliente no envía p
    if c == 0 and isinstance(p, dict) and ("ct" in p or "ms" in p or "tk" in p):
        return ("handshake_resp", False)
    if c == 0 and a == 0: return ("handshake_req", False)
    if c == 0 and a == 1:
        # v8.2: el servidor responde con ec!=0 → error de login
        if isinstance(p, dict) and p.get("ec", 0) != 0:
            return ("login_error", False)
        # v8.4: login exitoso — el servidor devuelve rs=0, zn, un
        if isinstance(p, dict) and "rs" in p:
            return ("login_resp_ok", False)
        return ("login_req", False)
    if c == 1 and a == 0: return ("login_resp", False)
    # v8.3: SFS2X server-initiated disconnect (a=1005, p={dr:X})
    # dr=1 = sesión kickeada por login duplicado, dr=2 = ban, etc.
    if a == 1005:
        dr = p.get("dr", "?") if isinstance(p, dict) else "?"
        return (f"server_disconnect:dr{dr}", False)
    if c == 4: return ("room_join", False)
    if a == 13:
        if isinstance(p, dict):
            cmd = p.get("c", "")
            if isinstance(cmd, str):
                cmd_lower = cmd.lower().strip()
                for gc in GAME_COMMANDS:
                    if gc.lower() == cmd_lower:
                        return (f"ext_game:{cmd}", True)
                for sc in STATE_COMMANDS:
                    if sc.lower() == cmd_lower:
                        return (f"ext_state:{cmd}", True)
                return (f"ext:{cmd}", True)
        return ("extension", True)
    if isinstance(p, dict) and any(k in p for k in GAME_KEYS):
        return (f"sfs_c{c}_a{a}_gamedata", True)
    if c == 7: return ("ping", False)
    return (f"sfs_c{c}_a{a}", False)


# ---------------------------------------------------------------------------
# Multiplier extraction — EXACTO de v6
# ---------------------------------------------------------------------------
def extract_game_mults(obj, label=""):
    if not isinstance(obj, dict): return [], None
    p = obj.get("p")
    if not isinstance(p, dict): return [], None
    inner_p = p.get("p"); cmd = p.get("c", "")
    # Filtrar ticks en vuelo Y frames de estado — NO contienen crashes reales
    if isinstance(cmd, str):
        cmd_l = cmd.lower()
        skip_cmds = {c.lower() for c in TICK_COMMANDS} | {c.lower() for c in STATE_COMMANDS}
        if cmd_l in skip_cmds:
            # Única excepción: ext:x con crashX = crash real
            has_crash_x = isinstance(inner_p, dict) and "crashX" in inner_p
            if not has_crash_x:
                return [], None
    round_id = None
    if isinstance(inner_p, dict): round_id = inner_p.get("roundId")
    results = []
    if isinstance(inner_p, dict):
        results.extend(_scan_game_obj(inner_p, label, cmd))
    for subkey in ("r", "data", "result", "res", "round", "game", "info"):
        sub = p.get(subkey) if isinstance(p, dict) else None
        if isinstance(sub, dict): results.extend(_scan_game_obj(sub, label, cmd))
        elif isinstance(sub, list):
            for item in sub:
                if isinstance(item, dict): results.extend(_scan_game_obj(item, label, cmd))
    if not results: results.extend(_scan_game_obj(p, label, cmd))
    if not results: results.extend(_scan_game_obj(obj, label, cmd))
    return results, round_id


def _scan_game_obj(obj, label="", cmd="", depth=0):
    if depth > 8 or not isinstance(obj, dict): return []
    # Si este dict huele a cashout de jugador (tiene player_id, betId, winAmount),
    # NO extraer multiplier de aquí — es el mult al que cashouteo el jugador, NO el crash
    if any(k in obj for k in ("player_id", "betId", "winAmount", "cashOutAt", "username")):
        return []
    results = []
    for key, val in obj.items():
        if key in PROTOCOL_KEYS: continue
        # Ignorar listas de cashouts/bets — contienen multiplier de jugadores
        if key in ("cashouts", "cashOuts", "bets", "currentBets", "activeBets",
                    "topPlayerProfileImages"):
            continue
        if key in GAME_KEYS:
            X100_INT_KEYS = {"odd", "odds", "k", "coef", "coeff",
                             "crashValue", "endK", "crashMultiplier",
                             "finalMultiplier", "coefficient"}
            if key in X100_INT_KEYS and isinstance(val, int):
                if val < 100: continue
                parsed_val = round(val / 100.0, 2)
            else:
                parsed_val = val
            m = _parse_mult(parsed_val)
            if m is not None:
                results.append(m)
                log.info("[%s] FOUND mult=%.2fx key='%s' raw=%s cmd='%s'", label, m, key, val, cmd)
        elif isinstance(val, dict):
            results.extend(_scan_game_obj(val, label, cmd, depth+1))
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    results.extend(_scan_game_obj(item, label, cmd, depth+1))
    return results


def _parse_mult(val):
    if not isinstance(val, (int, float)):
        if isinstance(val, str):
            try: val = float(val)
            except: return None
        else: return None
    fv = float(val)
    if 1.00 <= fv <= 9999.99: return round(fv, 2)
    return None


# ---------------------------------------------------------------------------
# Process frames — EXACTO de v6
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
                mults, _ = extract_game_mults(parsed, label)
    if not mults and decoder_type in ("auto", "msgpack"):
        for payload in _variants(raw):
            try:
                import msgpack
                obj = msgpack.unpackb(payload, raw=False)
                if obj and isinstance(obj, dict):
                    dbg["decoded"] = _sj(obj)[:1500]; dbg["method"] = "msgpack"
                    ftype, is_game = classify_frame(obj); dbg["frame_type"] = ftype
                    if is_game: mults, _ = extract_game_mults(obj, label)
                    if mults: break
            except: continue
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
                                ms, _ = extract_game_mults(obj, label)
                                if ms:
                                    dbg["method"] = "json_embed"; dbg["frame_type"] = ftype
                                    mults.extend(ms)
                    except: continue
            except: continue
    seen = set(); unique = []
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)
    dbg["mults"] = unique; _store_dbg(bm_id, dbg)
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
        dbg["decoded"] = _sj(obj)[:1500]; dbg["method"] = "json"
        if isinstance(obj, dict):
            ftype, is_game = classify_frame(obj); dbg["frame_type"] = ftype
            if is_game: mults, _ = extract_game_mults(obj, label)
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
                        ms, _ = extract_game_mults(obj, label)
                        if ms: mults.extend(ms)
            except: continue
    seen = set(); unique = []
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)
    dbg["mults"] = unique; _store_dbg(bm_id, dbg)
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
# WebSocket Client — v6 auth logic + frame 0 capture + robust reconnect
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

    gen = ws_generation.get(bm_id, 0) + 1
    ws_generation[bm_id] = gen

    # v8.3: matar TODOS los zombies previos (no solo el último guardado)
    old = ws_connections.pop(bm_id, None)
    if old:
        try: old.close()
        except: pass
    for zombie in ws_all_connections.pop(bm_id, []):
        try: zombie.close()
        except: pass

    # v8.8: pausa 5s para que el servidor registre el cierre antes del nuevo login
    # Sin esto, la reconexión proactiva puede provocar ec=28 (sesión duplicada transitoria)
    # Aumentado de 2s a 5s para compatibilidad con 1win (más lento registrando cierres)
    time.sleep(5)

    msg1 = _b64_to_bytes(bm.get("msg1_b64", ""))
    msg2 = _b64_to_bytes(bm.get("msg2_b64", ""))
    msg3 = _b64_to_bytes(bm.get("msg3_b64", ""))
    decoder_type = bm.get("decoder_type", "auto").lower()
    name = bm.get("name", bm_id)
    connection_status[bm_id] = "connecting"

    log.info("[%s] ═══ CONNECTING gen=%d to %s ═══", name, gen, ws_url)

    auth_state = {"step": "wait_connect", "handshake_done": False,
                  "login_done": False, "msg3_sent": False}
    stats = {"sent": 0, "recv_bin": 0, "recv_txt": 0}
    last_frame_time = [time.time()]   # v8.6: tracker de último frame recibido

    def send_msg(ws, msg, idx, label_type):
        if not msg: return
        try:
            ws.send(msg, opcode=websocket.ABNF.OPCODE_BINARY)
            stats["sent"] += 1
            log.info("[%s] → SENT msg%d (%s, %d bytes)", name, idx, label_type, len(msg))
        except Exception as e:
            log.error("[%s] ✗ SEND FAIL msg%d: %s", name, idx, e)

    def send_delayed(ws, msg, idx, label_type, delay=0.1):
        """v8.2: envía en hilo separado para NO bloquear on_message con time.sleep."""
        def _send():
            time.sleep(delay)
            send_msg(ws, msg, idx, label_type)
        threading.Thread(target=_send, daemon=True).start()

    def on_open(ws):
        log.info("[%s] ✓ CONNECTED gen=%d", name, gen)
        connection_status[bm_id] = "connected"
        ws_reconnect_attempts[bm_id] = 0
        ws_login_retries[bm_id] = 0       # v8.8: reset reintentos de login al conectar
        auth_state["step"] = "wait_handshake_resp"
        send_msg(ws, msg1, 1, "Handshake")

        # v8.6: watchdog por conexión — detecta silent disconnect (>45s sin frames)
        def _silence_watchdog():
            time.sleep(30)   # dar tiempo al auth
            while ws_generation.get(bm_id) == gen:
                silence = time.time() - last_frame_time[0]
                if silence > 45:
                    log.warning("[%s] ⏱ Silencio %.0fs sin frames — reconectando", name, silence)
                    try: ws.close()
                    except: pass
                    return
                time.sleep(10)
        threading.Thread(target=_silence_watchdog, daemon=True).start()

        # v8.7: reconexión PROACTIVA a los 120s para evitar hard timeout Spribe (150s)
        # v8.11: solo para spribegaming.com — 1win usa spribegaming64.click que NO tiene
        # timeout de 150s. En 1win la reconexión proactiva genera sesión duplicada → dr=1
        # → gap de ~65s sin datos. Se desactiva detectando el dominio del ws_url.
        _is_spribe_timeout = "spribegaming.com" in ws_url and "spribegaming64" not in ws_url
        def _proactive_reconnect():
            time.sleep(120)
            if not _is_spribe_timeout:
                return  # 1win u otro servidor sin timeout 150s — no reconectar proactivamente
            if ws_generation.get(bm_id) == gen and \
               bm_id in bookmakers and bookmakers[bm_id].get("active", False) and \
               connection_status.get(bm_id) != "error":
                log.info("[%s] ↺ Reconexión proactiva (120s) — evitando timeout Spribe", name)
                _start_ws(bm_id)
        threading.Thread(target=_proactive_reconnect, daemon=True).start()

    def on_message(ws, message):
        if ws_generation.get(bm_id) != gen:
            return
        last_frame_time[0] = time.time()   # v8.6: actualizar timestamp en cada frame

        # ── TEXT frames ──
        if isinstance(message, str):
            stats["recv_txt"] += 1
            if stats["recv_txt"] <= 3 or stats["recv_txt"] % 100 == 0:
                log.info("[%s] ← TXT #%d (%d chars): %s",
                         name, stats["recv_txt"], len(message), message[:300])
            mults = process_text(message, bm_id, name)
            for m in mults:
                _record_crash(bm_id, name, m)
            return

        # ── BINARY frames ──
        stats["recv_bin"] += 1
        if stats["recv_bin"] <= 3 or stats["recv_bin"] % 100 == 0:
            log.info("[%s] ← BIN #%d (%d bytes): %s",
                     name, stats["recv_bin"], len(message), message[:60].hex())

        parsed, method = _try_sfs_decode(message)
        ftype = "unknown"
        is_game = False

        if parsed and isinstance(parsed, dict):
            ftype, is_game = classify_frame(parsed)
            log.info("[%s] Frame: %s (game=%s) decoded=%s",
                     name, ftype, is_game, _sj(parsed)[:600])

        # ── v8.5: SFS2X Keepalive — responder ping del servidor con el mismo frame ──
        # El servidor envía c=7 cada ~60s. Sin pong → timeout → CLOSED → reconnecting
        if ftype == "ping":
            try:
                ws.send(message, opcode=websocket.ABNF.OPCODE_BINARY)
                log.debug("[%s] ↩ PONG (keepalive)", name)
            except Exception as e:
                log.warning("[%s] Pong send failed: %s", name, e)
            return  # nada más que hacer con este frame

        # ── Auth state machine ──
        step = auth_state["step"]

        if step == "wait_handshake_resp" and ftype == "handshake_resp":
            auth_state["handshake_done"] = True
            auth_state["step"] = "wait_login_resp"
            send_delayed(ws, msg2, 2, "Login")

        # ── v8.3: SERVER DISCONNECT (sfs_c0_a1005 / dr=X) ──
        elif "server_disconnect" in ftype:
            dr = parsed.get("p", {}).get("dr", -1) if parsed else -1
            if dr == 1:
                # dr=1 = sesión kickeada por duplicado — ya hay una nueva gen activa, no reconectar
                log.warning("[%s] ⚡ SERVER DISCONNECT dr=1 — sesión reemplazada por gen más nuevo. "
                            "Cancelando reconexión de gen=%d.", name, gen)
                ws_generation[bm_id] = gen + 1
            else:
                # dr=0 u otros = cierre normal del servidor (timeout, mantenimiento, etc.)
                # Dejar que on_close maneje la reconexión normalmente
                log.info("[%s] SERVER DISCONNECT dr=%s — cierre normal, reconectará.", name, dr)
            return

        # ── v8.2/v8.8: LOGIN RECHAZADO ──
        elif ftype == "login_error":
            ec = parsed.get("p", {}).get("ec", "?") if parsed else "?"
            retries = ws_login_retries.get(bm_id, 0)
            # Reintentar hasta 3 veces — ec=28 puede ser duplicado transitorio (no ban real)
            # Backoff: intento 1→5s, 2→15s, 3→60s, 4+→error permanente
            if retries < 3:
                delays = [5, 15, 60]
                delay = delays[retries]
                ws_login_retries[bm_id] = retries + 1
                log.warning("[%s] ✗ LOGIN ec=%s (intento %d/3) — reintentando en %ds "
                            "(puede ser sesión duplicada transitoria)",
                            name, ec, retries + 1, delay)
                # NO incrementar ws_generation aquí — _start_ws ya lo incrementa internamente.
                # Incrementarlo dos veces dejaba la vieja conexión "huérfana" sin cerrar.
                ws.close()
                def _retry_login():
                    time.sleep(delay)
                    if bm_id in bookmakers and bookmakers[bm_id].get("active", False) and \
                       connection_status.get(bm_id) != "error":
                        _start_ws(bm_id)
                threading.Thread(target=_retry_login, daemon=True).start()
            else:
                # 3 intentos fallidos → ban real o token expirado
                ws_login_retries[bm_id] = 0
                log.error("[%s] ✗ LOGIN RECHAZADO definitivo (ec=%s) tras 3 intentos — "
                          "token expirado / ban real. Actualiza msg2_b64 en el admin.", name, ec)
                connection_status[bm_id] = "error"
                ws_generation[bm_id] = gen + 1
                ws.close()
            return

        # ── v8.4: LOGIN EXITOSO (c=0,a=1,rs=0) → avanzar auth inmediatamente ──
        elif step == "wait_login_resp" and ftype == "login_resp_ok":
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            send_delayed(ws, msg3, 3, "Extension:currentBets")

        elif step == "wait_login_resp" and ftype in ("login_resp", "room_join"):
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            send_delayed(ws, msg3, 3, "Extension:currentBets")
            # TAMBIÉN extraer de este frame
            if is_game and parsed:
                mults, rid = extract_game_mults(parsed, name)
                for m in mults:
                    _record_crash(bm_id, name, m, rid)

        elif step == "wait_login_resp" and "ext" in ftype:
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            if not auth_state["msg3_sent"]:
                auth_state["msg3_sent"] = True
                send_delayed(ws, msg3, 3, "Extension:currentBets")
            if is_game and parsed:
                mults, rid = extract_game_mults(parsed, name)
                for m in mults:
                    _record_crash(bm_id, name, m, rid)

        elif step == "wait_handshake_resp" and ftype != "handshake_resp":
            # No es handshake resp — intentar login de todas formas
            auth_state["handshake_done"] = True
            auth_state["step"] = "wait_login_resp"
            send_delayed(ws, msg2, 2, "Login")
            # TAMBIÉN intentar extraer game data
            if is_game and parsed:
                mults, rid = extract_game_mults(parsed, name)
                for m in mults:
                    _record_crash(bm_id, name, m, rid)

        # ── v8.4: Frame init — recuperar crashes perdidos durante reconexión ──
        elif "init" in ftype and parsed:
            inner_p = parsed.get("p", {}).get("p", {}) if isinstance(parsed.get("p"), dict) else {}
            rounds_info = inner_p.get("roundsInfo", []) if isinstance(inner_p, dict) else []
            if rounds_info:
                bm_crashes_list = crashes.get(bm_id, [])
                known_ids = {cr.get("round_id") for cr in bm_crashes_list if cr.get("round_id")}
                # Ordenar por roundId asc para preservar orden cronológico
                sorted_rounds = sorted(
                    [r for r in rounds_info if isinstance(r, dict)],
                    key=lambda x: x.get("roundId", 0)
                )
                recovered = 0
                for r in sorted_rounds:
                    rid  = r.get("roundId")
                    mult = _parse_mult(r.get("maxMultiplier"))
                    if mult and rid and rid not in known_ids:
                        _record_crash(bm_id, name, mult, rid)
                        known_ids.add(rid)
                        recovered += 1
                if recovered:
                    log.info("[%s] ↺ Recuperados %d crashes perdidos del frame init", name, recovered)

        # ── Post-auth: captura normal ──
        elif auth_state["step"] == "authenticated" and is_game:
            mults, rid = extract_game_mults(parsed, name)
            for m in mults:
                _record_crash(bm_id, name, m, rid)

        # ── Frame 0 capture: si aún no estamos authenticated pero es game data ──
        elif is_game and parsed:
            mults, rid = extract_game_mults(parsed, name)
            for m in mults:
                _record_crash(bm_id, name, m, rid)

        _store_dbg(bm_id, {
            "time": datetime.now(timezone.utc).isoformat(),
            "size": len(message), "type": "BIN",
            "hex": message[:100].hex(),
            "ascii": message[:100].decode("ascii", errors="replace"),
            "decoded": _sj(parsed)[:1500] if parsed else None,
            "method": f"sfs2x/{method}" if method else None,
            "frame_type": ftype,
            "mults": [m for m in (mults if 'mults' in dir() and isinstance(mults, list) else [])],
        })

    def on_error(ws, error):
        log.error("[%s] ✗ ERROR gen=%d: %s", name, gen, error)

    def on_close(ws, code, msg):
        total_recv = stats["recv_bin"] + stats["recv_txt"]
        log.info("[%s] CLOSED gen=%d code=%s | sent=%d recv=%d",
                 name, gen, code, stats["sent"], total_recv)
        # v8.3: limpiar de la lista de zombies
        try:
            lst = ws_all_connections.get(bm_id, [])
            if ws in lst: lst.remove(ws)
        except: pass
        if ws_generation.get(bm_id) != gen:
            return
        # No reconectar si el login fue rechazado (credenciales malas)
        if connection_status.get(bm_id) == "error":
            log.warning("[%s] ⛔ No reconectando — login rechazado. "
                        "Actualiza las credenciales (msg2_b64) en el admin.", name)
            return
        # v8.9: No reconectar si aún estamos en "connecting" — significa que
        # login_error ya programó un _retry_login. Si on_close también reconecta,
        # se generan dos conexiones paralelas que provocan ec=28 perpetuo.
        if connection_status.get(bm_id) == "connecting":
            log.info("[%s] on_close: status=connecting — reintento ya programado por login_error, omitiendo reconexión.", name)
            return
        if bm_id in bookmakers and bookmakers[bm_id].get("active", False):
            # ── v8.2: Backoff exponencial según intentos fallidos ──
            attempts = ws_reconnect_attempts.get(bm_id, 0) + 1
            ws_reconnect_attempts[bm_id] = attempts

            if total_recv == 0:
                # Nunca recibió nada → posible bloqueo de IP o WS URL incorrecta
                delay = min(15 * (2 ** (attempts - 1)), 300)  # 15, 30, 60, 120, 300s
                log.warning("[%s] Sin datos recibidos (intento #%d) — delay=%ds",
                            name, attempts, delay)
            elif total_recv < 10:
                # Recibió muy poco → conexión inestable
                delay = min(10 * (2 ** min(attempts - 1, 3)), 80)  # 10, 20, 40, 80s
            else:
                # Conexión estaba bien (cierre normal del servidor / dr=0)
                # v8.6: delay reducido 8s → 2s para minimizar pausa visible
                ws_reconnect_attempts[bm_id] = 0   # reset: no fue un fallo
                delay = 2

            connection_status[bm_id] = "reconnecting"
            log.info("[%s] Reconnect in %ds (intento #%d)...", name, delay, attempts)
            def _reconn():
                time.sleep(delay)
                if ws_generation.get(bm_id) == gen and \
                   bm_id in bookmakers and bookmakers[bm_id].get("active", False) and \
                   connection_status.get(bm_id) != "error":
                    _start_ws(bm_id)
            threading.Thread(target=_reconn, daemon=True).start()
        else:
            connection_status[bm_id] = "disconnected"

    # Headers
    try:
        from urllib.parse import urlparse
        pu = urlparse(ws_url)
        origin = f"{pu.scheme.replace('wss','https').replace('ws','http')}://{pu.netloc}"
    except:
        origin = "https://spribegaming.com"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Origin": origin,
        "Host": ws_url.split("/")[2] if "//" in ws_url else ws_url,
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    import ssl as _ssl
    ssl_opts = {"cert_reqs": _ssl.CERT_NONE, "check_hostname": False}

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close,
        header=headers,
    )
    ws_connections[bm_id] = ws_app
    ws_all_connections.setdefault(bm_id, []).append(ws_app)   # v8.3: zombie tracking
    t = threading.Thread(
        target=ws_app.run_forever,
        # NO enviar pings WebSocket — SFS2X/Spribe no responde pongs
        # y websocket-client cierra la conexión al expirar ping_timeout.
        # SFS2X tiene su propio keepalive interno.
        kwargs={"ping_interval": 0, "ping_timeout": None, "sslopt": ssl_opts},
        daemon=True,
    )
    t.start()
    ws_threads[bm_id] = t


def _record_crash(bm_id, bm_name, multiplier, round_id=None):
    """Registra crash con dedup, preservando orden cronológico."""
    now = datetime.now(timezone.utc)
    mult = round(float(multiplier), 2)

    # v8.12: lock global para que dos hilos WS no pasen el dedup simultáneamente
    with _crashes_lock:
        bm_crashes = crashes[bm_id]

        if round_id is not None:
            for prev in reversed(bm_crashes[-50:]):
                if prev.get("round_id") == round_id: return
        if round_id is not None:
            for prev in reversed(bm_crashes[-5:]):
                try:
                    age = (now - datetime.fromisoformat(prev["timestamp"])).total_seconds()
                    if age > 15: break
                    if round(float(prev["multiplier"]), 2) == mult and not prev.get("round_id"):
                        prev["round_id"] = round_id; return
                except: pass
        for prev in reversed(bm_crashes[-10:]):
            try:
                age = (now - datetime.fromisoformat(prev["timestamp"])).total_seconds()
                if age > 15: break
                if round(float(prev["multiplier"]), 2) == mult: return
            except: pass

        entry = {"multiplier": mult, "timestamp": now.isoformat(), "id": str(uuid.uuid4())[:8]}
        if round_id is not None: entry["round_id"] = round_id
        bm_crashes.append(entry)
        if len(bm_crashes) > MAX_CRASHES:
            crashes[bm_id] = bm_crashes[-MAX_CRASHES:]
        for q in sse_subscribers.get(bm_id, []):
            try: q.put_nowait(entry)
            except: pass
        log.info("[%s] ★★★ CRASH: %.2fx round=%s total=%d ★★★",
                 bm_name, mult, round_id, len(crashes[bm_id]))
        # v8.5/v8.6: marcar pendiente (thread-safe Event) en vez de thread por crash
        _save_event.set()


def _stop_ws(bm_id):
    ws_generation[bm_id] = ws_generation.get(bm_id, 0) + 1
    ws = ws_connections.pop(bm_id, None)
    if ws:
        try: ws.close()
        except: pass
    # v8.3: matar zombies también
    for zombie in ws_all_connections.pop(bm_id, []):
        try: zombie.close()
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
        bm_c = crashes.get(bid, [])
        item["crash_count"] = len(bm_c)
        item["last_crash_at"] = bm_c[-1]["timestamp"] if bm_c else None
        r.append(item)
    return jsonify(r)

@app.route("/api/aviator/bookmakers", methods=["POST"])
def create_bm():
    global _bm_counter
    d = request.get_json(force=True)
    bid = str(uuid.uuid4())[:12]
    _bm_counter += 1; nid = _bm_counter; _num_to_bid[nid] = bid
    bm = {
        "num_id": nid, "name": d.get("name", "Sin Nombre"),
        "description": d.get("description", ""),
        "image_url": d.get("image_url", ""),
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
    for k in ("name","description","image_url","ws_url","msg1_b64","msg2_b64","msg3_b64",
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
    crashes.pop(bid, None); debug_frames.pop(bid, None)
    _save_config(); _save_crashes()
    return jsonify({"deleted": bid})

@app.route("/api/aviator/bookmakers/<bid>/crashes", methods=["GET"])
def get_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    limit = request.args.get("limit", 100, type=int)
    return jsonify(crashes.get(bid, [])[-limit:])

@app.route("/api/aviator/rounds/<int:num_id>", methods=["GET"])
def get_rounds_by_num(num_id):
    bid = _num_to_bid.get(num_id)
    if not bid or bid not in bookmakers:
        return jsonify({"error": f"Bookmaker #{num_id} not found"}), 404
    limit = request.args.get("limit", 100, type=int)
    bm = bookmakers[bid]; data = crashes.get(bid, [])[-limit:]
    return jsonify({
        "num_id": num_id, "id": bid, "name": bm.get("name", bid),
        "description": bm.get("description", ""),
        "image_url": bm.get("image_url", ""),
        "connection_status": connection_status.get(bid, "disconnected"),
        "crash_count": len(crashes.get(bid, [])), "crashes": data,
    })

@app.route("/api/aviator/bookmakers/<bid>/crashes/stream")
def stream_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    q = Queue(maxsize=200); sse_subscribers[bid].append(q)
    initial = crashes.get(bid, [])[-50:]
    def gen():
        try:
            yield f"event: init\ndata: {json.dumps(initial)}\n\n"
            while True:
                try:
                    e = q.get(timeout=5)
                    yield f"data: {json.dumps(e)}\n\n"
                except Empty:
                    yield ": hb\n\n"
        except GeneratorExit: pass
        finally:
            try: sse_subscribers[bid].remove(q)
            except: pass
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no",
                             "Connection":"keep-alive"})

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
        "active_connections": sum(1 for s in connection_status.values()
                                  if s in ("connected","connecting","reconnecting")),
        "total_crashes": sum(len(v) for v in crashes.values()),
    })

@app.route("/api/aviator/bookmakers/<bid>/ingest", methods=["POST"])
def ingest_crash(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    d = request.get_json(force=True); mult = d.get("multiplier")
    if not isinstance(mult, (int, float)) or not (1.00 <= float(mult) <= 9999.99):
        return jsonify({"error": "Invalid multiplier"}), 400
    _record_crash(bid, bookmakers[bid].get("name", bid), float(mult))
    return jsonify({"ok": True, "multiplier": mult}), 201

@app.route("/api/aviator/bookmakers/<bid>/clear-crashes", methods=["POST"])
def clear_crashes_endpoint(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    count = len(crashes.get(bid, [])); crashes[bid] = []; _save_crashes()
    return jsonify({"ok": True, "cleared": count})

@app.route("/api/clear-all-crashes", methods=["POST"])
def clear_all_crashes():
    total = sum(len(v) for v in crashes.values())
    for k in list(crashes.keys()): crashes[k] = []
    _save_crashes()
    return jsonify({"ok": True, "cleared": total})

@app.route("/api/export-config")
def export_config():
    return jsonify({"bookmakers": bookmakers, "crashes": {}})

# ---------------------------------------------------------------------------
_load_config()

def _autostart():
    time.sleep(2)
    for bid, bm in list(bookmakers.items()):
        if bm.get("active", False):
            log.info("Auto-start: %s", bm.get("name", bid))
            _start_ws(bid); time.sleep(1)
threading.Thread(target=_autostart, daemon=True).start()

def _watchdog():
    """Safety net: solo reconecta bookmakers que están muertos (error/disconnected).
    NO toca 'reconnecting' — on_close ya maneja eso."""
    time.sleep(45)
    while True:
        try:
            for bid, bm in list(bookmakers.items()):
                if not bm.get("active", False): continue
                status = connection_status.get(bid, "disconnected")
                # "error" = credenciales rechazadas — NO reintentar automáticamente
                if status == "error":
                    continue
                # Solo actuar si completamente muerto
                if status in ("disconnected",):
                    log.info("[watchdog] %s dead (%s) — restarting", bm.get("name", bid), status)
                    _start_ws(bid)
                    time.sleep(5)
        except Exception as e:
            log.error("[watchdog] %s", e)
        time.sleep(60)  # Cada 60s — no competir con on_close
threading.Thread(target=_watchdog, daemon=True).start()

def _periodic_save():
    while True:
        # Espera hasta que haya cambios (o máx 30s para forzar guardado periódico)
        _save_event.wait(timeout=30)
        if _save_event.is_set():
            try:
                _save_crashes()
            except: pass
            _save_event.clear()
threading.Thread(target=_periodic_save, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
