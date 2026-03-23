"""
PREDICTORES TATAN — Aviator Bookmaker Scraping Manager
v6 — Fixes:
  1. _load_config lee BOOKMAKERS_JSON desde env var (Render ephemeral disk fix)
  2. /health retorna active_connections y total_crashes (match con admin.html JS)
  3. Nuevo endpoint GET /api/export-config para exportar config lista para Render
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
# Disco persistente de Render (/data) si existe, si no directorio local
_DATA_DIR   = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
CONFIG_PATH  = os.path.join(_DATA_DIR, "config.json")
CRASHES_PATH = os.path.join(_DATA_DIR, "crashes.json")
bookmakers: dict = {}
crashes: dict = defaultdict(list)
ws_connections: dict = {}
ws_threads: dict = {}
sse_subscribers: dict = defaultdict(list)
connection_status: dict = {}
debug_frames: dict = defaultdict(list)
MAX_CRASHES = 1000
MAX_DBG = 200

# Contador numérico para IDs tipo /rounds/1, /rounds/2 — igual que aviator-szte
_bm_counter: int = 0
# Mapa num_id (int) → bid (uuid string) para lookup en /rounds/:num_id
_num_to_bid: dict = {}

def _load_config():
    global bookmakers, crashes

    # 1. Prioridad: variable de entorno BOOKMAKERS_JSON (Render — disco efímero)
    env_json = os.environ.get("BOOKMAKERS_JSON", "").strip()
    if env_json:
        try:
            d = json.loads(env_json)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from BOOKMAKERS_JSON env var", len(bookmakers))
            return
        except Exception as e:
            log.error("Failed to load from BOOKMAKERS_JSON env var: %s", e)

    # 1b. Cargar crashes desde crashes.json si existe
    if os.path.exists(CRASHES_PATH):
        try:
            with open(CRASHES_PATH) as f:
                saved = json.load(f)
            for k, v in saved.items():
                crashes[k] = v
            log.info("Loaded crashes from %s (%d bookmakers)", CRASHES_PATH, len(saved))
        except Exception as e:
            log.error("crashes load fail: %s", e)

    # 2. Fallback: archivo local config.json (desarrollo local / primer deploy)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                d = json.load(f)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from config.json", len(bookmakers))
        except Exception as e:
            log.error("Load fail: %s", e)
    else:
        log.info("No config found — starting empty. Agrega bookmakers desde la UI.")

def _save_config():
    """
    Guarda en disco local.
    NOTA: En Render el disco es efímero — los datos se pierden al redesplegar.
    Usa GET /api/export-config y copia el JSON como BOOKMAKERS_JSON en Render.
    """
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"bookmakers": bookmakers, "crashes": dict(crashes)},
                      f, indent=2, default=str)
    except Exception as e:
        log.error("Save fail: %s", e)

def _save_crashes():
    """Guarda crashes en disco persistente."""
    try:
        slim = {k: v[-30:] for k, v in crashes.items()}
        with open(CRASHES_PATH, "w") as f:
            json.dump(slim, f, default=str)
    except Exception as e:
        log.error("crashes save fail: %s", e)

def _rebuild_num_index():
    """
    Reconstruye _num_to_bid y _bm_counter a partir de los bookmakers cargados.
    Cada bookmaker tiene un campo 'num_id' (entero) asignado al crearlo.
    Si no lo tiene (bookmakers viejos), se asigna uno nuevo.
    """
    global _bm_counter, _num_to_bid
    _num_to_bid = {}
    max_num = 0
    for bid, bm in bookmakers.items():
        nid = bm.get("num_id")
        if isinstance(nid, int):
            _num_to_bid[nid] = bid
            if nid > max_num:
                max_num = nid
    # Asignar num_id a bookmakers que no lo tengan (compatibilidad hacia atrás)
    for bid, bm in bookmakers.items():
        if not isinstance(bm.get("num_id"), int):
            max_num += 1
            bm["num_id"] = max_num
            _num_to_bid[max_num] = bid
    _bm_counter = max_num
    log.info("Num index rebuilt: %d bookmakers, max_id=%d", len(_num_to_bid), _bm_counter)

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

    # --- Header 0x80: SFS2X standard frame ---
    if raw and raw[0] == 0x80:
        off = 1
        if len(raw) > off + 1:
            if raw[off] & 0x40: off += 4
            else: off += 2
            payloads.append(("sfs_hdr", raw[off:]))
            try: payloads.append(("sfs_hdr_zlib", zlib.decompress(raw[off:])))
            except: pass

    # --- Header 0xa0: Spribe compressed frame (crash/state data) ---
    # Format: a0 XX YY [zlib data starting with 78 9c ...]
    # Seen in logs as: a00330789c..., a00411789c..., a0044a789c...
    if raw and raw[0] == 0xa0 and len(raw) > 3:
        # Try skipping 3-byte header (a0 + 2 size bytes) → zlib payload
        candidate = raw[3:]
        try:
            payloads.append(("a0_hdr_zlib", zlib.decompress(candidate)))
        except: pass
        # Also try 4-byte header in case size is 3 bytes
        if len(raw) > 4:
            candidate4 = raw[4:]
            try:
                payloads.append(("a0_hdr4_zlib", zlib.decompress(candidate4)))
            except: pass

    # --- Generic zlib attempt ---
    try: payloads.append(("zlib", zlib.decompress(raw)))
    except: pass

    # --- Find zlib magic bytes (78 9c or 78 da) anywhere in frame ---
    for i in range(1, min(16, len(raw) - 2)):
        if raw[i] == 0x78 and raw[i+1] in (0x9c, 0xda, 0x01, 0x5e):
            try:
                payloads.append((f"zlib_at{i}", zlib.decompress(raw[i:])))
            except: pass

    # --- Byte skips (small headers) ---
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
# ---------------------------------------------------------------------------

PROTOCOL_KEYS = {
    "ct", "ms", "tk", "aph",
    "zn", "un", "pw",
    "rn", "ri", "rc", "uc", "id",
    "xt", "pi",
    # cashout list keys — multiplier inside these is a player cashout, NOT a crash
    "cashouts", "openBetsCount", "winAmount", "betId", "player_id",
}

GAME_KEYS = {
    "odd", "odds", "crash", "result",
    "coefficient", "crashValue", "endK", "bust",
    "coef", "coeff",
    "crashMultiplier", "finalMultiplier", "roundResult",
    "maxMultiplier", # Spribe: roundChartInfo confirma crash value + roundId
    "crashX",        # Spribe: ext:x con crashX = crash INSTANTÁNEO (captura inmediata)
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
    "roundChartInfo",      # Spribe crash result (maxMultiplier = crash value)
}

TICK_COMMANDS = {
    "x", "X",          # ticks en vuelo — ignorar siempre
    "tick", "update",
    "updateCurrentCashOuts", "currentCashOuts", "cashouts",
    "B", "T", "S",
}

STATE_COMMANDS = {
    "currentBetsInfoHandler",
    "gameStateHandler",
    "statsHandler",
    "S", "T", "B", "C",
}


def classify_frame(obj):
    if not isinstance(obj, dict):
        return ("unknown", False)

    c = obj.get("c")
    a = obj.get("a")
    p = obj.get("p", {})

    if c == 0 and a == 0:
        return ("handshake_req", False)
    if c == 0 and isinstance(p, dict) and ("ct" in p or "ms" in p or "tk" in p):
        return ("handshake_resp", False)
    if c == 0 and a == 1:
        return ("login_req", False)
    if c == 1 and a == 0:
        return ("login_resp", False)
    if c == 4:
        return ("room_join", False)

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

    if c == 7:
        return ("ping", False)

    return (f"sfs_c{c}_a{a}", False)


# ---------------------------------------------------------------------------
# Multiplier extraction
# ---------------------------------------------------------------------------

def extract_game_mults(obj, label=""):
    """
    Retorna (list_of_multipliers, round_id_or_None).
    round_id viene de roundChartInfo.roundId — clave de dedup definitiva.
    """
    if not isinstance(obj, dict):
        return [], None

    p = obj.get("p")
    if not isinstance(p, dict):
        return [], None

    inner_p = p.get("p")
    cmd = p.get("c", "")

    # Filtrar ticks en vuelo (x subiendo: 1.01x, 1.02x...)
    # EXCEPCIÓN: ext:x con "crashX" = el avión acaba de caer → capturar INMEDIATAMENTE
    if isinstance(cmd, str) and cmd.lower() in {c.lower() for c in TICK_COMMANDS}:
        has_crash_x = isinstance(inner_p, dict) and "crashX" in inner_p
        if not has_crash_x:
            return [], None
        # tiene crashX → es el crash real, continuar

    # Extraer roundId si está disponible (viene en roundChartInfo o en ext:x con crashX)
    round_id = None
    if isinstance(inner_p, dict):
        round_id = inner_p.get("roundId")

    results = []

    if isinstance(inner_p, dict):
        results.extend(_scan_game_obj(inner_p, label, cmd))

    for subkey in ("r", "data", "result", "res", "round", "game", "info"):
        sub = p.get(subkey) if isinstance(p, dict) else None
        if isinstance(sub, dict):
            results.extend(_scan_game_obj(sub, label, cmd))
        elif isinstance(sub, list):
            for item in sub:
                if isinstance(item, dict):
                    results.extend(_scan_game_obj(item, label, cmd))

    if not results:
        results.extend(_scan_game_obj(p, label, cmd))

    if not results:
        results.extend(_scan_game_obj(obj, label, cmd))

    return results, round_id


def _scan_game_obj(obj, label="", cmd="", depth=0):
    if depth > 8 or not isinstance(obj, dict):
        return []

    results = []
    for key, val in obj.items():
        if key in PROTOCOL_KEYS:
            continue

        if key in GAME_KEYS:
            X100_INT_KEYS = {"odd", "odds", "k", "coef", "coeff",
                             "crashValue", "endK", "crashMultiplier",
                             "finalMultiplier", "coefficient"}
            # crashX and maxMultiplier are already floats (e.g. 1.0 = 1.00x) — never divide
            if key in X100_INT_KEYS and isinstance(val, int):
                if val < 100:
                    continue
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
                    if key in GAME_KEYS:
                        m = _parse_mult(item)
                        if m is not None:
                            results.append(m)

    return results


def _parse_mult(val):
    if not isinstance(val, (int, float)):
        if isinstance(val, str):
            try: val = float(val)
            except: return None
        else:
            return None

    fv = float(val)
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
            else:
                log.debug("[%s] Skipping non-game frame: %s", label, ftype)

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
                        mults, _ = extract_game_mults(obj, label)
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
                mults, _ = extract_game_mults(obj, label)
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
                        ms, _ = extract_game_mults(obj, label)
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
# WebSocket Client
# ---------------------------------------------------------------------------
def _b64_to_bytes(s):
    if not s or not s.strip(): return b""
    try: return base64.b64decode(s)
    except: return b""

# Generación activa por bookmaker — evita doble-registro cuando hay reconexión
ws_generation: dict = {}

def _start_ws(bm_id):
    bm = bookmakers.get(bm_id)
    if not bm: return
    ws_url = bm.get("ws_url", "").strip()
    if not ws_url:
        connection_status[bm_id] = "error"; return

    # PRIMERO incrementar generación — así el hilo viejo queda obsoleto
    # antes de que reciba el close y trate de pisar el estado
    gen = ws_generation.get(bm_id, 0) + 1
    ws_generation[bm_id] = gen

    # Ahora sí cerrar la conexión anterior — si dispara on_close,
    # ese hilo verá gen desactualizado y no tocará nada
    old_ws = ws_connections.pop(bm_id, None)
    if old_ws:
        try: old_ws.close()
        except: pass

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
        send_msg(ws, msg1, 1, "Handshake")
        auth_state["step"] = "wait_handshake_resp"

    def on_message(ws, message):
        if isinstance(message, str):
            stats["recv_txt"] += 1
            log.info("[%s] ← TXT #%d (%d chars): %s",
                     name, stats["recv_txt"], len(message), message[:500])
            mults = process_text(message, bm_id, name)
            for m in mults:
                _record_crash(bm_id, name, m)  # text frames sin roundId
            return

        stats["recv_bin"] += 1
        log.info("[%s] ← BIN #%d (%d bytes): %s",
                 name, stats["recv_bin"], len(message), message[:80].hex())

        parsed, method = _try_sfs_decode(message)
        ftype = "unknown"
        is_game = False

        if parsed and isinstance(parsed, dict):
            ftype, is_game = classify_frame(parsed)
            log.info("[%s] Frame: %s (game=%s) decoded=%s",
                     name, ftype, is_game, _sj(parsed)[:600])

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

        step = auth_state["step"]

        if step == "wait_handshake_resp" and ftype == "handshake_resp":
            log.info("[%s] ✓ Handshake response received. Sending Login (msg2)...", name)
            auth_state["handshake_done"] = True
            auth_state["step"] = "wait_login_resp"
            time.sleep(0.3)
            send_msg(ws, msg2, 2, "Login")

        elif step == "wait_login_resp" and ftype in ("login_resp", "room_join"):
            log.info("[%s] ✓ Login/Room response received. Sending Extension (msg3)...", name)
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            time.sleep(0.3)
            send_msg(ws, msg3, 3, "Extension:currentBets")
            if is_game and parsed:
                mults, rid = extract_game_mults(parsed, name)
                dbg["mults"] = mults
                for m in mults:
                    _record_crash(bm_id, name, m, rid)

        elif step == "wait_login_resp" and "ext" in ftype:
            log.info("[%s] ✓ Got extension before explicit login resp, auth seems ok", name)
            auth_state["login_done"] = True
            auth_state["step"] = "authenticated"
            if not auth_state.get("msg3_sent"):
                auth_state["msg3_sent"] = True
                time.sleep(0.3)
                send_msg(ws, msg3, 3, "Extension:currentBets")
            if is_game:
                mults, rid = extract_game_mults(parsed, name)
                dbg["mults"] = mults
                for m in mults:
                    _record_crash(bm_id, name, m, rid)

        elif auth_state["step"] == "authenticated" and is_game:
            # Verificar que este hilo sigue siendo el activo antes de registrar
            if ws_generation.get(bm_id) != gen:
                log.debug("[%s] Stale thread (gen %d != %d), discarding frame", name, gen, ws_generation.get(bm_id))
                return
            mults, rid = extract_game_mults(parsed, name)
            dbg["mults"] = mults
            for m in mults:
                _record_crash(bm_id, name, m, rid)

        elif auth_state["step"] == "authenticated":
            # Frame llegó pero no fue clasificado como game data.
            # Loguearlo para identificar el comando real de crash del servidor.
            if parsed:
                log.info("[%s] UNKNOWN post-auth frame: ftype=%s decoded=%s",
                         name, ftype, _sj(parsed)[:800])
                # Intentar extracción directa — puede ser game data con estructura no estándar
                mults, rid = extract_game_mults(parsed, name)
                if not mults:
                    mults = _scan_game_obj(parsed, name, "", 0)
                dbg["mults"] = mults
                for m in mults:
                    log.info("[%s] ★ CRASH from unknown frame: %.2fx", name, m)
                    _record_crash(bm_id, name, m, rid)
            else:
                log.info("[%s] Undecoded post-auth frame: hex=%s",
                         name, message[:30].hex() if isinstance(message, bytes) else "txt")

        else:
            if not auth_state["handshake_done"] and ftype != "handshake_resp":
                log.info("[%s] Non-handshake frame during handshake phase: %s", name, ftype)
                if msg2:
                    log.info("[%s] Attempting Login anyway...", name)
                    auth_state["handshake_done"] = True
                    auth_state["step"] = "wait_login_resp"
                    time.sleep(0.3)
                    send_msg(ws, msg2, 2, "Login")

        _store_dbg(bm_id, dbg)

    def on_error(ws, error):
        err_str = str(error)
        log.error("[%s] ✗ ERROR: %s", name, err_str)
        # Diagnóstico detallado según tipo de error
        if "Connection refused" in err_str:
            log.error("[%s] ▶ DIAGNÓSTICO: Servidor rechazó la conexión. Verifica la URL wss://", name)
        elif "timed out" in err_str.lower() or "timeout" in err_str.lower():
            log.error("[%s] ▶ DIAGNÓSTICO: Timeout — servidor no responde. Posible IP bloqueada en Render.", name)
        elif "SSL" in err_str or "certificate" in err_str.lower():
            log.error("[%s] ▶ DIAGNÓSTICO: Error SSL. Intentando reconectar con ssl_opt desactivado.", name)
        elif "403" in err_str or "Forbidden" in err_str:
            log.error("[%s] ▶ DIAGNÓSTICO: HTTP 403 — servidor rechaza los headers. Verifica Origin/Host.", name)
        elif "101" not in err_str and "handshake" in err_str.lower():
            log.error("[%s] ▶ DIAGNÓSTICO: Handshake WS fallido. El servidor puede requerir subprotocolo específico.", name)
        # NO pisar connection_status aquí — on_close lo maneja correctamente.
        # on_error siempre va seguido de on_close, así que el estado se actualiza ahí.

    def on_close(ws, code, msg):
        log.info("[%s] CLOSED code=%s msg=%s | handshake=%s login=%s step=%s | sent=%d bin=%d txt=%d",
                 name, code, msg,
                 auth_state["handshake_done"], auth_state["login_done"], auth_state["step"],
                 stats["sent"], stats["recv_bin"], stats["recv_txt"])
        # Diagnóstico según código de cierre
        if code == 1006:
            log.error("[%s] ▶ DIAGNÓSTICO código 1006: Cierre anormal — sin respuesta del servidor. "
                      "Posibles causas: URL incorrecta, IP bloqueada, servidor caído, headers faltantes.", name)
        elif code == 1008:
            log.error("[%s] ▶ DIAGNÓSTICO código 1008: Policy Violation — el servidor rechazó por policy. "
                      "Verifica que los mensajes Base64 sean correctos.", name)
        elif code == 1003:
            log.error("[%s] ▶ DIAGNÓSTICO código 1003: Tipo de dato no aceptado por el servidor.", name)
        elif code is None and stats["recv_bin"] == 0 and stats["recv_txt"] == 0:
            log.error("[%s] ▶ DIAGNÓSTICO: Cerró sin recibir NINGÚN frame. "
                      "El servidor no completó el handshake WebSocket. "
                      "Revisa: 1) URL wss:// correcta  2) Headers Origin/Host  3) msg1 Base64 válido.", name)
        # Ignorar si este hilo ya está obsoleto
        if ws_generation.get(bm_id) != gen:
            log.debug("[%s] on_close: hilo obsoleto gen=%d, ignorando", name, gen)
            return
        if bm_id in bookmakers and bookmakers[bm_id].get("active", False):
            connection_status[bm_id] = "connecting"
            log.info("[%s] Reconnect en 5s...", name)
            def _reconnect():
                time.sleep(5)
                if ws_generation.get(bm_id) == gen and bm_id in bookmakers and bookmakers[bm_id].get("active", False):
                    _start_ws(bm_id)
            threading.Thread(target=_reconnect, daemon=True).start()
        else:
            connection_status[bm_id] = "disconnected"

    # Headers de navegador real — evita que el servidor rechace la conexión por bot detection
    try:
        from urllib.parse import urlparse
        parsed = urlparse(ws_url)
        origin_host = f"{parsed.scheme.replace('wss','https').replace('ws','http')}://{parsed.netloc}"
    except Exception:
        origin_host = "https://spribegaming.com"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Origin": origin_host,
        "Host": ws_url.split("/")[2] if "//" in ws_url else ws_url,
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    log.info("[%s] Headers WebSocket: Origin=%s Host=%s", name, headers["Origin"], headers["Host"])

    import ssl as _ssl
    ssl_opts = {"cert_reqs": _ssl.CERT_NONE, "check_hostname": False}

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close,
        header=headers,
    )
    ws_connections[bm_id] = ws_app
    t = threading.Thread(
        target=ws_app.run_forever,
        kwargs={"ping_interval": 20, "ping_timeout": 10, "sslopt": ssl_opts},
        daemon=True,
    )
    t.start()
    ws_threads[bm_id] = t


def _record_crash(bm_id, bm_name, multiplier, round_id=None):
    """
    Registra crash. Estrategia:
    - crashX (sin round_id): registra INMEDIATAMENTE para captura instantánea.
    - roundChartInfo (con round_id): si el mismo valor ya fue registrado en los
      últimos 10s → solo añade round_id al entry existente (sin duplicar ni
      enviar SSE de nuevo). Si no existe → registra normalmente.
    """
    now = datetime.now(timezone.utc)
    mult = round(float(multiplier), 2)

    # Dedup por round_id exacto (roundChartInfo ya registrado)
    if round_id is not None:
        for prev in reversed(crashes.get(bm_id, [])[-30:]):
            if prev.get("round_id") == round_id:
                log.debug("[%s] Dedup: roundId=%s ya existe", bm_name, round_id)
                return

    # Si llega roundChartInfo y ya hay un entry con el mismo valor en últimos 10s
    # (capturado vía crashX), solo actualizar el round_id sin duplicar
    if round_id is not None:
        for prev in reversed(crashes.get(bm_id, [])[-5:]):
            try:
                prev_t = datetime.fromisoformat(prev["timestamp"])
                if (now - prev_t).total_seconds() > 10:
                    break
                if round(float(prev["multiplier"]), 2) == mult and prev.get("round_id") is None:
                    prev["round_id"] = round_id
                    log.debug("[%s] Dedup: roundChartInfo %.2fx enriquece entry crashX", bm_name, mult)
                    return
            except:
                pass

    # Dedup final: mismo valor en últimos 2s solamente
    for prev in reversed(crashes.get(bm_id, [])[-3:]):
        try:
            prev_t = datetime.fromisoformat(prev["timestamp"])
            if (now - prev_t).total_seconds() > 2:
                break
            if round(float(prev["multiplier"]), 2) == mult:
                log.debug("[%s] Dedup: %.2fx ya registrado en <2s", bm_name, mult)
                return
        except:
            pass

    entry = {
        "multiplier": mult,
        "timestamp": now.isoformat(),
        "id": str(uuid.uuid4())[:8],
    }
    if round_id is not None:
        entry["round_id"] = round_id

    crashes[bm_id].append(entry)
    if len(crashes[bm_id]) > MAX_CRASHES:
        crashes[bm_id] = crashes[bm_id][-MAX_CRASHES:]
    for q in sse_subscribers.get(bm_id, []):
        try: q.put_nowait(entry)
        except: pass
    log.info("[%s] ★★★ CRASH RECORDED: %.2fx (round_id=%s) ★★★", bm_name, mult, round_id)
    # Guardar en disco de forma asíncrona para no bloquear SSE
    threading.Thread(target=_save_crashes, daemon=True).start()

def _stop_ws(bm_id):
    # Invalidar generación primero para neutralizar on_close/on_error del hilo actual
    ws_generation[bm_id] = ws_generation.get(bm_id, 0) + 1
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
        bm_crashes = crashes.get(bid, [])
        item["crash_count"] = len(bm_crashes)
        item["last_crash_at"] = bm_crashes[-1]["timestamp"] if bm_crashes else None
        r.append(item)
    return jsonify(r)

@app.route("/api/aviator/bookmakers", methods=["POST"])
def create_bm():
    global _bm_counter
    d = request.get_json(force=True)
    bid = str(uuid.uuid4())[:12]
    _bm_counter += 1
    nid = _bm_counter
    _num_to_bid[nid] = bid
    bm = {
        "num_id": nid,
        "name": d.get("name", "Sin Nombre"),
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
    crashes.pop(bid, None); debug_frames.pop(bid, None); _save_config()
    return jsonify({"deleted": bid})

@app.route("/api/aviator/bookmakers/<bid>/crashes", methods=["GET"])
def get_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    limit = request.args.get("limit", 100, type=int)
    return jsonify(crashes.get(bid, [])[-limit:])

@app.route("/api/aviator/rounds/<int:num_id>", methods=["GET"])
def get_rounds_by_num(num_id):
    """
    Endpoint estilo aviator-szte: GET /api/aviator/rounds/<num_id>?limit=10
    Permite acceder a crashes de un bookmaker por su ID numérico corto.
    Ejemplo: /api/aviator/rounds/2?limit=50
    """
    bid = _num_to_bid.get(num_id)
    if not bid or bid not in bookmakers:
        return jsonify({"error": f"Bookmaker #{num_id} not found"}), 404
    limit = request.args.get("limit", 100, type=int)
    bm = bookmakers[bid]
    data = crashes.get(bid, [])[-limit:]
    return jsonify({
        "num_id": num_id,
        "id": bid,
        "name": bm.get("name", bid),
        "description": bm.get("description", ""),
        "image_url": bm.get("image_url", ""),
        "connection_status": connection_status.get(bid, "disconnected"),
        "crash_count": len(crashes.get(bid, [])),
        "crashes": data,
    })

@app.route("/api/aviator/bookmakers/<bid>/crashes/stream")
def stream_crashes(bid):
    if bid not in bookmakers: return jsonify({"error": "Not found"}), 404
    q = Queue(maxsize=100); sse_subscribers[bid].append(q)
    # Enviar los últimos 30 crashes al conectar (para que el cliente tenga estado inicial)
    initial = crashes.get(bid, [])[-30:]
    def gen():
        try:
            # Enviar estado inicial al conectar
            yield f"event: init\ndata: {json.dumps(initial)}\n\n"
            while True:
                try:
                    e = q.get(timeout=5)   # 5s timeout → heartbeat frecuente
                    yield f"data: {json.dumps(e)}\n\n"
                except Empty:
                    yield ": hb\n\n"    # heartbeat cada 5s mantiene conexión viva
        except GeneratorExit: pass
        finally:
            try: sse_subscribers[bid].remove(q)
            except: pass
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no",
                             "Connection":"keep-alive","X-Content-Type-Options":"nosniff"})

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
    # NOTA: active_connections y total_crashes coinciden con el JS de admin.html
    return jsonify({
        "status": "ok",
        "bookmakers": len(bookmakers),
        "active_connections": sum(1 for s in connection_status.values() if s in ("connected", "connecting")),
        "total_crashes": sum(len(v) for v in crashes.values()),
    })

@app.route("/api/aviator/bookmakers/<bid>/ingest", methods=["POST"])
def ingest_crash(bid):
    """Endpoint para DOM scraper (Playwright)."""
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

@app.route("/api/export-config")
def export_config():
    """
    Exporta la config actual lista para copiar como BOOKMAKERS_JSON en Render.
    Úsala después de agregar bookmakers desde la UI:
      1. Ve a /api/export-config
      2. Copia el JSON
      3. En Render → Environment → BOOKMAKERS_JSON = <JSON copiado>
      4. Manual Deploy
    """
    export = {
        "bookmakers": bookmakers,
        "crashes": {}
    }
    return jsonify(export)

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
    """
    Revisa cada 20s si los bookmakers activos siguen conectados.
    Si encuentra status 'error' o 'disconnected', reconecta.
    Resuelve el problema de Render free tier que duerme el servicio.
    """
    time.sleep(15)  # esperar que el autostart termine
    while True:
        try:
            for bid, bm in list(bookmakers.items()):
                if not bm.get("active", False):
                    continue
                status = connection_status.get(bid, "disconnected")
                if status in ("error", "disconnected"):
                    log.info("[watchdog] %s está en '%s' — reconectando...", bm.get("name", bid), status)
                    _start_ws(bid)
                    time.sleep(2)  # evitar reconectar todos al mismo tiempo
        except Exception as e:
            log.error("[watchdog] Error: %s", e)
        time.sleep(20)

threading.Thread(target=_watchdog, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
