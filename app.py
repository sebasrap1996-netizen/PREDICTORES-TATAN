"""
PREDICTORES TATAN — Aviator Bookmaker Scraping Manager
v7.1 — Fixes:
  1. Captura desde frame 0: TODOS los frames se procesan para game data
     sin esperar handshake/login (los msgs de auth se envían en paralelo)
  2. Filtrado estricto de crashes reales vs cashouts/ticks/estado
  3. Persistencia: crashes se guardan en disco (crashes.json) tras cada nuevo crash
     y se restauran al reiniciar el servidor → NO se pierde historial
  4. Al reiniciar: carga crashes del disco y continúa capturando en tiempo real
  5. _load_config lee BOOKMAKERS_JSON desde env var (Render ephemeral disk fix)
  6. /health retorna active_connections y total_crashes
  7. GET /api/export-config para exportar config lista para Render
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
_DATA_DIR   = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
CONFIG_PATH  = os.path.join(_DATA_DIR, "config.json")
CRASHES_PATH = os.path.join(_DATA_DIR, "crashes.json")
bookmakers: dict = {}
crashes: dict = defaultdict(list)  # Inicia vacío — captura desde 0
ws_connections: dict = {}
ws_threads: dict = {}
sse_subscribers: dict = defaultdict(list)
connection_status: dict = {}
debug_frames: dict = defaultdict(list)
MAX_CRASHES = 1000
MAX_DBG = 200

_bm_counter: int = 0
_num_to_bid: dict = {}

def _load_config():
    """
    Carga bookmakers Y crashes del disco.
    Al reiniciar el servidor, restaura el historial guardado y continúa
    capturando en tiempo real desde el último multiplicador.
    """
    global bookmakers

    # 1. Prioridad: variable de entorno BOOKMAKERS_JSON (Render)
    env_json = os.environ.get("BOOKMAKERS_JSON", "").strip()
    if env_json:
        try:
            d = json.loads(env_json)
            bookmakers = d.get("bookmakers", {})
            # Cargar crashes del env si vienen (backup)
            for k, v in d.get("crashes", {}).items():
                crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from BOOKMAKERS_JSON env var", len(bookmakers))
        except Exception as e:
            log.error("Failed to load from BOOKMAKERS_JSON env var: %s", e)

    # 2. Fallback: archivo local config.json
    if not bookmakers and os.path.exists(CONFIG_PATH):
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

    # 3. Cargar crashes desde crashes.json (disco persistente — tiene prioridad)
    if os.path.exists(CRASHES_PATH):
        try:
            with open(CRASHES_PATH) as f:
                saved = json.load(f)
            for k, v in saved.items():
                # Merge: si ya hay crashes de config.json, los de crashes.json
                # tienen prioridad porque se guardan más frecuentemente
                crashes[k] = v
            total_restored = sum(len(v) for v in crashes.values())
            log.info("Restored crashes from %s → %d crashes across %d bookmakers",
                     CRASHES_PATH, total_restored, len(saved))
        except Exception as e:
            log.error("crashes.json load fail: %s", e)

    if not bookmakers:
        log.info("No config found — starting empty. Agrega bookmakers desde la UI.")
    else:
        total_c = sum(len(v) for v in crashes.values())
        log.info("★ READY: %d bookmakers, %d crashes restored — captura continúa en tiempo real ★",
                 len(bookmakers), total_c)

def _save_config():
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"bookmakers": bookmakers, "crashes": dict(crashes)},
                      f, indent=2, default=str)
    except Exception as e:
        log.error("Save fail: %s", e)

def _save_crashes():
    """Guarda crashes en disco persistente — se ejecuta tras cada crash nuevo."""
    try:
        # Guardar últimos 500 por bookmaker para tener historial al reiniciar
        slim = {k: v[-500:] for k, v in crashes.items() if v}
        with open(CRASHES_PATH, "w") as f:
            json.dump(slim, f, default=str)
    except Exception as e:
        log.error("crashes save fail: %s", e)

def _rebuild_num_index():
    global _bm_counter, _num_to_bid
    _num_to_bid = {}
    max_num = 0
    for bid, bm in bookmakers.items():
        nid = bm.get("num_id")
        if isinstance(nid, int):
            _num_to_bid[nid] = bid
            if nid > max_num:
                max_num = nid
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
        try:
            payloads.append(("a0_hdr_zlib", zlib.decompress(candidate)))
        except: pass
        if len(raw) > 4:
            candidate4 = raw[4:]
            try:
                payloads.append(("a0_hdr4_zlib", zlib.decompress(candidate4)))
            except: pass

    try: payloads.append(("zlib", zlib.decompress(raw)))
    except: pass

    for i in range(1, min(16, len(raw) - 2)):
        if raw[i] == 0x78 and raw[i+1] in (0x9c, 0xda, 0x01, 0x5e):
            try:
                payloads.append((f"zlib_at{i}", zlib.decompress(raw[i:])))
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
# SFS2X Frame Classification — v7 STRICT
# ---------------------------------------------------------------------------

# Keys que SOLO aparecen en frames de crash/resultado final
CRASH_KEYS = {
    "odd", "odds", "crash", "result",
    "coefficient", "crashValue", "endK", "bust",
    "coef", "coeff",
    "crashMultiplier", "finalMultiplier", "roundResult",
    "maxMultiplier",   # Spribe: roundChartInfo
    "crashX",          # Spribe: crash instantáneo
}

# Keys que indican que el frame es un CASHOUT de jugador (NO un crash)
CASHOUT_KEYS = {
    "cashouts", "openBetsCount", "winAmount", "betId", "player_id",
    "cashOutAt", "cashedOut", "cashedOutAt", "cashout",
    "bets", "betAmount", "payout", "payoutMultiplier",
    "userName", "username", "nickname", "avatar",
    "currentBets", "activeBets",
}

# Comandos que indican CRASH / FIN DE RONDA
CRASH_COMMANDS = {
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

# Comandos de TICK en vuelo — SIEMPRE ignorar (excepto si tiene crashX)
TICK_COMMANDS = {
    "x", "X",
    "tick", "update",
    "updateCurrentCashOuts", "currentCashOuts", "cashouts",
    "B", "T", "S",
}

# Comandos de estado/info — ignorar para extracción
STATE_COMMANDS = {
    "currentBetsInfoHandler",
    "gameStateHandler",
    "statsHandler",
}

# Keys de protocolo SFS2X — nunca son game data
PROTOCOL_KEYS = {
    "ct", "ms", "tk", "aph",
    "zn", "un", "pw",
    "rn", "ri", "rc", "uc", "id",
    "xt", "pi",
}


def classify_frame(obj):
    """
    Clasifica un frame SFS2X decodificado.
    Retorna (tipo, is_crash_event).
    is_crash_event=True SOLO si el frame representa un CRASH REAL (fin de ronda).
    """
    if not isinstance(obj, dict):
        return ("unknown", False)

    c = obj.get("c")
    a = obj.get("a")
    p = obj.get("p", {})

    # Handshake / Login / Room — protocolo, no game
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
    if c == 7:
        return ("ping", False)

    # Extension frames (a == 13)
    if a == 13:
        if isinstance(p, dict):
            cmd = p.get("c", "")
            if isinstance(cmd, str):
                cmd_stripped = cmd.strip()
                cmd_lower = cmd_stripped.lower()

                # ¿Es un comando de CRASH / FIN DE RONDA?
                for gc in CRASH_COMMANDS:
                    if gc.lower() == cmd_lower:
                        return (f"ext_crash:{cmd_stripped}", True)

                # ¿Es tick en vuelo?
                for tc in TICK_COMMANDS:
                    if tc.lower() == cmd_lower:
                        # EXCEPCIÓN: ext:x con crashX = crash real
                        inner_p = p.get("p", {})
                        if isinstance(inner_p, dict) and "crashX" in inner_p:
                            return (f"ext_crashX:{cmd_stripped}", True)
                        return (f"ext_tick:{cmd_stripped}", False)

                # ¿Es estado?
                for sc in STATE_COMMANDS:
                    if sc.lower() == cmd_lower:
                        return (f"ext_state:{cmd_stripped}", False)

                # Comando desconocido — verificar si inner_p tiene crash keys
                inner_p = p.get("p", {})
                if isinstance(inner_p, dict):
                    has_crash = any(k in inner_p for k in CRASH_KEYS)
                    has_cashout = any(k in inner_p for k in CASHOUT_KEYS)
                    if has_crash and not has_cashout:
                        return (f"ext_crash_detected:{cmd_stripped}", True)

                return (f"ext:{cmd_stripped}", False)
        return ("extension", False)

    # Frame SFS con keys de crash directas en p (sin wrapper de extensión)
    if isinstance(p, dict):
        has_crash_key = any(k in p for k in CRASH_KEYS)
        has_cashout_key = any(k in p for k in CASHOUT_KEYS)
        if has_crash_key and not has_cashout_key:
            return (f"sfs_c{c}_a{a}_crash", True)
        if has_cashout_key:
            return (f"sfs_c{c}_a{a}_cashout", False)

    return (f"sfs_c{c}_a{a}", False)


# ---------------------------------------------------------------------------
# Multiplier extraction — v7 STRICT (solo de crash frames)
# ---------------------------------------------------------------------------

def extract_crash_mult(obj, label=""):
    """
    Extrae multiplicador de crash SOLO de frames clasificados como crash.
    Retorna (multiplier_or_None, round_id_or_None).
    """
    if not isinstance(obj, dict):
        return None, None

    p = obj.get("p")
    if not isinstance(p, dict):
        return None, None

    inner_p = p.get("p")
    cmd = p.get("c", "")

    round_id = None
    if isinstance(inner_p, dict):
        round_id = inner_p.get("roundId")

    # --- Buscar crash value en inner_p (extensión Spribe) ---
    mult = _extract_from_obj(inner_p, label, cmd)
    if mult is not None:
        return mult, round_id

    # --- Buscar en p directamente ---
    mult = _extract_from_obj(p, label, cmd)
    if mult is not None:
        return mult, round_id

    # --- Buscar en sub-keys conocidas de resultado ---
    for subkey in ("r", "data", "result", "res", "round", "game", "info"):
        sub = p.get(subkey) if isinstance(p, dict) else None
        if isinstance(sub, dict):
            mult = _extract_from_obj(sub, label, cmd)
            if mult is not None:
                return mult, round_id

    return None, round_id


def _extract_from_obj(obj, label="", cmd=""):
    """
    Extrae UN multiplicador de crash de un dict.
    Solo mira CRASH_KEYS, ignora cualquier cosa que huela a cashout.
    """
    if not isinstance(obj, dict):
        return None

    # Si el objeto tiene keys de cashout, rechazar completamente
    if any(k in obj for k in CASHOUT_KEYS):
        log.debug("[%s] Skipping obj with cashout keys: %s", label, list(obj.keys())[:10])
        return None

    # Si tiene una lista de "cashouts" o "bets", es data de jugadores
    for reject_key in ("cashouts", "bets", "currentBets", "activeBets"):
        val = obj.get(reject_key)
        if isinstance(val, list) and len(val) > 0:
            log.debug("[%s] Skipping obj with %s list (%d items)", label, reject_key, len(val))
            return None

    # Prioridad: crashX > maxMultiplier > odd/crash/result/etc.
    priority_keys = ["crashX", "maxMultiplier", "crash", "crashValue", "bust",
                     "odd", "odds", "result", "coefficient", "endK",
                     "coef", "coeff", "crashMultiplier", "finalMultiplier", "roundResult"]

    # Keys que vienen como entero x100 (odd=234 → 2.34x)
    X100_INT_KEYS = {"odd", "odds", "k", "coef", "coeff",
                     "crashValue", "endK", "crashMultiplier",
                     "finalMultiplier", "coefficient"}

    for key in priority_keys:
        if key not in obj:
            continue
        val = obj[key]

        if key in X100_INT_KEYS and isinstance(val, int):
            if val < 100:
                continue
            parsed_val = round(val / 100.0, 2)
            log.debug("[%s] x100 convert: key='%s' raw=%d -> %.2f", label, key, val, parsed_val)
        else:
            parsed_val = val

        m = _parse_mult(parsed_val)
        if m is not None:
            log.info("[%s] CRASH mult=%.2fx key='%s' raw_val=%s cmd='%s'",
                     label, m, key, val, cmd)
            return m

    return None


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
# Process frames — v7 simplified
# ---------------------------------------------------------------------------
def process_frame(raw_or_text, bm_id, bm_name, is_binary=True, decoder_type="auto"):
    """
    Procesa un frame (binario o texto).
    SOLO extrae multiplicadores de frames clasificados como CRASH.
    """
    label = bm_name or bm_id
    dbg = {
        "time": datetime.now(timezone.utc).isoformat(),
        "size": len(raw_or_text),
        "type": "BIN" if is_binary else "TEXT",
        "decoded": None, "method": None, "frame_type": None, "mults": [],
    }

    if is_binary:
        dbg["hex"] = raw_or_text[:100].hex()
        dbg["ascii"] = raw_or_text[:100].decode("ascii", errors="replace")
    else:
        dbg["text_preview"] = raw_or_text[:600] if isinstance(raw_or_text, str) else ""

    mult = None
    round_id = None

    # --- Binary frame processing ---
    if is_binary:
        if decoder_type in ("auto", "sfs"):
            parsed, method = _try_sfs_decode(raw_or_text)
            if parsed:
                dbg["decoded"] = _sj(parsed)[:1500]
                dbg["method"] = f"sfs2x/{method}"
                if isinstance(parsed, dict):
                    ftype, is_crash = classify_frame(parsed)
                    dbg["frame_type"] = ftype
                    log.info("[%s] SFS2X(%s) → %s (crash=%s): %s",
                             label, method, ftype, is_crash, _sj(parsed)[:600])
                    if is_crash:
                        mult, round_id = extract_crash_mult(parsed, label)
                else:
                    dbg["frame_type"] = "non-dict"

        if mult is None and decoder_type in ("auto", "msgpack"):
            for payload in _variants(raw_or_text):
                try:
                    import msgpack
                    obj = msgpack.unpackb(payload, raw=False)
                    if obj and isinstance(obj, dict):
                        dbg["decoded"] = _sj(obj)[:1500]
                        dbg["method"] = "msgpack"
                        ftype, is_crash = classify_frame(obj)
                        dbg["frame_type"] = ftype
                        if is_crash:
                            mult, round_id = extract_crash_mult(obj, label)
                        if mult is not None:
                            break
                except: continue

        # JSON embedded in binary
        if mult is None:
            for payload in _variants(raw_or_text):
                try:
                    txt = payload.decode("utf-8", errors="strict")
                    for m_match in re.finditer(r'[{\[].*?[}\]]', txt, re.DOTALL):
                        try:
                            obj = json.loads(m_match.group())
                            if isinstance(obj, dict):
                                ftype, is_crash = classify_frame(obj)
                                if is_crash:
                                    mult, round_id = extract_crash_mult(obj, label)
                                    if mult is not None:
                                        dbg["method"] = "json_embed"
                                        dbg["frame_type"] = ftype
                                        break
                        except: continue
                except: continue

    # --- Text frame processing ---
    else:
        text = raw_or_text
        try:
            obj = json.loads(text)
            dbg["decoded"] = _sj(obj)[:1500]
            dbg["method"] = "json"
            if isinstance(obj, dict):
                ftype, is_crash = classify_frame(obj)
                dbg["frame_type"] = ftype
                log.info("[%s] JSON: %s (crash=%s): %s", label, ftype, is_crash, _sj(obj)[:600])
                if is_crash:
                    mult, round_id = extract_crash_mult(obj, label)
        except: pass

        if mult is None:
            for line in text.split('\n'):
                line = line.strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        ftype, is_crash = classify_frame(obj)
                        if is_crash:
                            mult, round_id = extract_crash_mult(obj, label)
                            if mult is not None:
                                break
                except: continue

    if mult is not None:
        dbg["mults"] = [mult]
    _store_dbg(bm_id, dbg)
    return mult, round_id


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
# WebSocket Client — v7: captura desde frame 0
# ---------------------------------------------------------------------------
def _b64_to_bytes(s):
    if not s or not s.strip(): return b""
    try: return base64.b64decode(s)
    except: return b""

ws_generation: dict = {}

def _start_ws(bm_id):
    bm = bookmakers.get(bm_id)
    if not bm: return
    ws_url = bm.get("ws_url", "").strip()
    if not ws_url:
        connection_status[bm_id] = "error"; return

    gen = ws_generation.get(bm_id, 0) + 1
    ws_generation[bm_id] = gen

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

    # Auth state: solo para saber cuándo enviar los mensajes de handshake
    # PERO todos los frames se procesan para game data desde el frame 0
    auth_sent = {"msg1": False, "msg2": False, "msg3": False}
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
        log.info("[%s] ✓ CONNECTED — capturing from frame 0", name)
        connection_status[bm_id] = "connected"
        if msg1 and not auth_sent["msg1"]:
            auth_sent["msg1"] = True
            send_msg(ws, msg1, 1, "Handshake")

    def _try_advance_auth(ws, parsed):
        """
        Intenta avanzar la secuencia de autenticación basándose en el frame recibido.
        Se ejecuta EN PARALELO con el procesamiento de game data.
        """
        if not isinstance(parsed, dict):
            return

        ftype, _ = classify_frame(parsed)

        if "handshake_resp" in ftype and not auth_sent["msg2"]:
            auth_sent["msg2"] = True
            log.info("[%s] ✓ Handshake response → sending Login (msg2)", name)
            time.sleep(0.2)
            send_msg(ws, msg2, 2, "Login")
            return

        if ("login_resp" in ftype or "room_join" in ftype) and not auth_sent["msg3"]:
            auth_sent["msg3"] = True
            log.info("[%s] ✓ Login response → sending Extension (msg3)", name)
            time.sleep(0.2)
            send_msg(ws, msg3, 3, "Extension")
            return

        if "ext" in ftype and not auth_sent["msg2"]:
            auth_sent["msg2"] = True
            log.info("[%s] ✓ Got extension early → sending Login anyway", name)
            time.sleep(0.2)
            send_msg(ws, msg2, 2, "Login")
        elif "ext" in ftype and not auth_sent["msg3"]:
            auth_sent["msg3"] = True
            log.info("[%s] ✓ Got extension before msg3 → sending Extension", name)
            time.sleep(0.2)
            send_msg(ws, msg3, 3, "Extension")

    def on_message(ws, message):
        if ws_generation.get(bm_id) != gen:
            return

        if isinstance(message, str):
            stats["recv_txt"] += 1
            log.info("[%s] ← TXT #%d (%d chars): %s",
                     name, stats["recv_txt"], len(message), message[:500])

            # PROCESAR para game data (frame 0+)
            mult, rid = process_frame(message, bm_id, name,
                                      is_binary=False, decoder_type=decoder_type)
            if mult is not None:
                _record_crash(bm_id, name, mult, rid)

            # También intentar avanzar auth con JSON
            try:
                obj = json.loads(message)
                if isinstance(obj, dict):
                    _try_advance_auth(ws, obj)
            except: pass
            return

        stats["recv_bin"] += 1
        log.info("[%s] ← BIN #%d (%d bytes): %s",
                 name, stats["recv_bin"], len(message), message[:80].hex())

        # Decodificar para auth Y para game data
        parsed, method = _try_sfs_decode(message)

        # 1. Avanzar auth si es necesario
        if parsed and isinstance(parsed, dict):
            _try_advance_auth(ws, parsed)

        # 2. SIEMPRE procesar para game data — desde frame 0
        mult, rid = process_frame(message, bm_id, name,
                                  is_binary=True, decoder_type=decoder_type)
        if mult is not None:
            _record_crash(bm_id, name, mult, rid)

    def on_error(ws, error):
        err_str = str(error)
        log.error("[%s] ✗ ERROR: %s", name, err_str)
        if "Connection refused" in err_str:
            log.error("[%s] ▶ Servidor rechazó la conexión. Verifica la URL wss://", name)
        elif "timed out" in err_str.lower() or "timeout" in err_str.lower():
            log.error("[%s] ▶ Timeout — servidor no responde.", name)
        elif "SSL" in err_str or "certificate" in err_str.lower():
            log.error("[%s] ▶ Error SSL.", name)
        elif "403" in err_str or "Forbidden" in err_str:
            log.error("[%s] ▶ HTTP 403 — servidor rechaza los headers.", name)

    def on_close(ws, code, msg):
        log.info("[%s] CLOSED code=%s msg=%s | sent=%d bin=%d txt=%d",
                 name, code, msg, stats["sent"], stats["recv_bin"], stats["recv_txt"])
        if code == 1006:
            log.error("[%s] ▶ Código 1006: Cierre anormal.", name)
        elif code is None and stats["recv_bin"] == 0 and stats["recv_txt"] == 0:
            log.error("[%s] ▶ Cerró sin recibir NINGÚN frame.", name)

        if ws_generation.get(bm_id) != gen:
            log.debug("[%s] on_close: hilo obsoleto gen=%d, ignorando", name, gen)
            return
        if bm_id in bookmakers and bookmakers[bm_id].get("active", False):
            connection_status[bm_id] = "reconnecting"
            log.info("[%s] Reconnect en 5s...", name)
            def _reconnect():
                time.sleep(5)
                if ws_generation.get(bm_id) == gen and bm_id in bookmakers and bookmakers[bm_id].get("active", False):
                    _start_ws(bm_id)
            threading.Thread(target=_reconnect, daemon=True).start()
        else:
            connection_status[bm_id] = "disconnected"

    # Headers de navegador real
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(ws_url)
        origin_host = f"{parsed_url.scheme.replace('wss','https').replace('ws','http')}://{parsed_url.netloc}"
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
    log.info("[%s] Headers: Origin=%s Host=%s", name, headers["Origin"], headers["Host"])

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
    """Registra crash con dedup en 3 capas."""
    now = datetime.now(timezone.utc)
    mult = round(float(multiplier), 2)

    # Capa 1: dedup por round_id exacto
    if round_id is not None:
        for prev in reversed(crashes.get(bm_id, [])[-50:]):
            if prev.get("round_id") == round_id:
                log.debug("[%s] Dedup capa1: roundId=%s ya existe", bm_name, round_id)
                return

    # Capa 2: enriquecer con round_id sin duplicar
    if round_id is not None:
        for prev in reversed(crashes.get(bm_id, [])[-5:]):
            try:
                prev_t = datetime.fromisoformat(prev["timestamp"])
                if (now - prev_t).total_seconds() > 15:
                    break
                if round(float(prev["multiplier"]), 2) == mult and prev.get("round_id") is None:
                    prev["round_id"] = round_id
                    log.debug("[%s] Dedup capa2: enriqueció %.2fx con roundId", bm_name, mult)
                    return
            except:
                pass

    # Capa 3: mismo valor en últimos 15s
    for prev in reversed(crashes.get(bm_id, [])[-10:]):
        try:
            prev_t = datetime.fromisoformat(prev["timestamp"])
            age = (now - prev_t).total_seconds()
            if age > 15:
                break
            if round(float(prev["multiplier"]), 2) == mult:
                log.debug("[%s] Dedup capa3: %.2fx ya registrado hace %.1fs", bm_name, mult, age)
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
    log.info("[%s] ★★★ CRASH RECORDED: %.2fx (round_id=%s) total=%d ★★★",
             bm_name, mult, round_id, len(crashes.get(bm_id, [])))
    threading.Thread(target=_save_crashes, daemon=True).start()

def _stop_ws(bm_id):
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
    initial = crashes.get(bid, [])[-30:]
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
    return jsonify({
        "status": "ok",
        "bookmakers": len(bookmakers),
        "active_connections": sum(1 for s in connection_status.values() if s in ("connected", "connecting", "reconnecting")),
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

@app.route("/api/aviator/bookmakers/<bid>/clear-crashes", methods=["POST"])
def clear_crashes_endpoint(bid):
    """Limpia todos los crashes de un bookmaker — reset manual."""
    if bid not in bookmakers:
        return jsonify({"error": "Not found"}), 404
    count = len(crashes.get(bid, []))
    crashes[bid] = []
    _save_crashes()
    name = bookmakers[bid].get("name", bid)
    log.info("[%s] ★ CRASHES CLEARED (%d removed) ★", name, count)
    return jsonify({"ok": True, "cleared": count})

@app.route("/api/clear-all-crashes", methods=["POST"])
def clear_all_crashes():
    """Limpia crashes de TODOS los bookmakers."""
    total = sum(len(v) for v in crashes.values())
    for k in crashes:
        crashes[k] = []
    _save_crashes()
    log.info("★ ALL CRASHES CLEARED (%d total) ★", total)
    return jsonify({"ok": True, "cleared": total})

@app.route("/api/export-config")
def export_config():
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
    time.sleep(15)
    while True:
        try:
            for bid, bm in list(bookmakers.items()):
                if not bm.get("active", False):
                    continue
                status = connection_status.get(bid, "disconnected")
                if status in ("error", "disconnected"):
                    log.info("[watchdog] %s está en '%s' — reconectando...", bm.get("name", bid), status)
                    _start_ws(bid)
                    time.sleep(2)
        except Exception as e:
            log.error("[watchdog] Error: %s", e)
        time.sleep(20)

threading.Thread(target=_watchdog, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
