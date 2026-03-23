"""
PREDICTORES TATAN — Aviator Bookmaker Scraping Manager
v8 — Sobre base v6, cambios quirúrgicos:
  1. Captura desde frame 0 — game data se extrae SIN esperar auth
     (auth msgs se envían en paralelo, no bloquean extracción)
  2. classify_frame ESTRICTO — ticks/cashouts/estado retornan is_game=False
     Solo CRASH_COMMANDS y crashX retornan is_game=True
  3. _scan_game_obj rechaza objetos con CASHOUT_KEYS (evita falsos positivos)
  4. Persistencia: _save_crashes guarda 500 por bookmaker (no 30)
     Al reiniciar restaura del disco y continúa en orden cronológico
  5. Reconexión robusta: on_close + watchdog + periodic save cada 60s
  6. Endpoints clear-crashes por bookmaker y global
  7. Thread-safe con _crashes_lock
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
_DATA_DIR    = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
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
MAX_DBG     = 200

_bm_counter: int = 0
_num_to_bid: dict = {}
_crashes_lock = threading.Lock()

def _load_config():
    global bookmakers, crashes

    # 1. Env var BOOKMAKERS_JSON (Render)
    env_json = os.environ.get("BOOKMAKERS_JSON", "").strip()
    if env_json:
        try:
            d = json.loads(env_json)
            bookmakers = d.get("bookmakers", {})
            for k, v in d.get("crashes", {}).items():
                crashes[k] = v
            _rebuild_num_index()
            log.info("Loaded %d bookmakers from BOOKMAKERS_JSON env var", len(bookmakers))
        except Exception as e:
            log.error("Failed to load from BOOKMAKERS_JSON env var: %s", e)

    # 2. Fallback: config.json
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

    # 3. crashes.json tiene prioridad — se guarda más frecuentemente
    if os.path.exists(CRASHES_PATH):
        try:
            with open(CRASHES_PATH) as f:
                saved = json.load(f)
            for k, v in saved.items():
                crashes[k] = v
            total = sum(len(v) for v in crashes.values())
            log.info("Restored %d crashes from %s", total, CRASHES_PATH)
        except Exception as e:
            log.error("crashes load fail: %s", e)

    if not bookmakers:
        log.info("No config — starting empty.")
    else:
        total = sum(len(v) for v in crashes.values())
        log.info("★ READY: %d bookmakers, %d crashes restored ★", len(bookmakers), total)

def _save_config():
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"bookmakers": bookmakers, "crashes": dict(crashes)},
                      f, indent=2, default=str)
    except Exception as e:
        log.error("Save fail: %s", e)

def _save_crashes():
    """Guarda 500 crashes recientes por bookmaker en orden cronológico."""
    try:
        with _crashes_lock:
            slim = {}
            for k, v in crashes.items():
                if v:
                    slim[k] = v[-500:]
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
            if nid > max_num: max_num = nid
    for bid, bm in bookmakers.items():
        if not isinstance(bm.get("num_id"), int):
            max_num += 1
            bm["num_id"] = max_num
            _num_to_bid[max_num] = bid
    _bm_counter = max_num

# ---------------------------------------------------------------------------
# SFS2X Decoder
# ---------------------------------------------------------------------------
class SFS2XDecoder:
    def __init__(self, data):
        self.data = data; self.pos = 0
    def read_bytes(self, n):
        if self.pos + n > len(self.data): raise ValueError(f"EOF {n}@{self.pos}")
        r = self.data[self.pos:self.pos+n]; self.pos += n; return r
    def read_byte(self):  return self.read_bytes(1)[0]
    def read_short(self): return struct.unpack(">h", self.read_bytes(2))[0]
    def read_ushort(self):return struct.unpack(">H", self.read_bytes(2))[0]
    def read_int(self):   return struct.unpack(">i", self.read_bytes(4))[0]
    def read_long(self):  return struct.unpack(">q", self.read_bytes(8))[0]
    def read_float(self): return struct.unpack(">f", self.read_bytes(4))[0]
    def read_double(self):return struct.unpack(">d", self.read_bytes(8))[0]
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
        if t == 9:  n=self.read_short(); return [self.read_byte()!=0 for _ in range(n)]
        if t == 10: n=self.read_int();   return self.read_bytes(n)
        if t == 11: n=self.read_short(); return [self.read_short() for _ in range(n)]
        if t == 12: n=self.read_short(); return [self.read_int() for _ in range(n)]
        if t == 13: n=self.read_short(); return [self.read_long() for _ in range(n)]
        if t == 14: n=self.read_short(); return [round(self.read_float(),4) for _ in range(n)]
        if t == 15: n=self.read_short(); return [round(self.read_double(),4) for _ in range(n)]
        if t == 16: n=self.read_short(); return [self.read_utf() for _ in range(n)]
        if t == 17: n=self.read_short(); return [self.read_value() for _ in range(n)]
        if t == 18:
            n=self.read_short(); o={}
            for _ in range(n): k=self.read_utf(); o[k]=self.read_value()
            return o
        if t == 19:
            c=self.read_utf(); n=self.read_short(); o={"__class__": c}
            for _ in range(n): k=self.read_utf(); o[k]=self.read_value()
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
        try: payloads.append(("a0_hdr_zlib", zlib.decompress(raw[3:])))
        except: pass
        if len(raw) > 4:
            try: payloads.append(("a0_hdr4_zlib", zlib.decompress(raw[4:])))
            except: pass
    try: payloads.append(("zlib", zlib.decompress(raw)))
    except: pass
    for i in range(1, min(16, len(raw)-2)):
        if raw[i]==0x78 and raw[i+1] in (0x9c,0xda,0x01,0x5e):
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
# Frame Classification — v8 ESTRICTO
# ---------------------------------------------------------------------------

PROTOCOL_KEYS = {
    "ct","ms","tk","aph","zn","un","pw","rn","ri","rc","uc","id","xt","pi",
}

GAME_KEYS = {
    "odd","odds","crash","result",
    "coefficient","crashValue","endK","bust",
    "coef","coeff",
    "crashMultiplier","finalMultiplier","roundResult",
    "maxMultiplier","crashX",
}

CASHOUT_KEYS = {
    "cashouts","openBetsCount","winAmount","betId","player_id",
    "cashOutAt","cashedOut","cashedOutAt","cashout",
    "bets","betAmount","payout","payoutMultiplier",
    "userName","username","nickname","avatar",
    "currentBets","activeBets",
}

CRASH_COMMANDS = {
    "N","F","R",
    "end","finish",
    "currentbetsresult",
    "aviator.crash","game.result","crash",
    "roundEnd","roundend","gameend","gameEnd",
    "roundChartInfo",
}

TICK_COMMANDS = {
    "x","X","tick","update",
    "updateCurrentCashOuts","currentCashOuts","cashouts",
    "B","T","S",
}

STATE_COMMANDS = {
    "currentBetsInfoHandler","gameStateHandler","statsHandler","C",
}


def classify_frame(obj):
    """v8: ESTRICTO. Solo CRASH_COMMANDS y crashX → is_game=True."""
    if not isinstance(obj, dict):
        return ("unknown", False)
    c = obj.get("c"); a = obj.get("a"); p = obj.get("p", {})

    if c==0 and a==0: return ("handshake_req", False)
    if c==0 and isinstance(p, dict) and ("ct" in p or "ms" in p or "tk" in p):
        return ("handshake_resp", False)
    if c==0 and a==1: return ("login_req", False)
    if c==1 and a==0: return ("login_resp", False)
    if c==4: return ("room_join", False)
    if c==7: return ("ping", False)

    if a == 13 and isinstance(p, dict):
        cmd = p.get("c", "")
        if isinstance(cmd, str):
            cl = cmd.lower().strip()
            # 1. CRASH command
            for gc in CRASH_COMMANDS:
                if gc.lower() == cl:
                    return (f"ext_crash:{cmd}", True)
            # 2. TICK — excepto crashX
            for tc in TICK_COMMANDS:
                if tc.lower() == cl:
                    inner_p = p.get("p", {})
                    if isinstance(inner_p, dict) and "crashX" in inner_p:
                        return (f"ext_crashX:{cmd}", True)
                    return (f"ext_tick:{cmd}", False)
            # 3. STATE
            for sc in STATE_COMMANDS:
                if sc.lower() == cl:
                    return (f"ext_state:{cmd}", False)
            # 4. Desconocido — buscar crash keys en inner_p sin cashout keys
            inner_p = p.get("p", {})
            if isinstance(inner_p, dict):
                if any(k in inner_p for k in GAME_KEYS) and not any(k in inner_p for k in CASHOUT_KEYS):
                    return (f"ext_crash_detected:{cmd}", True)
            return (f"ext:{cmd}", False)
        return ("extension", False)

    if isinstance(p, dict):
        if any(k in p for k in GAME_KEYS) and not any(k in p for k in CASHOUT_KEYS):
            return (f"sfs_c{c}_a{a}_gamedata", True)

    return (f"sfs_c{c}_a{a}", False)

# ---------------------------------------------------------------------------
# Multiplier extraction — v8 con filtro cashout
# ---------------------------------------------------------------------------
def extract_game_mults(obj, label=""):
    if not isinstance(obj, dict): return [], None
    p = obj.get("p")
    if not isinstance(p, dict): return [], None
    inner_p = p.get("p"); cmd = p.get("c", "")

    if isinstance(cmd, str) and cmd.lower() in {c.lower() for c in TICK_COMMANDS}:
        if not (isinstance(inner_p, dict) and "crashX" in inner_p):
            return [], None

    round_id = inner_p.get("roundId") if isinstance(inner_p, dict) else None
    results = []

    if isinstance(inner_p, dict):
        results.extend(_scan_game_obj(inner_p, label, cmd))
    for subkey in ("r","data","result","res","round","game","info"):
        sub = p.get(subkey)
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
    """v8: rechaza objetos con CASHOUT_KEYS."""
    if depth > 8 or not isinstance(obj, dict): return []
    # ★ v8: si tiene keys de cashout → rechazar
    if any(k in obj for k in CASHOUT_KEYS):
        return []

    results = []
    X100 = {"odd","odds","k","coef","coeff","crashValue","endK",
            "crashMultiplier","finalMultiplier","coefficient"}
    for key, val in obj.items():
        if key in PROTOCOL_KEYS: continue
        if key in GAME_KEYS:
            if key in X100 and isinstance(val, int):
                if val < 100: continue
                pv = round(val/100.0, 2)
            else:
                pv = val
            m = _parse_mult(pv)
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
    if not isinstance(val, (int,float)):
        if isinstance(val, str):
            try: val = float(val)
            except: return None
        else: return None
    fv = float(val)
    if 1.00 <= fv <= 9999.99: return round(fv, 2)
    return None

# ---------------------------------------------------------------------------
# Process frames
# ---------------------------------------------------------------------------
def process_binary(raw, bm_id, bm_name, decoder_type):
    label = bm_name or bm_id
    dbg = {"time": datetime.now(timezone.utc).isoformat(), "size": len(raw), "type": "BIN",
           "hex": raw[:100].hex(), "ascii": raw[:100].decode("ascii",errors="replace"),
           "decoded": None, "method": None, "frame_type": None, "mults": []}
    mults = []
    if decoder_type in ("auto","sfs"):
        parsed, method = _try_sfs_decode(raw)
        if parsed:
            dbg["decoded"] = _sj(parsed)[:1500]; dbg["method"] = f"sfs2x/{method}"
            ftype, is_game = classify_frame(parsed) if isinstance(parsed, dict) else ("other", False)
            dbg["frame_type"] = ftype
            if is_game: mults, _ = extract_game_mults(parsed, label)
    if not mults and decoder_type in ("auto","msgpack"):
        for payload in _variants(raw):
            try:
                import msgpack
                obj = msgpack.unpackb(payload, raw=False)
                if obj and isinstance(obj, dict):
                    ftype, is_game = classify_frame(obj)
                    dbg["method"] = "msgpack"; dbg["frame_type"] = ftype
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
                                if ms: dbg["method"]="json_embed"; dbg["frame_type"]=ftype; mults.extend(ms)
                    except: continue
            except: continue
    seen=set(); unique=[]
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)
    dbg["mults"] = unique; _store_dbg(bm_id, dbg)
    return unique

def process_text(text, bm_id, bm_name):
    label = bm_name or bm_id
    dbg = {"time": datetime.now(timezone.utc).isoformat(), "size": len(text), "type": "TEXT",
           "text_preview": text[:600], "decoded": None, "method": None, "frame_type": None, "mults": []}
    mults = []
    try:
        obj = json.loads(text); dbg["decoded"]=_sj(obj)[:1500]; dbg["method"]="json"
        if isinstance(obj, dict):
            ftype, is_game = classify_frame(obj); dbg["frame_type"]=ftype
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
    seen=set(); unique=[]
    for m in mults:
        if m not in seen: seen.add(m); unique.append(m)
    dbg["mults"]=unique; _store_dbg(bm_id, dbg)
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
# WebSocket Client — v8: captura desde frame 0
# ---------------------------------------------------------------------------
def _b64_to_bytes(s):
    if not s or not s.strip(): return b""
    try: return base64.b64decode(s)
    except: return b""

ws_generation: dict = {}

def _start_ws(bm_id):
    bm = bookmakers.get(bm_id)
    if not bm: return
    ws_url = bm.get("ws_url","").strip()
    if not ws_url: connection_status[bm_id]="error"; return

    gen = ws_generation.get(bm_id, 0) + 1
    ws_generation[bm_id] = gen
    old_ws = ws_connections.pop(bm_id, None)
    if old_ws:
        try: old_ws.close()
        except: pass

    msg1 = _b64_to_bytes(bm.get("msg1_b64",""))
    msg2 = _b64_to_bytes(bm.get("msg2_b64",""))
    msg3 = _b64_to_bytes(bm.get("msg3_b64",""))
    decoder_type = bm.get("decoder_type","auto").lower()
    name = bm.get("name", bm_id)
    connection_status[bm_id] = "connecting"

    log.info("="*60)
    log.info("[%s] CONNECTING to %s", name, ws_url)
    log.info("[%s] msg1=%d msg2=%d msg3=%d bytes", name, len(msg1), len(msg2), len(msg3))
    log.info("="*60)

    auth_sent = {"msg1": False, "msg2": False, "msg3": False}
    stats = {"sent": 0, "recv_bin": 0, "recv_txt": 0}

    def send_msg(ws, msg, idx, lt):
        if not msg: return
        try:
            ws.send(msg, opcode=websocket.ABNF.OPCODE_BINARY)
            stats["sent"] += 1
            log.info("[%s] → SENT msg%d (%s, %d bytes)", name, idx, lt, len(msg))
        except Exception as e:
            log.error("[%s] ✗ SEND FAIL msg%d: %s", name, idx, e)

    def _advance_auth(ws, ftype):
        """Avanza auth en paralelo — NO bloquea game data."""
        if "handshake_resp" in ftype and not auth_sent["msg2"]:
            auth_sent["msg2"] = True
            log.info("[%s] ✓ Handshake resp → Login", name)
            time.sleep(0.3); send_msg(ws, msg2, 2, "Login")
        elif ("login_resp" in ftype or "room_join" in ftype) and not auth_sent["msg3"]:
            auth_sent["msg3"] = True
            log.info("[%s] ✓ Login resp → Extension", name)
            time.sleep(0.3); send_msg(ws, msg3, 3, "Extension")
        elif "ext" in ftype and not auth_sent["msg2"]:
            auth_sent["msg2"] = True
            log.info("[%s] ✓ Early ext → Login", name)
            time.sleep(0.3); send_msg(ws, msg2, 2, "Login")
        elif "ext" in ftype and not auth_sent["msg3"]:
            auth_sent["msg3"] = True
            log.info("[%s] ✓ Ext before msg3 → Extension", name)
            time.sleep(0.3); send_msg(ws, msg3, 3, "Extension")

    def on_open(ws):
        log.info("[%s] ✓ CONNECTED — capturing from frame 0", name)
        connection_status[bm_id] = "connected"
        if msg1 and not auth_sent["msg1"]:
            auth_sent["msg1"] = True
            send_msg(ws, msg1, 1, "Handshake")

    def on_message(ws, message):
        if ws_generation.get(bm_id) != gen: return

        if isinstance(message, str):
            stats["recv_txt"] += 1
            log.info("[%s] ← TXT #%d (%d chars): %s",
                     name, stats["recv_txt"], len(message), message[:500])
            # ★ v8: SIEMPRE procesar — desde frame 0
            mults = process_text(message, bm_id, name)
            for m in mults: _record_crash(bm_id, name, m)
            try:
                obj = json.loads(message)
                if isinstance(obj, dict):
                    ft, _ = classify_frame(obj)
                    _advance_auth(ws, ft)
            except: pass
            return

        stats["recv_bin"] += 1
        log.info("[%s] ← BIN #%d (%d bytes): %s",
                 name, stats["recv_bin"], len(message), message[:80].hex())

        parsed, method = _try_sfs_decode(message)
        ftype = "unknown"; is_game = False
        if parsed and isinstance(parsed, dict):
            ftype, is_game = classify_frame(parsed)
            log.info("[%s] Frame: %s (game=%s) decoded=%s",
                     name, ftype, is_game, _sj(parsed)[:600])

        dbg = {"time": datetime.now(timezone.utc).isoformat(),
               "size": len(message), "type": "BIN",
               "hex": message[:100].hex(),
               "ascii": message[:100].decode("ascii",errors="replace"),
               "decoded": _sj(parsed)[:1500] if parsed else None,
               "method": f"sfs2x/{method}" if method else None,
               "frame_type": ftype, "mults": []}

        # ★ v8: auth Y game data EN PARALELO
        if parsed and isinstance(parsed, dict):
            _advance_auth(ws, ftype)

        # ★ v8: extraer game data SIEMPRE que is_game=True — sin esperar auth
        if is_game and parsed:
            mults, rid = extract_game_mults(parsed, name)
            dbg["mults"] = mults
            for m in mults: _record_crash(bm_id, name, m, rid)
        elif not parsed:
            # Fallback: process_binary completo
            mults = process_binary(message, bm_id, name, decoder_type)
            dbg["mults"] = mults
            for m in mults: _record_crash(bm_id, name, m)

        _store_dbg(bm_id, dbg)

    def on_error(ws, error):
        log.error("[%s] ✗ ERROR: %s", name, error)

    def on_close(ws, code, msg):
        log.info("[%s] CLOSED code=%s msg=%s | sent=%d bin=%d txt=%d",
                 name, code, msg, stats["sent"], stats["recv_bin"], stats["recv_txt"])
        if code == 1006:
            log.error("[%s] ▶ 1006: Cierre anormal", name)
        elif code is None and stats["recv_bin"]==0 and stats["recv_txt"]==0:
            log.error("[%s] ▶ Cerró sin recibir frames", name)
        if ws_generation.get(bm_id) != gen: return
        if bm_id in bookmakers and bookmakers[bm_id].get("active", False):
            connection_status[bm_id] = "reconnecting"
            log.info("[%s] Reconnect en 5s...", name)
            def _rc():
                time.sleep(5)
                if ws_generation.get(bm_id)==gen and bm_id in bookmakers and bookmakers[bm_id].get("active",False):
                    _start_ws(bm_id)
            threading.Thread(target=_rc, daemon=True).start()
        else:
            connection_status[bm_id] = "disconnected"

    try:
        from urllib.parse import urlparse
        pu = urlparse(ws_url)
        origin = f"{pu.scheme.replace('wss','https').replace('ws','http')}://{pu.netloc}"
    except: origin = "https://spribegaming.com"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Origin": origin,
        "Host": ws_url.split("/")[2] if "//" in ws_url else ws_url,
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    }

    import ssl as _ssl
    ssl_opts = {"cert_reqs": _ssl.CERT_NONE, "check_hostname": False}

    ws_app = websocket.WebSocketApp(ws_url,
        on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close, header=headers)
    ws_connections[bm_id] = ws_app
    t = threading.Thread(target=ws_app.run_forever,
        kwargs={"ping_interval":20, "ping_timeout":10, "sslopt":ssl_opts}, daemon=True)
    t.start()
    ws_threads[bm_id] = t


def _record_crash(bm_id, bm_name, multiplier, round_id=None):
    """Thread-safe. Crashes se guardan en orden cronológico (append)."""
    now = datetime.now(timezone.utc)
    mult = round(float(multiplier), 2)

    with _crashes_lock:
        bm_crashes = crashes[bm_id]

        # Capa 1: dedup round_id
        if round_id is not None:
            for prev in reversed(bm_crashes[-50:]):
                if prev.get("round_id") == round_id: return

        # Capa 2: enriquecer round_id
        if round_id is not None:
            for prev in reversed(bm_crashes[-5:]):
                try:
                    pt = datetime.fromisoformat(prev["timestamp"])
                    if (now-pt).total_seconds() > 15: break
                    if round(float(prev["multiplier"]),2)==mult and prev.get("round_id") is None:
                        prev["round_id"] = round_id; return
                except: pass

        # Capa 3: mismo valor en 15s
        for prev in reversed(bm_crashes[-10:]):
            try:
                pt = datetime.fromisoformat(prev["timestamp"])
                age = (now-pt).total_seconds()
                if age > 15: break
                if round(float(prev["multiplier"]),2)==mult: return
            except: pass

        entry = {"multiplier": mult, "timestamp": now.isoformat(), "id": str(uuid.uuid4())[:8]}
        if round_id is not None: entry["round_id"] = round_id
        bm_crashes.append(entry)
        if len(bm_crashes) > MAX_CRASHES:
            crashes[bm_id] = bm_crashes[-MAX_CRASHES:]

    # SSE push fuera del lock
    for q in sse_subscribers.get(bm_id, []):
        try: q.put_nowait(entry)
        except: pass
    log.info("[%s] ★★★ CRASH: %.2fx (rid=%s) total=%d ★★★",
             bm_name, mult, round_id, len(crashes.get(bm_id,[])))
    threading.Thread(target=_save_crashes, daemon=True).start()

def _stop_ws(bm_id):
    ws_generation[bm_id] = ws_generation.get(bm_id,0)+1
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
        bc = crashes.get(bid, [])
        item["crash_count"] = len(bc)
        item["last_crash_at"] = bc[-1]["timestamp"] if bc else None
        r.append(item)
    return jsonify(r)

@app.route("/api/aviator/bookmakers", methods=["POST"])
def create_bm():
    global _bm_counter
    d = request.get_json(force=True)
    bid = str(uuid.uuid4())[:12]
    _bm_counter += 1; nid = _bm_counter; _num_to_bid[nid] = bid
    bm = {
        "num_id": nid,
        "name": d.get("name","Sin Nombre"), "description": d.get("description",""),
        "image_url": d.get("image_url",""), "ws_url": d.get("ws_url",""),
        "msg1_b64": d.get("msg1_b64",""), "msg2_b64": d.get("msg2_b64",""),
        "msg3_b64": d.get("msg3_b64",""), "decoder_type": d.get("decoder_type","auto"),
        "active": d.get("active",True), "recommended": d.get("recommended",False),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    bookmakers[bid] = bm; _save_config()
    if bm["active"]: _start_ws(bid)
    return jsonify({"id": bid, **bm}), 201

@app.route("/api/aviator/bookmakers/<bid>", methods=["PUT"])
def update_bm(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    d = request.get_json(force=True); bm = bookmakers[bid]
    for k in ("name","description","image_url","ws_url","msg1_b64","msg2_b64","msg3_b64",
              "decoder_type","active","recommended"):
        if k in d: bm[k] = d[k]
    bm["updated_at"] = datetime.now(timezone.utc).isoformat()
    bookmakers[bid] = bm; _save_config(); _stop_ws(bid)
    if bm.get("active",False): _start_ws(bid)
    return jsonify({"id": bid, **bm})

@app.route("/api/aviator/bookmakers/<bid>", methods=["DELETE"])
def delete_bm(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    _stop_ws(bid); del bookmakers[bid]
    crashes.pop(bid,None); debug_frames.pop(bid,None); _save_config()
    return jsonify({"deleted": bid})

@app.route("/api/aviator/bookmakers/<bid>/crashes", methods=["GET"])
def get_crashes(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    limit = request.args.get("limit",100,type=int)
    return jsonify(crashes.get(bid,[])[-limit:])

@app.route("/api/aviator/rounds/<int:num_id>", methods=["GET"])
def get_rounds_by_num(num_id):
    bid = _num_to_bid.get(num_id)
    if not bid or bid not in bookmakers:
        return jsonify({"error": f"Bookmaker #{num_id} not found"}), 404
    limit = request.args.get("limit",100,type=int)
    bm = bookmakers[bid]; data = crashes.get(bid,[])[-limit:]
    return jsonify({"num_id":num_id,"id":bid,"name":bm.get("name",bid),
        "description":bm.get("description",""),"image_url":bm.get("image_url",""),
        "connection_status":connection_status.get(bid,"disconnected"),
        "crash_count":len(crashes.get(bid,[])),"crashes":data})

@app.route("/api/aviator/bookmakers/<bid>/crashes/stream")
def stream_crashes(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    q = Queue(maxsize=100); sse_subscribers[bid].append(q)
    initial = crashes.get(bid,[])[-30:]
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
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    return jsonify({"bookmaker":bid,"name":bookmakers[bid].get("name"),
        "connection_status":connection_status.get(bid,"disconnected"),
        "frame_count":len(debug_frames.get(bid,[])),
        "crash_count":len(crashes.get(bid,[])),
        "frames":debug_frames.get(bid,[])[-30:]})

@app.route("/health")
def health():
    return jsonify({"status":"ok","bookmakers":len(bookmakers),
        "active_connections":sum(1 for s in connection_status.values() if s in ("connected","connecting","reconnecting")),
        "total_crashes":sum(len(v) for v in crashes.values())})

@app.route("/api/aviator/bookmakers/<bid>/ingest", methods=["POST"])
def ingest_crash(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    d = request.get_json(force=True); mult = d.get("multiplier")
    if not isinstance(mult,(int,float)) or not (1.00<=float(mult)<=9999.99):
        return jsonify({"error":"Invalid multiplier"}), 400
    _record_crash(bid, bookmakers[bid].get("name",bid), float(mult))
    return jsonify({"ok":True,"multiplier":mult}), 201

@app.route("/api/aviator/bookmakers/<bid>/clear-crashes", methods=["POST"])
def clear_crashes_bm(bid):
    if bid not in bookmakers: return jsonify({"error":"Not found"}), 404
    with _crashes_lock:
        count = len(crashes.get(bid,[])); crashes[bid] = []
    _save_crashes()
    return jsonify({"ok":True,"cleared":count})

@app.route("/api/clear-all-crashes", methods=["POST"])
def clear_all_crashes():
    with _crashes_lock:
        total = sum(len(v) for v in crashes.values())
        for k in crashes: crashes[k] = []
    _save_crashes()
    return jsonify({"ok":True,"cleared":total})

@app.route("/api/export-config")
def export_config():
    return jsonify({"bookmakers":bookmakers,"crashes":{}})

# ---------------------------------------------------------------------------
_load_config()

def _autostart():
    time.sleep(2)
    for bid, bm in list(bookmakers.items()):
        if bm.get("active",False):
            log.info("Auto-start: %s", bm.get("name",bid))
            _start_ws(bid); time.sleep(1)
threading.Thread(target=_autostart, daemon=True).start()

def _watchdog():
    time.sleep(15)
    while True:
        try:
            for bid, bm in list(bookmakers.items()):
                if not bm.get("active",False): continue
                status = connection_status.get(bid,"disconnected")
                if status in ("error","disconnected"):
                    log.info("[watchdog] %s '%s' → reconectando", bm.get("name",bid), status)
                    _start_ws(bid); time.sleep(2)
        except Exception as e:
            log.error("[watchdog] %s", e)
        time.sleep(20)
threading.Thread(target=_watchdog, daemon=True).start()

def _periodic_save():
    """Guardado periódico cada 60s — backup por si el async falla."""
    time.sleep(30)
    while True:
        try: _save_crashes()
        except: pass
        time.sleep(60)
threading.Thread(target=_periodic_save, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",10000)), debug=False)
