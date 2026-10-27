"""
TOOL QUEST SAKURA FIELDS
- Đọc cấu hình từ quest_config.json (bao gồm bật/tắt xuất các file .txt và hẹn giờ chạy lại 12 tiếng).
- Dùng chung tokenstore.py với balance.py / draw.py:
  + Đọc token mới nhất từ tokens.txt trước khi gia hạn.
  + Chỉ gia hạn khi access token sắp/đã hết hạn (access_valid).
  + Ghi NGAY token mới vào đúng dòng của ví trong tokens.txt (có khóa file .lock chống đè giữa các tool).
  + Đã bỏ khối ghi đè lại toàn bộ tokens.txt ở cuối lượt chạy.
- Nếu phát hiện ví chưa có token hoặc token bị die (refresh thất bại), tự động gọi login.py để login lại ví đó,
  sau khi login xong sẽ chạy lại ngay quest cho ví đó.
Chạy: py quest.py
"""
import os, sys, time, json, threading, re, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests
import tokenstore

# Import module login.py cùng thư mục để gọi khi token die
import login as login_tool

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")

BASE = Path(__file__).parent
F_TOKENS, F_PROXY, F_CODES = BASE / "tokens.txt", BASE / "proxy.txt", BASE / "codes.json"
F_WALLETS = BASE / "wallet.txt"
F_LOG, F_RESULT, F_ERROR = BASE / "log_quest.txt", BASE / "result_quest.txt", BASE / "error_quest.txt"
F_CONFIG = BASE / "quest_config.json"

DEFAULT_CONFIG = {
    "ghi_file": {"log": False, "result": False, "error": False, "linked_x": False},
    "he_thong": {
        "threads": 50,
        "max_retry": 3,
        "timeout": 20,
        "delay_between": 0,
        "dry_run": False,
        "tu_dong_lap_lai": True,
        "hen_gio_chay_lai_gio": 12
    },
    "login_va_x": {
        "goi_login_khi_token_die": True,
        "do_link_x": True,
        "always_solve_captcha": True,
        "sitekey": "0x4AAAAAAE_-UaxUbwRXSJtL",
        "pageurl": "https://sakurafields.com/"
    },
    "nhiem_vu": {
        "do_checkin": True,
        "do_farm": True,
        "claim_min_fill": 0.0,
        "do_draw": True,
        "do_onboard": True,
        "starter_card": "mizuchi",
        "do_auto_duel": True,
        "dwell_seconds": 20
    },
    "skip_task_ids": [
        "cmujz1o8x0yi7p701dsoydki9",
        "cmud2cg271vztxu01pp63a42h",
        "cmten94u30001tp01q0lmgwq1",
    ],
    "manual_duel_sau_auto": True,
    "manual_duel_toi_da": 18,
    "giu_lai_resolve": 0,
}


def load_config():
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        if F_CONFIG.exists():
            user = json.loads(F_CONFIG.read_text(encoding="utf-8-sig"))
            for k, v in user.items():
                if isinstance(v, dict) and isinstance(cfg.get(k), dict): cfg[k].update(v)
                else: cfg[k] = v
        F_CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return cfg


CONFIG = load_config()
WRITE = CONFIG.get("ghi_file", {})
SYS_CFG = CONFIG.get("he_thong", {})
LOGIN_CFG = CONFIG.get("login_va_x", {})
TASK_CFG = CONFIG.get("nhiem_vu", {})

THREADS        = int(SYS_CFG.get("threads", 50))
MAX_RETRY      = int(SYS_CFG.get("max_retry", 3))
TIMEOUT        = int(SYS_CFG.get("timeout", 20))
DELAY_BETWEEN  = float(SYS_CFG.get("delay_between", 0))
DRY_RUN        = bool(SYS_CFG.get("dry_run", False))

GOI_LOGIN_KHI_DIE = bool(LOGIN_CFG.get("goi_login_khi_token_die", True))

DWELL_SECONDS  = int(TASK_CFG.get("dwell_seconds", 20))
DO_CHECKIN     = bool(TASK_CFG.get("do_checkin", True))
DO_FARM        = bool(TASK_CFG.get("do_farm", True))
CLAIM_MIN_FILL = float(TASK_CFG.get("claim_min_fill", 0.0))
DO_DRAW        = bool(TASK_CFG.get("do_draw", True))
DO_ONBOARD     = bool(TASK_CFG.get("do_onboard", True))
STARTER_CARD   = str(TASK_CFG.get("starter_card", "mizuchi"))
DO_AUTO_DUEL   = bool(TASK_CFG.get("do_auto_duel", True))

SKIP_TASK_IDS = set(str(x) for x in CONFIG.get("skip_task_ids", []))
lock = threading.Lock()

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
    "origin": "https://sakurafields.com",
    "referer": "https://sakurafields.com/"
}
SKIP_TYPES = {"x_profile_code", "manual"}
VERIFY_ONLY_TYPES = {"x_like", "x_engagement", "x_engagement_api", "x_follow", "x_comment"}


class Permanent(Exception):
    pass


class TokenDeadError(Exception):
    """Báo hiệu refreshToken đã chết hoặc trống, cần gọi login.py để lấy lại."""


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def short(a): return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else str(a)
def mask_proxy(p): return re.sub(r"//[^/@]*@", "//***@", p) if p else "không proxy"


def format_seconds(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def log(msg):
    line = f"[{now()}] {msg}"
    with lock:
        print(line, flush=True)
        if WRITE.get("log", False):
            with open(F_LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def append(path, text):
    key = {F_RESULT: "result", F_ERROR: "error"}.get(path)
    if key and not WRITE.get(key, False): return
    with lock:
        with open(path, "a", encoding="utf-8") as f: f.write(text + "\n")


def read_lines(path, required=True):
    if not path.exists():
        if required: sys.exit(f"❌ Không tìm thấy file {path.name}")
        return []
    seen, out = set(), []
    for l in path.read_text(encoding="utf-8-sig").splitlines():
        l = l.strip()
        if l and not l.startswith("#") and l not in seen:
            seen.add(l); out.append(l)
    return out


def load_codes():
    try:
        if F_CODES.exists(): return json.loads(F_CODES.read_text(encoding="utf-8-sig"))
    except Exception: pass
    return {}


CODES = load_codes()
PROXIES = [p if "://" in p else "http://" + p for p in read_lines(F_PROXY, required=False)]
def proxy_for(i): return PROXIES[i % len(PROXIES)] if PROXIES else None


# ---- Chạy lại ví lỗi: ví lỗi proxy được đổi sang proxy LIVE khác ----
PROXY_OVERRIDE = {}   # idx -> proxy mới
LAST_ERROR = {}       # idx -> lỗi lần chạy gần nhất
PROXY_STATUS = {}     # proxy -> True (live) / False (chết)
_pstat_lock = threading.Lock()
RERUN_WAIT_SECONDS = 60   # khi tool tự lặp 12h: hỏi chạy lại ví lỗi, quá số giây này không bấm gì thì bỏ qua
PROXY_ERR_KEYS = ("proxy", "tunnel", "connecttimeout", "connection", "timed out", "timeout",
                  "ssl", "max retries", "407", "502", "503", "504", "reset by peer", "remote end closed")


def is_proxy_error(msg):
    m = (msg or "").lower()
    return any(k in m for k in PROXY_ERR_KEYS)


def check_proxy_live(p, timeout=10):
    """Gọi thử tới server Sakura; có phản hồi HTTP (kể cả 404) là proxy sống."""
    with _pstat_lock:
        if p in PROXY_STATUS: return PROXY_STATUS[p]
    try:
        requests.get(API, proxies={"http": p, "https": p}, timeout=timeout)
        live = True
    except Exception:
        live = False
    with _pstat_lock:
        PROXY_STATUS[p] = live
    return live


def pick_new_proxy(current):
    """Đánh dấu proxy hiện tại là chết, nhặt random 1 proxy khác đang LIVE (None nếu hết)."""
    if current:
        with _pstat_lock: PROXY_STATUS[current] = False
    cands = [p for p in PROXIES if p != current and PROXY_STATUS.get(p) is not False]
    random.shuffle(cands)
    for p in cands:
        if check_proxy_live(p): return p
    return None


def swap_dead_proxies(failed, threads):
    """Với các ví lỗi do proxy: thay bằng proxy live khác. In tóm tắt."""
    proxy_fail = [i for i in failed if PROXIES and is_proxy_error(LAST_ERROR.get(i))]
    if proxy_fail: log(f"🔎 Đang tìm proxy live thay cho {len(proxy_fail)} ví lỗi proxy...")

    def swap(i):
        new = pick_new_proxy(PROXY_OVERRIDE.get(i) or proxy_for(i))
        if new: PROXY_OVERRIDE[i] = new
        return bool(new)

    with ThreadPoolExecutor(max_workers=threads) as ex:
        swapped = sum(ex.map(swap, proxy_fail))
    dead = sum(1 for v in PROXY_STATUS.values() if v is False)
    log(f"🔁 Chạy lại {len(failed)} ví lỗi ({swapped}/{len(proxy_fail)} ví đã đổi sang proxy live, "
        f"{dead}/{len(PROXIES)} proxy bị đánh dấu chết)")


def ask_rerun(n, wait=None):
    """Enter = chạy lại ví lỗi, n = thoát. wait = số giây chờ (hết giờ coi như thoát); None = chờ mãi."""
    msg = f"\n👉 Nhấn Enter để chạy lại {n} ví lỗi (gõ n rồi Enter để thoát)"
    if wait is None:
        try: ans = input(msg + ": ").strip().lower()
        except (EOFError, KeyboardInterrupt): return False
        return ans not in ("n", "no", "q", "exit")
    import msvcrt
    print(msg + f" - tự bỏ qua sau {wait}s: ", end="", flush=True)
    end, buf = time.time() + wait, ""
    while time.time() < end:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ("\r", "\n"):
                print()
                return buf.strip().lower() not in ("n", "no", "q", "exit")
            buf += ch
            print(ch, end="", flush=True)
        else:
            time.sleep(0.1)
    print()
    return False


def retry(fn):
    for n in range(1, MAX_RETRY + 1):
        try: return fn()
        except (Permanent, TokenDeadError): raise
        except Exception as e:
            if n == MAX_RETRY: raise
            time.sleep(8 * n if "429" in str(e) else min(0.7 * n, 3))


class Client:
    def __init__(self, addr, access, refresh, proxy):
        self.addr = addr
        self.access, self.refresh, self.s = access, refresh, requests.Session()
        self.s.headers.update(HEADERS)
        if proxy: self.s.proxies = {"http": proxy, "https": proxy}

    def _do(self, method, path, body=None, auth=True):
        h = {"Authorization": f"Bearer {self.access}"} if auth and self.access else {}
        r = self.s.request(method, API + path, json=body if method != "GET" else None, headers=h, timeout=TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504): raise RuntimeError(f"HTTP {r.status_code} {path}")
        try: data = r.json()
        except Exception: data = {}
        return r.status_code, data

    def refresh_session(self, force=False):
        # Đọc token mới nhất trong file (phòng khi tool khác như balance.py/draw.py vừa gia hạn)
        cur = tokenstore.latest(F_TOKENS, self.addr)
        if cur and (cur[0] or cur[1]):
            self.access, self.refresh = cur
        # Chỉ gia hạn khi cần: nếu access token còn hạn thì dùng luôn
        if not force and tokenstore.access_valid(self.access):
            return
        if not self.refresh:
            raise TokenDeadError("thiếu refreshToken")
        code, d = self._do("POST", "/api/session/refresh", {"refreshToken": self.refresh}, auth=False)
        if code != 200 or not d.get("accessToken"):
            # Phòng trường hợp tool khác vừa xoay token cùng lúc, đọc lại file kiểm tra lần nữa
            cur_after = tokenstore.latest(F_TOKENS, self.addr)
            if cur_after and cur_after[0] != self.access and tokenstore.access_valid(cur_after[0]):
                self.access, self.refresh = cur_after
                return
            raise TokenDeadError(f"gia hạn phiên thất bại HTTP {code}")
        self.access = d["accessToken"]
        if d.get("refreshToken"): self.refresh = d["refreshToken"]
        # Ghi ngay lập tức vào đúng dòng của ví trong tokens.txt (có khóa file .lock)
        tokenstore.save(F_TOKENS, self.addr, self.access, self.refresh)

    def call(self, method, path, body=None):
        code, d = self._do(method, path, body)
        if code == 401:
            self.refresh_session(force=True)
            code, d = self._do(method, path, body)
        return code, d


def norm_type(t):
    return str(t or "").lower().strip()


def task_list(d):
    if isinstance(d, list): return d
    for k in ("tasks", "items", "data", "quests"):
        if isinstance(d.get(k), list): return d[k]
    return []


def task_status(t):
    prog = t.get("progress") if isinstance(t.get("progress"), dict) else {}
    return str(t.get("status") or t.get("state") or prog.get("status") or "").upper()


def work_task(c, t):
    tid = t.get("id")
    ttype = norm_type((t.get("verification") or {}).get("type"))
    status = task_status(t)
    title = str(t.get("title") or tid)[:30]
    if str(tid) in SKIP_TASK_IDS: return None
    if status in ("CLAIMED", "DONE", "COMPLETED"): return None
    if ttype in SKIP_TYPES: return f"{title}: bỏ qua (loại {ttype}, làm tay)"
    if status == "VERIFIED":
        if DRY_RUN: return f"{title}: [thử] đã verified, sẽ claim"
        code, r = c.call("POST", f"/api/tasks/{tid}/claim", {})
        return f"{title}: CLAIM " + ("OK" if code == 200 else f"chưa được ({str(r.get('error', code))[:40]})")
    if ttype == "code_word":
        word = CODES.get(str(tid))
        if not word: return f"{title}: cần từ khóa trong codes.json (id {tid})"
        body = {"codeWord": word}
    elif ttype == "dwell" or ttype in VERIFY_ONLY_TYPES:
        body = {}
    else:
        return f"{title}: bỏ qua (loại {ttype or '?'} chưa hỗ trợ)"
    if DRY_RUN: return f"{title}: [thử] sẽ start -> verify ({ttype}) -> claim"
    c.call("POST", f"/api/tasks/{tid}/start", {})
    if ttype == "dwell": time.sleep(DWELL_SECONDS)
    code, r = c.call("POST", f"/api/tasks/{tid}/verify", body)
    if code != 200 or str(r.get("status", "")).upper() != "VERIFIED":
        why = str(r.get("rejectionReason") or r.get("error") or code)[:50]
        hint = " - hãy làm việc đó trên X rồi chạy lại" if ttype in VERIFY_ONLY_TYPES else ""
        return f"{title}: verify chưa qua ({why}){hint}"
    code, r = c.call("POST", f"/api/tasks/{tid}/claim", {})
    return f"{title}: OK" if code == 200 else f"{title}: verify ok, claim lỗi ({str(r.get('error', code))[:40]})"


def run_wallet(c, skip_refresh=False):
    if not skip_refresh:
        c.refresh_session()
    code, d = c.call("GET", "/api/tasks")
    if code != 200: return [f"không đọc được danh sách quest (HTTP {code})"]
    tasks = task_list(d)
    notes = []
    notes += daily_actions(c)
    if DO_DRAW:
        notes += draw_pull(c)
    if DO_ONBOARD or DO_AUTO_DUEL:
        notes += onboard_and_duel(c)
    code, d = c.call("GET", "/api/tasks")
    tasks = task_list(d) if code == 200 else tasks
    notes += [n for n in (work_task(c, t) for t in tasks if isinstance(t, dict)) if n]
    notes += claim_milestones(c)
    return notes or [f"không có quest cần làm ({len(tasks)} quest)"]


def find_farm(st):
    if not isinstance(st, dict): return None
    for k in ("farm", "gameState", "state"):
        v = st.get(k)
        if isinstance(v, dict):
            if "pendingPetals" in v or "fillFraction" in v: return v
            if isinstance(v.get("farm"), dict): return v["farm"]
    return st if ("pendingPetals" in st or "fillFraction" in st) else None


def daily_actions(c):
    out = []
    code, xs = c.call("GET", "/api/x/status")
    linked = None
    if code == 200 and isinstance(xs, dict):
        for k in ("linked", "connected", "isLinked", "hasX"):
            if isinstance(xs.get(k), bool): linked = xs[k]; break
        if linked is None and xs.get("username"): linked = True
    out.append("X: " + ("ĐÃ link" if linked else "CHƯA link" if linked is False else f"không rõ (HTTP {code})"))

    if DO_CHECKIN:
        code, ci = c.call("GET", "/api/sakura/checkin")
        petals = (ci.get("nextReward") or {}).get("petals", "?") if isinstance(ci, dict) else "?"
        if DRY_RUN: out.append(f"check-in: [thử] thưởng kế {petals}")
        else:
            code, r = c.call("POST", "/api/sakura/checkin")
            err = str(r.get("error", code)) if isinstance(r, dict) else str(code)
            if code == 200:
                out.append(f"check-in OK +{petals}")
            elif "already_checked_in" in err.lower():
                out.append("check-in: ĐÃ điểm danh hôm nay")
            else:
                out.append(f"check-in chưa được: {err[:40]}")

    if DO_FARM:
        code, st = c.call("GET", "/api/sakura/state")
        farm = find_farm(st) if code == 200 else None
        if farm is None: out.append(f"farm: không đọc được trạng thái (HTTP {code})")
        else:
            fill = float(farm.get("fillFraction") or 0)
            pend = farm.get("pendingPetals", "?")
            if fill < CLAIM_MIN_FILL: out.append(f"farm: chưa đầy ({fill:.0%}), bỏ qua")
            elif DRY_RUN: out.append(f"farm: [thử] đầy {fill:.0%}, chờ claim {pend} petals")
            else:
                code, r = c.call("POST", "/api/sakura/farm/claim")
                out.append(f"farm OK (đầy {fill:.0%}, {pend} petals)" if code == 200 else f"farm chưa claim được: {str(r.get('error', code))[:40]}")
    return out


def draw_pull(c):
    code, st = c.call("GET", "/api/sakura/threads")
    if code != 200 or not isinstance(st, dict): return [f"bốc thăm: không đọc được trạng thái (HTTP {code})"]
    if not st.get("enabled", True): return ["bốc thăm: tính năng đang tắt"]
    status = st.get("status") or {}
    if st.get("xRequired") and not st.get("xLinked"): return ["bốc thăm: cần link X trước, bỏ qua"]
    if not status.get("available"):
        s = status.get("cooldownSeconds")
        try: wait = f"{int(s)//3600}h{(int(s)%3600)//60:02d}m"
        except Exception: wait = "?"
        return [f"bốc thăm: chưa tới giờ (còn {wait})"]
    if DRY_RUN: return [f"bốc thăm: [thử] bốc được (còn {status.get('remainingToday', '?')} lượt hôm nay)"]
    code, r = c.call("POST", "/api/sakura/threads/pull", {})
    if code == 200 and r.get("ok"):
        p = r.get("pull") or {}
        return [f"bốc thăm OK: {p.get('tier', '?')} | {p.get('rewardType', '?')} {p.get('amount', p.get('reward', ''))}".strip()]
    return [f"bốc thăm chưa được: {str(r.get('error', code))[:40]}"]


def play_manual_duel(c):
    code, r = c.call("POST", "/api/duel/match/start", {"auto": False})
    if code != 200 or not r.get("ok"): return f"không bắt đầu được duel ({str(r.get('error', code))[:40]})", False
    m = r.get("match") or {}
    for _ in range(12):
        if m.get("finished"): break
        energy, plays = m.get("energy", 0), []
        hand = sorted(m.get("hand") or [], key=lambda x: -x.get("cost", 0))
        used_lanes = set()
        for card in hand:
            for lane, ok in enumerate(card.get("playable") or []):
                cost = (card.get("costPerLane") or [card.get("cost", 99)] * 3)[lane]
                if ok and lane not in used_lanes and cost <= energy:
                    plays.append({"cardId": card["id"], "lane": lane}); energy -= cost; used_lanes.add(lane)
                    break
        code, r = c.call("POST", "/api/duel/match/play", {"turn": m.get("turn"), "plays": plays})
        if code != 200 or not r.get("ok"): return f"duel dừng giữa chừng ({str(r.get('error', code))[:40]})", False
        m = r.get("match") or m
    if not m.get("finished"): return "duel chưa kết thúc sau 12 vòng", False
    res = str(r.get("outcome") or "").lower() or "?"
    return f"duel tay: {res} (petals {r.get('petals', '?')})", True


def onboard_and_duel(c):
    out = []
    code, st = c.call("GET", "/api/duel/state")
    if code != 200: return [f"duel: không đọc được trạng thái (HTTP {code})"]
    if DO_ONBOARD and not st.get("started"):
        if DRY_RUN: out.append(f"[thử] sẽ chọn nhân vật {STARTER_CARD}")
        else:
            code, r = c.call("POST", "/api/duel/starter", {"cardId": STARTER_CARD})
            out.append("chọn nhân vật OK" if code == 200 and r.get("ok") else f"chọn nhân vật lỗi ({str(r.get('error', code))[:40]})")
            code, st = c.call("GET", "/api/duel/state")
    if DO_ONBOARD and st.get("started") and st.get("duels", 0) == 0 and not st.get("activeRunId"):
        out.append("[thử] sẽ duel tay lần đầu" if DRY_RUN else play_manual_duel(c)[0])
        code, st = c.call("GET", "/api/duel/state") if not DRY_RUN else (code, st)
    if DO_AUTO_DUEL and st.get("started") and (st.get("duels", 0) > 0 or DRY_RUN):
        left = (st.get("auto") or {}).get("left", 0)
        if not left: out.append("auto-duel: hết lượt hôm nay")
        elif DRY_RUN: out.append(f"auto-duel: [thử] còn {left} lượt")
        else:
            wins = done = 0
            for _ in range(left):
                code, r = c.call("POST", "/api/duel/match/start", {"auto": True})
                if code != 200 or not r.get("ok"): break
                done += 1; wins += 1 if r.get("outcome") == "win" else 0
            out.append(f"auto-duel: {done} trận, thắng {wins}")
    if CONFIG.get("manual_duel_sau_auto") and st.get("started") and (st.get("duels", 0) > 0 or DRY_RUN):
        if not DRY_RUN: code, st = c.call("GET", "/api/duel/state")
        keep = int(CONFIG.get("giu_lai_resolve", 0))
        cap = int(CONFIG.get("manual_duel_toi_da", 20))
        resolve = (st.get("resolve") or {}).get("now")
        left_auto = (st.get("auto") or {}).get("left", 0) if not DRY_RUN else 0
        if DO_AUTO_DUEL and left_auto: pass
        elif DRY_RUN: out.append(f"duel tay: [thử] sẽ chơi tới {cap} trận (Resolve {resolve}, giữ lại {keep})")
        else:
            done = wins = 0
            for _ in range(cap):
                if resolve is not None and resolve <= keep: break
                text, ok = play_manual_duel(c)
                if not ok:
                    out.append(f"duel tay dừng: {text}"); break
                done += 1; wins += 1 if "win" in text else 0
                code, st = c.call("GET", "/api/duel/state")
                resolve = (st.get("resolve") or {}).get("now")
            out.append(f"duel tay: {done} trận, thắng {wins} (Resolve còn {resolve})")
    return out or ["duel: không có gì cần làm"]


def claim_milestones(c):
    code, d = c.call("GET", "/api/sakura/quests")
    if code != 200: return [f"mốc: không đọc được (HTTP {code})"]
    qs = d.get("quests") if isinstance(d, dict) else d
    out = []
    for q in (qs or []):
        if not isinstance(q, dict) or str(q.get("status", "")).lower() != "completed": continue
        qid, name = q.get("id"), str(q.get("title") or q.get("name") or q.get("id"))[:30]
        if DRY_RUN:
            out.append(f"mốc {name}: [thử] đã hoàn thành, sẽ claim")
            continue
        code, r = c.call("POST", f"/api/sakura/quests/{qid}/claim")
        out.append(f"mốc {name}: CLAIM " + ("OK" if code == 200 else f"chưa được ({str(r.get('error', code))[:40]})"))
    return out


def process(idx, total, line):
    parts = [p.strip() for p in line.split("|")]
    addr = parts[0] if parts else ""
    access = parts[1] if len(parts) > 1 else ""
    refresh = parts[2] if len(parts) > 2 else ""

    if not addr:
        log(f"[{idx+1}/{total}] ❌ Dòng {idx+1} thiếu địa chỉ ví")
        append(F_ERROR, line)
        return False

    proxy = PROXY_OVERRIDE.get(idx) or proxy_for(idx)
    c = Client(addr, access, refresh, proxy)
    try:
        try:
            notes = retry(lambda: run_wallet(c, skip_refresh=False))
        except TokenDeadError as tde:
            if not GOI_LOGIN_KHI_DIE:
                raise Permanent(f"{tde} (goi_login_khi_token_die đang tắt)")
            # Gọi sang file login.py để login lại đúng ví đó
            log(f"[{idx+1}/{total}] {short(addr)} | ⚠️ Token die ({tde}) -> Gọi login.py đăng nhập lại...")
            new_tokens = login_tool.relogin_single_wallet(idx, target_address=addr)
            if not new_tokens or not new_tokens[0]:
                raise Permanent("Gọi login.py nhưng không lấy được token mới")
            c.access, c.refresh = new_tokens[0], new_tokens[1]
            # Ghi ngay bằng tokenstore (có khóa file .lock)
            tokenstore.save(F_TOKENS, addr, c.access, c.refresh)
            log(f"[{idx+1}/{total}] {short(addr)} | 🔄 Đã lấy token mới từ login.py -> Chạy lại quest...")
            notes = retry(lambda: run_wallet(c, skip_refresh=True))

        log(f"[{idx+1}/{total}] {short(addr)} | ✅ {mask_proxy(proxy)}")
        for n in notes: log(f"      - {n}")
        append(F_RESULT, f"{now()} | {addr} | OK | " + " ; ".join(notes))
        return True
    except Exception as e:
        LAST_ERROR[idx] = str(e)
        msg = str(e)[:150]
        log(f"[{idx+1}/{total}] {short(addr)} | ❌ {msg}")
        append(F_RESULT, f"{now()} | {addr} | LỖI | {msg}")
        append(F_ERROR, f"{addr}|{c.access}|{c.refresh}")
        return False


def run_once(vong=1):
    """Chạy 1 chu kỳ toàn bộ danh sách ví."""
    global CONFIG, WRITE, SYS_CFG, LOGIN_CFG, TASK_CFG
    global THREADS, MAX_RETRY, TIMEOUT, DELAY_BETWEEN, DRY_RUN, GOI_LOGIN_KHI_DIE
    global DWELL_SECONDS, DO_CHECKIN, DO_FARM, CLAIM_MIN_FILL, DO_DRAW, DO_ONBOARD, STARTER_CARD, DO_AUTO_DUEL, SKIP_TASK_IDS
    global CODES, PROXIES

    # Đọc lại config, codes, proxy ở đầu mỗi vòng
    CONFIG = load_config()
    WRITE = CONFIG.get("ghi_file", {})
    SYS_CFG = CONFIG.get("he_thong", {})
    LOGIN_CFG = CONFIG.get("login_va_x", {})
    TASK_CFG = CONFIG.get("nhiem_vu", {})

    THREADS           = int(SYS_CFG.get("threads", 50))
    MAX_RETRY         = int(SYS_CFG.get("max_retry", 3))
    TIMEOUT           = int(SYS_CFG.get("timeout", 20))
    DELAY_BETWEEN     = float(SYS_CFG.get("delay_between", 0))
    DRY_RUN           = bool(SYS_CFG.get("dry_run", False))
    GOI_LOGIN_KHI_DIE = bool(LOGIN_CFG.get("goi_login_khi_token_die", True))

    DWELL_SECONDS     = int(TASK_CFG.get("dwell_seconds", 20))
    DO_CHECKIN        = bool(TASK_CFG.get("do_checkin", True))
    DO_FARM           = bool(TASK_CFG.get("do_farm", True))
    CLAIM_MIN_FILL    = float(TASK_CFG.get("claim_min_fill", 0.0))
    DO_DRAW           = bool(TASK_CFG.get("do_draw", True))
    DO_ONBOARD        = bool(TASK_CFG.get("do_onboard", True))
    STARTER_CARD      = str(TASK_CFG.get("starter_card", "mizuchi"))
    DO_AUTO_DUEL      = bool(TASK_CFG.get("do_auto_duel", True))
    SKIP_TASK_IDS     = set(str(x) for x in CONFIG.get("skip_task_ids", []))

    CODES = load_codes()
    PROXIES = [p if "://" in p else "http://" + p for p in read_lines(F_PROXY, required=False)]

    # Nếu chưa có tokens.txt mà có wallet.txt thì gọi login.py tạo khung hoặc chạy trước
    if not F_TOKENS.exists() and F_WALLETS.exists():
        log("⚠️ Chưa có tokens.txt -> Gọi login.py khởi tạo trước...")
        login_tool.main()

    lines = read_lines(F_TOKENS)
    if not lines: sys.exit("❌ tokens.txt rỗng")
    if WRITE.get("error", False): F_ERROR.write_text("", encoding="utf-8")
    threads = max(1, min(THREADS, len(lines)))
    log(f"=== [VÒNG {vong}] Bắt đầu QUEST: {len(lines)} ví | {threads} luồng | {len(PROXIES) or 'không'} proxy | DRY_RUN={DRY_RUN} ===")
    t0, results = time.time(), {}

    def run_batch(indices):
        with ThreadPoolExecutor(max_workers=threads) as ex:
            futs = {}
            for i in indices:
                futs[ex.submit(process, i, len(lines), lines[i])] = i
                if DELAY_BETWEEN: time.sleep(DELAY_BETWEEN)
            for f in as_completed(futs): results[futs[f]] = f.result()
        return [i for i in indices if not results.get(i)]

    # Token đã được ghi ngay lập tức vào tokens.txt mỗi lần gia hạn qua tokenstore.save() (không ghi đè lại ở cuối lượt)
    failed = run_batch(list(range(len(lines))))
    log(f"🎉 [VÒNG {vong}] XONG: {len(lines)-len(failed)}/{len(lines)} ví hoàn thành, {len(failed)} lỗi, {time.time()-t0:.1f}s")

    # Hỏi chạy lại ví lỗi (tool tự lặp 12h thì chỉ chờ RERUN_WAIT_SECONDS giây rồi bỏ qua)
    auto_loop = bool(SYS_CFG.get("tu_dong_lap_lai", True)) and float(SYS_CFG.get("hen_gio_chay_lai_gio", 12)) > 0
    while failed:
        if not ask_rerun(len(failed), RERUN_WAIT_SECONDS if auto_loop else None): break
        swap_dead_proxies(failed, threads)
        if WRITE.get("error", False): F_ERROR.write_text("", encoding="utf-8")
        t1 = time.time()
        failed = run_batch(failed)
        log(f"🎉 [VÒNG {vong}] XONG LẦN CHẠY LẠI: {len(lines)-len(failed)}/{len(lines)} ví hoàn thành, "
            f"{len(failed)} còn lỗi, {time.time()-t1:.1f}s")


def main():
    vong = 1
    try:
        while True:
            run_once(vong)

            cfg_sys = load_config().get("he_thong", {})
            auto_loop = bool(cfg_sys.get("tu_dong_lap_lai", True))
            wait_hours = float(cfg_sys.get("hen_gio_chay_lai_gio", 12))

            if not auto_loop or wait_hours <= 0:
                break

            delay = int(wait_hours * 3600)
            log(f"💤 Đã hoàn thành vòng {vong}. Hẹn giờ chạy lại sau {wait_hours:g} tiếng...")
            while delay > 0:
                print(
                    f"\r⏳ Đếm ngược chạy vòng {vong + 1}: {format_seconds(delay)} (Nhấn Ctrl+C để dừng)   ",
                    end="",
                    flush=True
                )
                time.sleep(1)
                delay -= 1
            print()
            vong += 1
    except KeyboardInterrupt:
        print()
        log("⚠️ Đã dừng bằng Ctrl+C")


if __name__ == "__main__":
    main()