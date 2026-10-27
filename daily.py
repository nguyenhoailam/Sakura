"""
TOOL HẰNG NGÀY SAKURA FIELDS - gia hạn phiên, check-in, claim farm, kiểm tra & tự động link X.
Cần cùng thư mục: 
  - tokens.txt (địa chỉ|accessToken|refreshToken, mỗi ví 1 dòng)
  - Cookies-X.txt (chứa danh sách cookie X, ví nào chưa link sẽ tự động bốc 1 cookie ra dùng)
  - proxy.txt (không bắt buộc)
Chạy: py daily.py
"""
import os, sys, time, json, threading, re, random
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

# ======================= CẤU HÌNH =======================
THREADS = 5            # số ví chạy cùng lúc
MAX_RETRY = 3          # số lần thử lại khi lỗi mạng/proxy
TIMEOUT = 20           # giây
DELAY_BETWEEN = 0      # giây nghỉ giữa các lần khởi động ví
DRY_RUN = False        # True = chỉ ĐỌC trạng thái, KHÔNG check-in, KHÔNG claim, KHÔNG link X
DO_LINK_X = True       # Tự động link X nếu kiểm tra thấy ví chưa link
DO_CHECKIN = True      # có check-in không
DO_FARM = True         # có claim farm không
CLAIM_MIN_FILL = 0.0   # chỉ claim farm khi đã đầy từ mức này (0 = có gì claim nấy, 1 = chờ đầy 12h)
API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")
# ========================================================

BASE = Path(__file__).parent
F_TOKENS = BASE / "tokens.txt"
F_PROXY = BASE / "proxy.txt"
F_COOKIES_X = BASE / "Cookies-X.txt"
F_LINKED_X = BASE / "linked_x.txt"
F_LOG, F_RESULT, F_ERROR = BASE / "log_daily.txt", BASE / "result_daily.txt", BASE / "error_daily.txt"

lock = threading.Lock()
cookie_lock = threading.Lock()

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://sakurafields.com",
    "referer": "https://sakurafields.com/"
}

X_BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"


class Permanent(Exception):
    """Lỗi không retry (phiên hết hạn, server từ chối)."""


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def short(a): return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else str(a)
def mask_proxy(p): return re.sub(r"//[^/@]*@", "//***@", p) if p else "không proxy"


def log(msg):
    line = f"[{now()}] {msg}"
    with lock:
        print(line, flush=True)
        with open(F_LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def append(path, text):
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


def read_unique_tokens(path):
    """
    Đọc tokens.txt và lọc trùng theo địa chỉ ví (nếu 1 ví bị ghi nhiều lần thì chỉ lấy dòng MỚI NHẤT ở dưới cùng).
    """
    if not path.exists():
        sys.exit(f"❌ Không tìm thấy file {path.name}")
    by_addr = {}
    order = []
    for l in path.read_text(encoding="utf-8-sig").splitlines():
        l = l.strip()
        if not l or l.startswith("#"):
            continue
        addr = l.split("|")[0].strip().lower()
        if addr not in by_addr:
            order.append(addr)
        by_addr[addr] = l  # Ghi đè bằng dòng mới nhất của ví đó
    return [by_addr[a] for a in order]


PROXIES = [p if "://" in p else "http://" + p for p in read_lines(F_PROXY, required=False)]
COOKIES_QUEUE = deque(read_lines(F_COOKIES_X, required=False))

def proxy_for(i): return PROXIES[i % len(PROXIES)] if PROXIES else None


# ---- Chạy lại ví lỗi: ví lỗi proxy được đổi sang proxy LIVE khác ----
PROXY_OVERRIDE = {}   # idx -> proxy mới
LAST_ERROR = {}       # idx -> lỗi lần chạy gần nhất
PROXY_STATUS = {}     # proxy -> True (live) / False (chết)
_pstat_lock = threading.Lock()
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


def ask_rerun(n):
    try:
        ans = input(f"\n👉 Nhấn Enter để chạy lại {n} ví lỗi (gõ n rồi Enter để thoát): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return ans not in ("n", "no", "q", "exit")


def parse_x_cookie(raw_line):
    """Quét và trích xuất auth_token và ct0 từ mọi định dạng trong Cookies-X.txt."""
    if not raw_line:
        return None, None
    
    m_auth = re.search(r"auth_token=([a-fA-F0-9]{40})", raw_line)
    m_ct0 = re.search(r"ct0=([a-fA-F0-9]+)", raw_line)
    
    auth_token = m_auth.group(1) if m_auth else None
    ct0 = m_ct0.group(1) if m_ct0 else None
    
    if not auth_token:
        for part in re.split(r"[|;\s]+", raw_line.strip()):
            if re.fullmatch(r"[a-fA-F0-9]{40}", part):
                auth_token = part
                break
                
    return auth_token, ct0


def pop_valid_cookie_x():
    """Lấy 1 dòng cookie hợp lệ từ hàng đợi Cookies-X.txt."""
    with cookie_lock:
        while COOKIES_QUEUE:
            raw = COOKIES_QUEUE.popleft()
            auth_token, ct0 = parse_x_cookie(raw)
            if auth_token:
                return raw, auth_token, ct0
            log(f"⚠️ Bỏ qua dòng cookie X không hợp lệ: {raw[:35]}...")
        return None, None, None


def push_back_cookie_x(raw_cookie):
    """Trả lại cookie vào kho nếu lỗi do mạng/state chứ không phải do cookie X die."""
    if raw_cookie:
        with cookie_lock:
            COOKIES_QUEUE.appendleft(raw_cookie)


def save_remaining_cookies():
    """Cập nhật lại file Cookies-X.txt (chỉ giữ lại các cookie chưa dùng)."""
    with cookie_lock:
        if F_COOKIES_X.exists():
            content = "\n".join(COOKIES_QUEUE) + ("\n" if COOKIES_QUEUE else "")
            F_COOKIES_X.write_text(content, encoding="utf-8")


def retry(fn):
    for n in range(1, MAX_RETRY + 1):
        try: return fn()
        except Permanent: raise
        except Exception:
            if n == MAX_RETRY: raise
            time.sleep(min(0.7 * n, 3))


class Client:
    def __init__(self, access, refresh, proxy):
        self.access, self.refresh, self.proxy = access, refresh, proxy
        self.s = requests.Session()
        self.s.headers.update(HEADERS)
        if proxy: self.s.proxies = {"http": proxy, "https": proxy}
        self.changed = False

    def _do(self, method, path, body=None, auth=True, params=None):
        h = {"Authorization": f"Bearer {self.access}"} if auth and self.access else {}
        kwargs = {"headers": h, "timeout": TIMEOUT, "params": params}
        if method != "GET" and body is not None:
            kwargs["json"] = body
        r = self.s.request(method, API + path, **kwargs)
        if r.status_code in (429, 500, 502, 503, 504): raise RuntimeError(f"HTTP {r.status_code} {path}")
        try: data = r.json()
        except Exception: data = {}
        return r.status_code, data

    def refresh_session(self):
        if not self.refresh: raise Permanent("thiếu refreshToken")
        code, d = self._do("POST", "/api/session/refresh", {"refreshToken": self.refresh}, auth=False)
        if code != 200 or not d.get("accessToken"):
            raise Permanent(f"gia hạn phiên thất bại HTTP {code} ({str(d.get('error',''))[:60]}) - cần đăng nhập lại")
        self.access = d["accessToken"]
        if d.get("refreshToken"): self.refresh = d["refreshToken"]
        self.changed = True

    def call(self, method, path, body=None, params=None):
        """Gọi API có token; 401 thì gia hạn phiên một lần rồi gọi lại."""
        code, d = self._do(method, path, body, params=params)
        if code == 401:
            self.refresh_session()
            code, d = self._do(method, path, body, params=params)
        return code, d

    def link_x_flow(self, auth_token, ct0=None):
        """
        Thực hiện trọn vẹn 3 bước Link X trong 1 mạch để tránh hết hạn state:
        1. POST /api/x/link-start -> lấy oauth_url
        2. GET & POST https://api.x.com/2/oauth2/authorize -> lấy redirect_uri chứa code & state
        3. GET /api/auth/x/callback?code=...&state=... (1 lần duy nhất, không redirect)
        """
        # --- BƯỚC 1: Lấy OAuth URL từ Sakura Fields ---
        code_start, d_start = self.call("POST", "/api/x/link-start", body=None)
        if code_start != 200 or not isinstance(d_start, dict) or not d_start.get("url"):
            raise RuntimeError(f"link-start lỗi HTTP {code_start}: {str(d_start)[:80]}")
        oauth_url = d_start["url"]

        # --- BƯỚC 2: Authorize trên X bằng Cookie ---
        xsess = requests.Session()
        if self.proxy:
            xsess.proxies = {"http": self.proxy, "https": self.proxy}

        for dom in (".x.com", ".twitter.com", "api.x.com"):
            xsess.cookies.set("auth_token", auth_token, domain=dom)
            if ct0:
                xsess.cookies.set("ct0", ct0, domain=dom)

        ua = HEADERS["user-agent"]
        if not ct0:
            xsess.get("https://x.com", headers={"user-agent": ua}, timeout=TIMEOUT)
            ct0 = xsess.cookies.get("ct0", domain=".x.com") or xsess.cookies.get("ct0")
            if not ct0:
                raise ValueError("Cookie X die hoặc không lấy được ct0")

        parsed = urlparse(oauth_url)
        query_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        expected_state = query_params.get("state")

        x_headers = {
            "user-agent": ua,
            "authorization": f"Bearer {X_BEARER}",
            "x-csrf-token": ct0,
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "origin": "https://x.com",
            "referer": "https://x.com/",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
        }

        api_url = "https://api.x.com/2/oauth2/authorize"
        r1 = xsess.get(api_url, params=query_params, headers=x_headers, timeout=TIMEOUT)
        new_ct0 = xsess.cookies.get("ct0", domain=".x.com") or xsess.cookies.get("ct0")
        if new_ct0 and new_ct0 != ct0:
            ct0 = new_ct0
            x_headers["x-csrf-token"] = ct0
            for dom in (".x.com", ".twitter.com", "api.x.com"):
                xsess.cookies.set("ct0", ct0, domain=dom)
            r1 = xsess.get(api_url, params=query_params, headers=x_headers, timeout=TIMEOUT)

        if r1.status_code in (401, 403):
            raise ValueError(f"Cookie X die/khóa (HTTP {r1.status_code})")
        if not r1.ok:
            raise RuntimeError(f"X OAuth2 GET lỗi {r1.status_code}: {r1.text[:80]}")

        d1 = r1.json()
        auth_code = d1.get("auth_code")
        if not auth_code:
            raise RuntimeError(f"X không trả về auth_code: {str(d1)[:80]}")

        # POST xác nhận ủy quyền (Approve)
        r2 = xsess.post(
            api_url,
            headers={**x_headers, "content-type": "application/x-www-form-urlencoded"},
            data={
                "approval": "true",
                "code": auth_code,
                "consent_flow": "web_consent"
            },
            timeout=TIMEOUT
        )
        if not r2.ok:
            raise RuntimeError(f"X OAuth2 Approve lỗi {r2.status_code}: {r2.text[:80]}")

        d2 = r2.json()
        redirect_uri = d2.get("redirect_uri", "")
        if redirect_uri:
            cb_parsed = urlparse(redirect_uri)
            cb_qs = {k: v[0] for k, v in parse_qs(cb_parsed.query).items()}
            final_code = cb_qs.get("code") or auth_code
            final_state = cb_qs.get("state") or expected_state
        else:
            final_code = auth_code
            final_state = expected_state

        # --- BƯỚC 3: Gọi Callback DUY NHẤT 1 LẦN (vì state chỉ dùng 1 lần) ---
        cb_headers = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://sakurafields.com/",
            "origin": "https://sakurafields.com",
        }
        r_cb = self.s.get(
            f"{API}/api/auth/x/callback",
            params={"code": final_code, "state": final_state},
            headers=cb_headers,
            allow_redirects=False,  # Không đuổi theo 302 redirect sang frontend
            timeout=TIMEOUT
        )

        # Kiểm tra lại /api/x/status để xác nhận đã link thành công
        code_st, xs = self.call("GET", "/api/x/status")
        if code_st == 200 and check_is_linked(xs) is True:
            return xs

        # Nếu HTTP 200 hoặc 302 Redirect về ?x=linked thì cũng là thành công
        if r_cb.status_code in (200, 201, 204, 301, 302, 303, 307, 308):
            loc = r_cb.headers.get("Location", "")
            if "error" not in loc.lower():
                try: return r_cb.json()
                except Exception: return {}

        try: err_json = r_cb.json()
        except Exception: err_json = {}
        err_msg = err_json.get("error") or err_json.get("message") or r_cb.text[:80]
        raise RuntimeError(f"Callback HTTP {r_cb.status_code}: {err_msg}")


def pick(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur: return default
        cur = cur[k]
    return cur


def find_farm(state):
    """Tìm khối farm trong /api/sakura/state (không chắc tên nên tìm linh hoạt)."""
    if not isinstance(state, dict): return None
    for k in ("farm", "gameState", "state"):
        v = state.get(k)
        if isinstance(v, dict):
            if "pendingPetals" in v or "fillFraction" in v: return v
            if isinstance(v.get("farm"), dict): return v["farm"]
    if "pendingPetals" in state or "fillFraction" in state: return state
    return None


def check_is_linked(xs_data):
    if not isinstance(xs_data, dict):
        return None
    for k in ("linked", "connected", "isLinked", "hasX"):
        if isinstance(xs_data.get(k), bool):
            return xs_data[k]
    if xs_data.get("username") or xs_data.get("xUsername") or xs_data.get("twitterUsername"):
        return True
    return None


def run_wallet(c, addr):
    """Trả về (danh sách ghi chú, link_x)."""
    notes = []
    c.refresh_session()   # luôn gia hạn đầu phiên để token mới nhất
    notes.append("phiên OK")

    # --- 1. Kiểm tra link X & Tự động bốc Cookie từ Cookies-X.txt để link nếu chưa link ---
    code, xs = c.call("GET", "/api/x/status")
    linked = check_is_linked(xs) if code == 200 else None

    if linked is True:
        uname = xs.get("username") or xs.get("xUsername") or ""
        notes.append(f"X: ĐÃ link ({'@' + uname if uname else 'OK'})")
    elif linked is False:
        if DO_LINK_X and not DRY_RUN:
            raw_cookie, auth_token, ct0 = pop_valid_cookie_x()
            if not auth_token:
                notes.append("X: CHƯA link (hết cookie hợp lệ trong Cookies-X.txt)")
            else:
                try:
                    cb_data = c.link_x_flow(auth_token, ct0)
                    linked = True
                    uname = (cb_data.get("username") or cb_data.get("xUsername") if isinstance(cb_data, dict) else "") or ""
                    notes.append(f"X: Vừa link OK! {'(@' + uname + ')' if uname else ''}".strip())
                    append(F_LINKED_X, f"{addr}|{uname}|{raw_cookie}")
                    save_remaining_cookies()
                except ValueError as ve:
                    # Cookie X bị die/khóa -> bỏ cookie này và lưu lại danh sách còn lại
                    notes.append(f"X: lỗi cookie ({str(ve)[:60]})")
                    save_remaining_cookies()
                except Exception as e:
                    # Lỗi mạng hoặc lỗi state -> trả lại cookie vào kho để không bị mất
                    push_back_cookie_x(raw_cookie)
                    notes.append(f"X: lỗi link ({str(e)[:75]})")
        else:
            notes.append("X: CHƯA link")
    else:
        notes.append(f"X: không rõ (HTTP {code})")

    # --- 2. Check-in ---
    if DO_CHECKIN:
        code, ci = c.call("GET", "/api/sakura/checkin")
        petals = pick(ci, "nextReward", "petals", default="?")
        if DRY_RUN:
            notes.append(f"check-in: [thử] thưởng kế {petals}")
        else:
            code, r = c.call("POST", "/api/sakura/checkin")
            if code == 200: notes.append(f"check-in OK +{petals}")
            else: notes.append(f"check-in chưa được: {str(r.get('error', code))[:60]}")

    # --- 3. Farm ---
    if DO_FARM:
        code, st = c.call("GET", "/api/sakura/state")
        farm = find_farm(st) if code == 200 else None
        if farm is None:
            notes.append(f"farm: không đọc được trạng thái (HTTP {code})")
        else:
            fill = float(farm.get("fillFraction") or 0)
            pend = farm.get("pendingPetals", "?")
            if fill < CLAIM_MIN_FILL:
                notes.append(f"farm: chưa đầy ({fill:.0%}), bỏ qua")
            elif DRY_RUN:
                notes.append(f"farm: [thử] đầy {fill:.0%}, chờ claim {pend} petals")
            else:
                code, r = c.call("POST", "/api/sakura/farm/claim")
                if code == 200: notes.append(f"farm OK (đầy {fill:.0%}, {pend} petals)")
                else: notes.append(f"farm chưa claim được: {str(r.get('error', code))[:60]}")
    return notes, linked


def process(idx, total, line):
    parts = line.split("|")
    if len(parts) < 3 or not parts[1].strip():
        log(f"[{idx+1}/{total}] ❌ Dòng {idx+1} sai định dạng (cần địa chỉ|access|refresh)")
        append(F_ERROR, line)
        return None
    addr, access, refresh = (p.strip() for p in parts[:3])
    proxy = PROXY_OVERRIDE.get(idx) or proxy_for(idx)
    c = Client(access, refresh, proxy)
    try:
        notes, linked = retry(lambda: run_wallet(c, addr))
        log(f"[{idx+1}/{total}] {short(addr)} | ✅ " + " | ".join(notes) + f" | {mask_proxy(proxy)}")
        append(F_RESULT, f"{now()} | {addr} | OK | " + " | ".join(notes))
        new_line = f"{addr}|{c.access}|{c.refresh}"
        return new_line, linked
    except Exception as e:
        LAST_ERROR[idx] = str(e)
        msg = str(e)[:150]
        log(f"[{idx+1}/{total}] {short(addr)} | ❌ {msg}")
        append(F_RESULT, f"{now()} | {addr} | LỖI | {msg}")
        append(F_ERROR, line)
        return None


def main():
    lines = read_unique_tokens(F_TOKENS)
    if not lines: sys.exit("❌ tokens.txt rỗng")
    F_ERROR.write_text("", encoding="utf-8")
    threads = max(1, min(THREADS, len(lines)))
    log(
        f"Bắt đầu: {len(lines)} ví (đã lọc trùng) | {threads} luồng | "
        f"{len(PROXIES) or 'không'} proxy | {len(COOKIES_QUEUE)} cookie X trong kho | DRY_RUN={DRY_RUN}"
    )
    if not PROXIES: log("Không có proxy.txt -> dùng IP máy")
    t0, results = time.time(), {}

    def run_batch(indices):
        try:
            with ThreadPoolExecutor(max_workers=threads) as ex:
                futs = {}
                for i in indices:
                    futs[ex.submit(process, i, len(lines), lines[i])] = i
                    if DELAY_BETWEEN: time.sleep(DELAY_BETWEEN)
                for f in as_completed(futs): results[futs[f]] = f.result()
        except KeyboardInterrupt:
            log("⚠️ Đã dừng bằng Ctrl+C")
        # ghi lại token đã gia hạn và dọn sạch các dòng trùng trong tokens.txt
        out = [results[i][0] if results.get(i) else lines[i] for i in range(len(lines))]
        tmp = F_TOKENS.with_suffix(".tmp")
        tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
        os.replace(tmp, F_TOKENS)
        return [i for i in indices if not results.get(i)]

    failed = run_batch(list(range(len(lines))))
    ok = len(lines) - len(failed)
    no_x = sum(1 for v in results.values() if v and v[1] is False)
    log(f"🎉 XONG: {ok}/{len(lines)} thành công, {len(failed)} lỗi, {no_x} ví chưa link X, {time.time()-t0:.1f}s")

    while failed:
        if not ask_rerun(len(failed)): break
        swap_dead_proxies(failed, threads)
        F_ERROR.write_text("", encoding="utf-8")
        t1 = time.time()
        failed = run_batch(failed)
        ok = len(lines) - len(failed)
        no_x = sum(1 for v in results.values() if v and v[1] is False)
        log(f"🎉 XONG LẦN CHẠY LẠI: {ok}/{len(lines)} thành công, {len(failed)} còn lỗi, "
            f"{no_x} ví chưa link X, {time.time()-t1:.1f}s")
    if failed: log("Dòng token lỗi lưu ở error_daily.txt (cần đăng nhập lại ví đó)")


if __name__ == "__main__":
    main()