"""
TOOL BỐC THĂM SAKURA FIELDS (Threads) - mỗi ví bốc 1 lần / 12h.
Cần cùng thư mục: tokens.txt (địa chỉ|accessToken|refreshToken), proxy.txt (không bắt buộc)
Chạy: py draw.py
Kết quả: result_draw.txt, log_draw.txt, error_draw.txt (dòng token lỗi, đúng định dạng tokens.txt)
Không động tới cookie X. Ví cần link X sẵn (nếu server yêu cầu) thì mới bốc được.
"""
import os, sys, time, threading, re, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests
import tokenstore

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

# ======================= CẤU HÌNH =======================
THREADS = 5          # số ví chạy cùng lúc
MAX_RETRY = 3        # số lần thử lại khi lỗi mạng/proxy
TIMEOUT = 20         # giây
DELAY_BETWEEN = 0    # giây nghỉ giữa các lần khởi động ví
DRY_RUN = False       # True = chỉ xem trạng thái bốc thăm, KHÔNG bốc
API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")
# ========================================================

BASE = Path(__file__).parent
F_TOKENS, F_PROXY = BASE / "tokens.txt", BASE / "proxy.txt"
F_LOG, F_RESULT, F_ERROR = BASE / "log_draw.txt", BASE / "result_draw.txt", BASE / "error_draw.txt"
lock = threading.Lock()
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
           "origin": "https://sakurafields.com", "referer": "https://sakurafields.com/"}


class Permanent(Exception):
    """Lỗi không retry (phiên hết hạn...)."""


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


PROXIES = [p if "://" in p else "http://" + p for p in read_lines(F_PROXY, required=False)]
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


def retry(fn):
    for n in range(1, MAX_RETRY + 1):
        try: return fn()
        except Permanent: raise
        except Exception:
            if n == MAX_RETRY: raise
            time.sleep(min(0.7 * n, 3))


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
        # lấy token mới nhất trong file (tool khác có thể vừa gia hạn), còn hạn thì dùng luôn, khỏi xoay token
        cur = tokenstore.latest(F_TOKENS, self.addr)
        if cur: self.access, self.refresh = cur
        if not force and tokenstore.access_valid(self.access): return
        if not self.refresh: raise Permanent("thiếu refreshToken")
        code, d = self._do("POST", "/api/session/refresh", {"refreshToken": self.refresh}, auth=False)
        if code != 200 or not d.get("accessToken"):
            raise Permanent(f"gia hạn phiên thất bại HTTP {code} - cần đăng nhập lại")
        self.access = d["accessToken"]
        if d.get("refreshToken"): self.refresh = d["refreshToken"]
        tokenstore.save(F_TOKENS, self.addr, self.access, self.refresh)   # ghi ngay, không chờ cuối lượt

    def call(self, method, path, body=None):
        code, d = self._do(method, path, body)
        if code == 401:
            self.refresh_session(force=True)
            code, d = self._do(method, path, body)
        return code, d


def fmt_wait(secs):
    try: s = int(secs)
    except Exception: return "?"
    return f"{s//3600}h{(s%3600)//60:02d}m"


def run_wallet(c):
    c.refresh_session()
    code, st = c.call("GET", "/api/sakura/threads")
    if code != 200: return f"không đọc được trạng thái (HTTP {code})"
    if not st.get("enabled", True): return "tính năng đang tắt"
    status = st.get("status") or {}
    if st.get("xRequired") and not st.get("xLinked"):
        return "cần link X trước, bỏ qua"
    if not status.get("available"):
        wait = status.get("cooldownSeconds")
        return f"chưa tới giờ bốc (còn {fmt_wait(wait)})" if wait is not None else "chưa tới giờ bốc"
    if DRY_RUN:
        return f"[thử] đang bốc được (còn {status.get('remainingToday', '?')} lượt hôm nay)"
    code, r = c.call("POST", "/api/sakura/threads/pull", {})
    if code == 200 and r.get("ok"):
        p = r.get("pull") or {}
        return f"BỐC OK: {p.get('tier', '?')} | {p.get('rewardType', '?')} {p.get('amount', p.get('reward', ''))}".strip()
    return f"chưa bốc được: {str(r.get('error', code))[:60]}"


def process(idx, total, line):
    parts = line.split("|")
    if len(parts) < 3 or not parts[1].strip():
        log(f"[{idx+1}/{total}] ❌ Dòng {idx+1} sai định dạng (cần địa chỉ|access|refresh)")
        append(F_ERROR, line)
        return None
    addr, access, refresh = (p.strip() for p in parts[:3])
    proxy = PROXY_OVERRIDE.get(idx) or proxy_for(idx)
    c = Client(addr, access, refresh, proxy)
    try:
        note = retry(lambda: run_wallet(c))
        log(f"[{idx+1}/{total}] {short(addr)} | ✅ {note} | {mask_proxy(proxy)}")
        append(F_RESULT, f"{now()} | {addr} | OK | {note}")
        return f"{addr}|{c.access}|{c.refresh}"
    except Exception as e:
        LAST_ERROR[idx] = str(e)
        msg = str(e)[:150]
        log(f"[{idx+1}/{total}] {short(addr)} | ❌ {msg}")
        append(F_RESULT, f"{now()} | {addr} | LỖI | {msg}")
        append(F_ERROR, line)
        return None


def main():
    lines = read_lines(F_TOKENS)
    if not lines: sys.exit("❌ tokens.txt rỗng")
    F_ERROR.write_text("", encoding="utf-8")
    threads = max(1, min(THREADS, len(lines)))
    log(f"Bắt đầu: {len(lines)} ví | {threads} luồng | {len(PROXIES) or 'không'} proxy | DRY_RUN={DRY_RUN}")
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
        return [i for i in indices if not results.get(i)]

    # token đã được ghi ngay vào tokens.txt mỗi lần gia hạn (tokenstore), không cần ghi lại ở cuối
    failed = run_batch(list(range(len(lines))))
    log(f"🎉 XONG: {len(lines)-len(failed)}/{len(lines)} ví chạy được, {len(failed)} lỗi, {time.time()-t0:.1f}s")

    while failed:
        if not ask_rerun(len(failed)): break
        swap_dead_proxies(failed, threads)
        F_ERROR.write_text("", encoding="utf-8")
        t1 = time.time()
        failed = run_batch(failed)
        log(f"🎉 XONG LẦN CHẠY LẠI: {len(lines)-len(failed)}/{len(lines)} ví chạy được, "
            f"{len(failed)} còn lỗi, {time.time()-t1:.1f}s")
    if failed: log("Dòng token lỗi lưu ở error_draw.txt (cần đăng nhập lại ví đó)")


if __name__ == "__main__":
    main()
