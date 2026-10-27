"""
TOOL SỐ DƯ + LỊCH SỬ BỐC THĂM SAKURA FIELDS (chỉ đọc, không check-in/claim/bốc gì cả).
Cần cùng thư mục: tokens.txt (địa chỉ|accessToken|refreshToken), proxy.txt (không bắt buộc)
Chạy: py balance.py
Kết quả:
  màn hình          : bảng số dư Petals / $SAKURA / BNB từng ví + tổng, kèm lịch sử bốc thăm
  balance_report.txt: số dư từng ví, dòng i = ví thứ i trong tokens.txt (bật/tắt bằng WRITE_FILES)
  draw_history.txt  : lịch sử bốc thăm (thưởng nhận được), dòng i = ví thứ i, các lần bốc nối bằng ';'
  Hai file được ghi đè mỗi lần chạy, ví lỗi vẫn giữ đúng dòng của nó (ghi "LỖI: ...").
Lưu ý: mỗi lần chạy tool vẫn gia hạn phiên và lưu refresh token mới vào tokens.txt như quest.py.
"""
import os, sys, time, threading, re, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import requests
import tokenstore

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

# ======================= CẤU HÌNH =======================
THREADS = 50          # số ví chạy cùng lúc
MAX_RETRY = 3        # số lần thử lại khi lỗi mạng/proxy
TIMEOUT = 20         # giây
DELAY_BETWEEN = 0    # giây nghỉ giữa các lần khởi động ví
SHOW_HISTORY = True  # in lịch sử bốc thăm ra màn hình
HISTORY_LIMIT = 10   # in tối đa bao nhiêu lần bốc gần nhất cho mỗi ví trên màn hình (file txt luôn ghi đủ)
WRITE_FILES = True   # True = ghi balance_report.txt và draw_history.txt (mỗi ví một dòng cố định)
API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")
# ========================================================

BASE = Path(__file__).parent
F_TOKENS, F_PROXY = BASE / "tokens.txt", BASE / "proxy.txt"
F_REPORT, F_HISTORY = BASE / "balance_report.txt", BASE / "draw_history.txt"
lock = threading.Lock()
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
           "origin": "https://sakurafields.com", "referer": "https://sakurafields.com/"}


class Permanent(Exception):
    pass


def short(a): return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else str(a)
def mask_proxy(p): return re.sub(r"//[^/@]*@", "//***@", p) if p else "không proxy"


def dec(x):
    try: return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError): return Decimal(0)


def fmt(d, places=4):
    q = Decimal(1).scaleb(-places)
    s = f"{d.quantize(q):,f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


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


def swap_dead_proxies(failed, errors, threads):
    """Với các ví lỗi do proxy: thay bằng proxy live khác. In tóm tắt."""
    proxy_fail = [i for i in failed if PROXIES and is_proxy_error(errors.get(i))]
    if proxy_fail: print(f"🔎 Đang tìm proxy live thay cho {len(proxy_fail)} ví lỗi proxy...", flush=True)

    def swap(i):
        new = pick_new_proxy(PROXY_OVERRIDE.get(i) or proxy_for(i))
        if new: PROXY_OVERRIDE[i] = new
        return bool(new)

    with ThreadPoolExecutor(max_workers=threads) as ex:
        swapped = sum(ex.map(swap, proxy_fail))
    dead = sum(1 for v in PROXY_STATUS.values() if v is False)
    print(f"🔁 Chạy lại {len(failed)} ví lỗi ({swapped}/{len(proxy_fail)} ví đã đổi sang proxy live, "
          f"{dead}/{len(PROXIES)} proxy bị đánh dấu chết)", flush=True)


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
        except Exception as e:
            if n == MAX_RETRY: raise
            # 429 = bị giới hạn tốc độ, chờ lâu hơn rồi mới thử lại
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


def fetch_wallet(c):
    c.refresh_session()
    code, st = c.call("GET", "/api/sakura/state")
    if code != 200 or not isinstance(st, dict): raise RuntimeError(f"không đọc được số dư (HTTP {code})")
    bal, farm = st.get("balances") or {}, st.get("farm") or {}
    info = {
        "petal": dec(bal.get("PETAL")), "sakura": dec(bal.get("SAKURA")),
        "pend_petal": dec(farm.get("pendingPetals")), "pend_sakura": dec(farm.get("pendingSakura")),
        "bnb": Decimal(0), "history": [], "pulled_today": None,
    }
    code, th = c.call("GET", "/api/sakura/threads")
    if code == 200 and isinstance(th, dict):
        info["bnb"] = dec((th.get("bnb") or {}).get("balance"))
        info["history"] = [h for h in (th.get("mine") or []) if isinstance(h, dict)]
    return info


def process(idx, total, line):
    parts = line.split("|")
    if len(parts) < 3 or not parts[1].strip():
        return idx, None, f"Dòng {idx+1} sai định dạng (cần địa chỉ|access|refresh)", line
    addr, access, refresh = (p.strip() for p in parts[:3])
    c = Client(addr, access, refresh, PROXY_OVERRIDE.get(idx) or proxy_for(idx))
    try:
        info = retry(lambda: fetch_wallet(c))
        return idx, {"addr": addr, **info}, None, f"{addr}|{c.access}|{c.refresh}"
    except Exception as e:
        return idx, {"addr": addr}, str(e)[:100], line


def main():
    lines = read_lines(F_TOKENS)
    if not lines: sys.exit("❌ tokens.txt rỗng")
    threads = max(1, min(THREADS, len(lines)))
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Đọc số dư {len(lines)} ví | {threads} luồng | {len(PROXIES) or 'không'} proxy")
    t0, results, new_lines = time.time(), {}, {}

    def run_batch(indices):
        try:
            with ThreadPoolExecutor(max_workers=threads) as ex:
                futs = []
                for i in indices:
                    futs.append(ex.submit(process, i, len(lines), lines[i]))
                    if DELAY_BETWEEN: time.sleep(DELAY_BETWEEN)
                for n_done, f in enumerate(as_completed(futs), 1):
                    i, info, err, newl = f.result()
                    results[i] = (info, err); new_lines[i] = newl
                    # báo tiến độ ngay khi từng ví xong để biết tool đang chạy, không bị "đứng yên"
                    who = short(info["addr"]) if info else f"dòng {i+1}"
                    print(f"  [{n_done}/{len(indices)}] ví #{i+1} {who} " + (f"❌ {err}" if err else "✅"), flush=True)
        except KeyboardInterrupt:
            print("⚠️ Đã dừng bằng Ctrl+C")
        return [i for i in indices if i in results and results[i][1]]

    failed = run_batch(list(range(len(lines))))
    while failed:
        print(f"\n{len(lines)-len(failed)}/{len(lines)} ví đọc được, {len(failed)} lỗi.")
        if not ask_rerun(len(failed)): break
        swap_dead_proxies(failed, {i: results[i][1] for i in failed}, threads)
        failed = run_batch(failed)

    tot_p = tot_s = tot_b = Decimal(0)
    ok = 0
    bal_lines = [""] * len(lines)     # dòng i của file = ví thứ i trong tokens.txt (cố định)
    hist_lines = [""] * len(lines)
    print(f"\n{'#':>3} {'Ví':<14} {'Petals':>12} {'$SAKURA':>14} {'BNB':>10}  Chờ claim (petals / sakura)")
    for i in range(len(lines)):
        info, err = results.get(i, (None, "chưa chạy"))
        who = short(info["addr"]) if info else f"dòng {i+1}"
        full = info["addr"] if info else f"dong-{i+1}"
        if err:
            print(f"{i+1:>3} {who:<14} ❌ {err}")
            bal_lines[i] = f"{full} | LỖI: {err}"
            hist_lines[i] = f"{full} | LỖI: {err}"
            continue
        ok += 1
        tot_p += info["petal"]; tot_s += info["sakura"]; tot_b += info["bnb"]
        print(f"{i+1:>3} {who:<14} {fmt(info['petal'], 0):>12} {fmt(info['sakura'], 4):>14} {fmt(info['bnb'], 4):>10}"
              f"  {fmt(info['pend_petal'], 0)} / {fmt(info['pend_sakura'], 4)}")
        bal_lines[i] = (f"{info['addr']} | Petals: {fmt(info['petal'], 0)} | SAKURA: {fmt(info['sakura'], 4)} | "
                        f"BNB: {fmt(info['bnb'], 4)} | Chờ claim: {fmt(info['pend_petal'], 0)} petals, "
                        f"{fmt(info['pend_sakura'], 4)} sakura")
        hs_all = sorted(info["history"], key=lambda h: h.get("at", ""), reverse=True)
        items = [f"{str(h.get('at', ''))[:16].replace('T', ' ')} {h.get('rewardType', '?')} {h.get('amount', '?')}"
                 f" ({h.get('tier', '')})" for h in hs_all]
        hist_lines[i] = f"{info['addr']} | {len(items)} lần bốc | " + (" ; ".join(items) if items else "chưa bốc")
        if SHOW_HISTORY and info["history"]:
            hs = sorted(info["history"], key=lambda h: h.get("at", ""), reverse=True)[:HISTORY_LIMIT]
            for h in hs:
                print(f"      · {str(h.get('at', ''))[:19].replace('T', ' ')}  {h.get('rewardType', '?'):<7} "
                      f"{h.get('amount', '?'):>10}  ({h.get('tier', '')}) {h.get('label', '')}")
    print("-" * 70)
    print(f"{'TỔNG':>3} {ok}/{len(lines)} ví {fmt(tot_p, 0):>12} {fmt(tot_s, 4):>14} {fmt(tot_b, 4):>10}")

    if WRITE_FILES:
        # ghi đè mỗi lần chạy; dòng i luôn là ví thứ i trong tokens.txt
        F_REPORT.write_text("\n".join(bal_lines) + "\n", encoding="utf-8")
        F_HISTORY.write_text("\n".join(hist_lines) + "\n", encoding="utf-8")
        print(f"Đã ghi {F_REPORT.name} và {F_HISTORY.name} ({len(lines)} dòng, mỗi ví một dòng cố định)")

    # token đã được ghi ngay vào tokens.txt mỗi lần gia hạn (tokenstore), không cần ghi lại ở cuối
    print(f"Xong trong {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
