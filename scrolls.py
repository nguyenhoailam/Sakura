"""
TOOL SCROLLS (SET THẺ) + CLAIM THE PASS - SAKURA FIELDS
Mỗi ví, theo thứ tự:
  1. Mở các set đang có sẵn (held).
  2. Mua set theo lượng Petals:
       - Đủ mua mỗi loại 1 set (tổng giá tất cả set)  -> mua mỗi set 1 lần.
       - Ít hơn -> mua NGẪU NHIÊN trong các set mua nổi (kể cả set 20k), lặp tới khi Petals < MIN_PETALS_TO_BUY.
       - Chỉ vừa đủ mua set rẻ nhất (dưới MIN_PETALS_TO_BUY) -> KHÔNG mua.
  3. Mở các set vừa mua.
  4. Claim các mốc The Pass (season) đã đủ điểm.
Cần cùng thư mục: tokens.txt (địa chỉ|accessToken|refreshToken), proxy.txt (không bắt buộc), tokenstore.py
Chạy: py scrolls.py      (chạy thật luôn, không hỏi gì; thêm --dry để chỉ xem kế hoạch, thêm số để giới hạn số ví, vd: py scrolls.py 10)
Kết quả: log_scrolls.txt, result_scrolls.txt (bật/tắt ở WRITE_LOG / WRITE_RESULT)
"""
import os, sys, time, random, threading, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests
import tokenstore

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

# ======================= CẤU HÌNH =======================
THREADS = 50               # số ví chạy cùng lúc
MAX_RETRY = 3             # số lần thử lại khi lỗi mạng/proxy
TIMEOUT = 20              # giây
DELAY_BETWEEN = 0         # giây nghỉ giữa các lần khởi động ví
DRY_RUN = False           # False = chạy thật (mua tốn Petals!). Đặt True nếu chỉ muốn in kế hoạch, hoặc chạy: py scrolls.py --dry
DO_OPEN_HELD = True       # mở các set đang có sẵn
DO_BUY = True             # mua set
MIN_PETALS_TO_BUY = 30000 # còn dưới số này thì dừng mua (nên >= giá set rẻ nhất 20k; 30000 để đúng 20k thì không mua)
KEEP_PETALS = 0           # luôn chừa lại bao nhiêu Petals sau khi mua
MAX_BUY_PER_WALLET = 20   # tối đa số set mua mỗi ví mỗi lần chạy
DO_OPEN_AFTER_BUY = True  # mua xong thì mở luôn
DO_CLAIM_PASS = True      # claim các mốc The Pass đã đủ điểm
WRITE_LOG = True          # ghi log_scrolls.txt
WRITE_RESULT = True       # ghi result_scrolls.txt
API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")
# ========================================================

BASE = Path(__file__).parent
F_TOKENS, F_PROXY = BASE / "tokens.txt", BASE / "proxy.txt"
F_LOG, F_RESULT = BASE / "log_scrolls.txt", BASE / "result_scrolls.txt"
lock = threading.Lock()
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
           "origin": "https://sakurafields.com", "referer": "https://sakurafields.com/"}


class Permanent(Exception):
    pass


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def short(a): return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else str(a)
def mask_proxy(p): return re.sub(r"//[^/@]*@", "//***@", p) if p else "không proxy"


def log(msg):
    line = f"[{now()}] {msg}"
    with lock:
        print(line, flush=True)
        if WRITE_LOG:
            with open(F_LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def log_block(lines):
    """In cả khối của một ví cùng lúc để các ví chạy song song không bị xen dòng vào nhau."""
    stamp = f"[{now()}] "
    with lock:
        for l in lines: print(stamp + l, flush=True)
        if WRITE_LOG:
            with open(F_LOG, "a", encoding="utf-8") as f:
                for l in lines: f.write(stamp + l + "\n")


def append_result(text):
    if not WRITE_RESULT: return
    with lock:
        with open(F_RESULT, "a", encoding="utf-8") as f: f.write(text + "\n")


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


def retry(fn):
    for n in range(1, MAX_RETRY + 1):
        try: return fn()
        except Permanent: raise
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
        cur = tokenstore.latest(F_TOKENS, self.addr)
        if cur: self.access, self.refresh = cur
        if not force and tokenstore.access_valid(self.access): return
        if not self.refresh: raise Permanent("thiếu refreshToken")
        code, d = self._do("POST", "/api/session/refresh", {"refreshToken": self.refresh}, auth=False)
        if code != 200 or not d.get("accessToken"):
            raise Permanent(f"gia hạn phiên thất bại HTTP {code} - cần đăng nhập lại")
        self.access = d["accessToken"]
        if d.get("refreshToken"): self.refresh = d["refreshToken"]
        tokenstore.save(F_TOKENS, self.addr, self.access, self.refresh)

    def call(self, method, path, body=None):
        code, d = self._do(method, path, body)
        if code == 401:
            self.refresh_session(force=True)
            code, d = self._do(method, path, body)
        return code, d


def get_shop(c):
    """Trả về (packs{id: giá}, held{id: số lượng}, petals)."""
    code, d = c.call("GET", "/api/duel/cards")
    if code != 200 or not isinstance(d, dict): raise RuntimeError(f"không đọc được danh sách set (HTTP {code})")
    packs = {p["id"]: int(p["price"]) for p in (d.get("packs") or []) if isinstance(p, dict) and "id" in p and "price" in p}
    held = {k: int(v) for k, v in (d.get("held") or {}).items() if int(v) > 0}
    code, st = c.call("GET", "/api/sakura/state")
    if code != 200 or not isinstance(st, dict): raise RuntimeError(f"không đọc được số dư (HTTP {code})")
    try: petals = int(float((st.get("balances") or {}).get("PETAL", 0)))
    except (TypeError, ValueError): petals = 0
    return packs, held, petals


def open_held(c, held):
    """Mở hết các set đang có. Trả về (số set đã mở, số thẻ mới, số thẻ)."""
    opened = cards = new = 0
    for pid, qty in held.items():
        for _ in range(qty):
            code, r = c.call("POST", f"/api/duel/packs/{pid}/open", {"shelfOnly": True})
            if code != 200 or not r.get("ok"):
                if str(r.get("error")) == "no_scrolls": break
                raise RuntimeError(f"mở {pid} lỗi ({str(r.get('error', code))[:40]})")
            opened += 1
            pulls = r.get("pulls") or []
            cards += len(pulls); new += sum(1 for p in pulls if p.get("isNew"))
    return opened, cards, new


def plan_buys(packs, petals):
    """Lập danh sách set sẽ mua theo luật. Trả về list id (có thể lặp)."""
    budget = petals - KEEP_PETALS
    total_all = sum(packs.values())
    if packs and budget >= total_all:                       # nhiều Petals: mỗi set 1 lần
        return list(packs.keys())
    plan = []
    while len(plan) < MAX_BUY_PER_WALLET and budget >= MIN_PETALS_TO_BUY:
        affordable = [pid for pid, price in packs.items() if price <= budget]
        if not affordable: break
        pid = random.choice(affordable)                      # kể cả set 20k nếu mua nổi
        plan.append(pid); budget -= packs[pid]
    return plan


def run_wallet(c):
    c.refresh_session()
    notes = []
    packs, held, petals = get_shop(c)
    notes.append(f"Petals {petals:,}; set đang có: {sum(held.values())}")

    if DO_OPEN_HELD and held:
        if DRY_RUN: notes.append(f"[thử] sẽ mở {sum(held.values())} set có sẵn")
        else:
            o, n, new = open_held(c, held)
            notes.append(f"đã mở {o} set ({n} thẻ, {new} mới)")

    if DO_BUY:
        plan = plan_buys(packs, petals)
        if not plan: notes.append(f"không mua (Petals chưa đủ ngưỡng {MIN_PETALS_TO_BUY:,})")
        elif DRY_RUN:
            cost = sum(packs[p] for p in plan)
            notes.append(f"[thử] sẽ mua {len(plan)} set ({cost:,} Petals): {', '.join(plan)}")
        else:
            bought, spent = [], 0
            for pid in plan:
                code, r = c.call("POST", f"/api/duel/packs/{pid}/buy", {"qty": 1})
                if code != 200 or not r.get("ok"):
                    notes.append(f"mua {pid} dừng ({str(r.get('error', code))[:40]})"); break
                bought.append(pid); spent += int(r.get("price", packs.get(pid, 0)))
            notes.append(f"đã mua {len(bought)} set, tốn {spent:,} Petals")
            if DO_OPEN_AFTER_BUY and bought:
                _, held2, _ = get_shop(c)
                o, n, new = open_held(c, held2)
                notes.append(f"mở sau khi mua: {o} set ({n} thẻ, {new} mới)")

    if DO_CLAIM_PASS:
        code, se = c.call("GET", "/api/sakura/season")
        if code != 200 or not isinstance(se, dict): notes.append(f"pass: không đọc được (HTTP {code})")
        elif not se.get("active", True): notes.append("pass: mùa không hoạt động")
        else:
            todo = [t for t in (se.get("tiers") or []) if t.get("claimable") and not t.get("claimed")]
            if not todo: notes.append(f"pass: không có mốc để claim (điểm {se.get('points', '?')})")
            for t in todo:
                if DRY_RUN: notes.append(f"pass: [thử] sẽ claim mốc {t['tier']}"); continue
                code, r = c.call("POST", f"/api/sakura/season/tiers/{t['tier']}/claim")
                notes.append(f"pass mốc {t['tier']}: " + (f"OK +{r.get('petals')} petals, +{r.get('sakura')} sakura"
                                                        if code == 200 and r.get("ok") else f"chưa được ({str(r.get('error', code))[:40]})"))
    return notes


def process(idx, total, line):
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 3 or not parts[1]:
        log(f"[{idx+1}/{total}] ❌ Dòng {idx+1} sai định dạng (cần địa chỉ|access|refresh)")
        return False
    addr, access, refresh = parts[:3]
    proxy = proxy_for(idx)
    c = Client(addr, access, refresh, proxy)
    try:
        notes = retry(lambda: run_wallet(c))
        log_block([f"[{idx+1}/{total}] {short(addr)} | ✅ {mask_proxy(proxy)}"] + [f"      - {n}" for n in notes])
        append_result(f"{now()} | {addr} | OK | " + " ; ".join(notes))
        return True
    except Exception as e:
        msg = str(e)[:150]
        log(f"[{idx+1}/{total}] {short(addr)} | ❌ {msg}")
        append_result(f"{now()} | {addr} | LỖI | {msg}")
        return False


def main():
    lines = read_lines(F_TOKENS)
    if not lines: sys.exit("❌ tokens.txt rỗng")
    global DRY_RUN
    nums = [a for a in sys.argv[1:] if a.isdigit()]
    if nums: lines = lines[:int(nums[0])]                     # py scrolls.py 10  -> chỉ chạy 10 ví đầu để thử
    if "--dry" in sys.argv: DRY_RUN = True                    # py scrolls.py --dry -> chỉ in kế hoạch, không mua/mở/claim
    if "--live" in sys.argv: DRY_RUN = False
    threads = max(1, min(THREADS, len(lines)))
    log(f"Bắt đầu SCROLLS: {len(lines)} ví | {threads} luồng | {len(PROXIES) or 'không'} proxy | DRY_RUN={DRY_RUN}")
    t0, ok = time.time(), 0
    try:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            futs = []
            for i, l in enumerate(lines):
                futs.append(ex.submit(process, i, len(lines), l))
                if DELAY_BETWEEN: time.sleep(DELAY_BETWEEN)
            for f in as_completed(futs): ok += 1 if f.result() else 0
    except KeyboardInterrupt:
        log("⚠️ Đã dừng bằng Ctrl+C")
    log(f"🎉 XONG: {ok}/{len(lines)} ví, {len(lines)-ok} lỗi, {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
