"""
TOOL LOGIN & LINK X SAKURA FIELDS
- Dùng chung tokenstore.py với quest.py / balance.py / draw.py:
  + Đọc token mới nhất từ tokens.txt trước khi kiểm tra/gia hạn.
  + Chỉ gia hạn khi access token hết hạn (tokenstore.access_valid).
  + Ghi NGAY token mới sau mỗi lần gia hạn/login bằng tokenstore.save (có khóa .lock chống đè giữa các tool).
  + Khởi tạo khung dòng tokens.txt khớp cố định 1-1 với wallet.txt và bỏ khối ghi đè ở cuối lượt.
- Tự động thay thế Cookie X bị suspend/die từ Cookies-X-thay-the.txt vào đúng dòng bị lỗi trong Cookies-X.txt.
- Hỗ trợ để quest.py gọi sang khi có 1 ví bị die token (hàm relogin_single_wallet hoặc py login.py --idx <dòng>).
"""
import os, sys, time, threading, json, re, argparse, random
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests
from eth_account import Account
from eth_account.messages import encode_defunct
import tokenstore

for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

BASE = Path(__file__).parent
FILE_WALLETS           = BASE / "wallet.txt"
FILE_TOKENS            = BASE / "tokens.txt"
FILE_COOKIES_X         = BASE / "Cookies-X.txt"
FILE_COOKIES_X_REPLACE = BASE / "Cookies-X-thay-the.txt"
FILE_PROXY             = BASE / "proxy.txt"
FILE_2CAPTCHA          = BASE / "2captcha.txt"
FILE_CONFIG            = BASE / "quest_config.json"

FILE_LOG               = BASE / "log.txt"
FILE_RESULT            = BASE / "result.txt"
FILE_ERROR             = BASE / "error_wallets.txt"
FILE_LINKED_X          = BASE / "linked_x.txt"

API = os.environ.get("SAKURA_API", "https://api.sakurafields.com")

DEFAULT_CONFIG = {
    "ghi_file": {"log": False, "result": False, "error": False, "linked_x": False},
    "he_thong": {"threads": 5, "max_retry": 1, "timeout": 30, "delay_between": 0, "dry_run": False},
    "login_va_x": {
        "goi_login_khi_token_die": True,
        "do_link_x": True,
        "always_solve_captcha": True,
        "sitekey": "0x4AAAAAAE_-UaxUbwRXSJtL",
        "pageurl": "https://sakurafields.com/"
    }
}


def load_quest_config():
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        if FILE_CONFIG.exists():
            user = json.loads(FILE_CONFIG.read_text(encoding="utf-8-sig"))
            for k, v in user.items():
                if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                    cfg[k].update(v)
                else:
                    cfg[k] = v
    except Exception:
        pass
    return cfg


CFG = load_quest_config()
WRITE = CFG.get("ghi_file", {})
SYS_CFG = CFG.get("he_thong", {})
LOGIN_CFG = CFG.get("login_va_x", {})

THREADS              = int(SYS_CFG.get("threads", 5))
MAX_RETRY            = int(SYS_CFG.get("max_retry", 1))
TIMEOUT              = int(SYS_CFG.get("timeout", 30))
DELAY_BETWEEN        = float(SYS_CFG.get("delay_between", 0))
DRY_RUN              = bool(SYS_CFG.get("dry_run", False))

DO_LINK_X            = bool(LOGIN_CFG.get("do_link_x", True))
ALWAYS_SOLVE_CAPTCHA = bool(LOGIN_CFG.get("always_solve_captcha", True))
DEFAULT_SITEKEY      = str(LOGIN_CFG.get("sitekey", "0x4AAAAAAE_-UaxUbwRXSJtL"))
DEFAULT_PAGEURL      = str(LOGIN_CFG.get("pageurl", "https://sakurafields.com/"))

lock = threading.Lock()
cookie_lock = threading.Lock()

HEADERS = {
    "content-type": "application/json",
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://sakurafields.com",
    "referer": "https://sakurafields.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
}

X_BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"


class PermanentError(Exception):
    """Lỗi không retry (server từ chối, chữ ký sai...)."""


class BadCookieXError(Exception):
    """Cookie X bị suspend, khóa, die hoặc đã gắn vào ví khác -> cần lấy cookie thay thế."""


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def short(a): return f"{a[:6]}...{a[-4:]}" if a and len(a) > 12 else str(a)
def mask_proxy(p): return re.sub(r"//[^/@]*@", "//***@", p) if p else "không proxy"


def log(msg):
    line = f"[{now()}] {msg}"
    with lock:
        print(line, flush=True)
        if WRITE.get("log", False):
            with open(FILE_LOG, "a", encoding="utf-8") as f:
                f.write(line + "\n")


def append(path, text):
    key = {FILE_RESULT: "result", FILE_ERROR: "error", FILE_LINKED_X: "linked_x"}.get(path)
    if key and not WRITE.get(key, False):
        return
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")


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


def read_indexed_lines(path, required=True):
    """Đọc file giữ nguyên vị trí dòng: Ví dòng i (wallet.txt) <-> Cookie X dòng i (Cookies-X.txt)."""
    if not path.exists():
        if required: sys.exit(f"❌ Không tìm thấy file {path.name}")
        return []
    out = []
    for l in path.read_text(encoding="utf-8-sig").splitlines():
        l = l.strip()
        out.append("" if l.startswith("#") else l)
    while out and not out[-1]:
        out.pop()
    return out


def sync_wallet_order_in_tokenstore(ordered_addresses):
    """
    Đảm bảo file tokens.txt có đúng thứ tự dòng của wallet.txt ngay từ đầu (an toàn qua khóa của tokenstore),
    nhưng luôn giữ nguyên các token mới nhất đang có trong file.
    Từ đó về sau mỗi lần gia hạn/login chỉ gọi tokenstore.save() để cập nhật từng dòng tức thì.
    """
    with tokenstore._tlock, tokenstore._file_lock(FILE_TOKENS):
        existing = {}
        if FILE_TOKENS.exists():
            for l in FILE_TOKENS.read_text(encoding="utf-8-sig").splitlines():
                s = l.strip()
                if not s or s.startswith("#"):
                    continue
                p = [x.strip() for x in s.split("|")]
                if len(p) >= 3 and p[0]:
                    existing[p[0].lower()] = (p[1], p[2])
        lines = []
        for addr in ordered_addresses:
            if not addr:
                lines.append("")
                continue
            acc, ref = existing.get(addr.lower(), ("", ""))
            lines.append(f"{addr}|{acc}|{ref}")
        tmp = Path(str(FILE_TOKENS) + ".tmp")
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(tmp, FILE_TOKENS)


def parse_x_cookie(raw_line):
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


PROXIES = [p if "://" in p else "http://" + p for p in read_lines(FILE_PROXY, required=False)]
COOKIES_X_LINES = read_indexed_lines(FILE_COOKIES_X, required=False)
COOKIES_REPLACE_QUEUE = deque(read_lines(FILE_COOKIES_X_REPLACE, required=False))


def proxy_for(i): return PROXIES[i % len(PROXIES)] if PROXIES else None


def cookie_x_for(i):
    with cookie_lock:
        return COOKIES_X_LINES[i] if i < len(COOKIES_X_LINES) else ""


def _save_cookies_files_locked():
    """Ghi cập nhật cả 2 file Cookies-X.txt (theo đúng dòng) và Cookies-X-thay-the.txt (đã xóa dòng vừa nhặt)."""
    tmp_main = FILE_COOKIES_X.with_name(f"Cookies-X.{os.getpid()}.{threading.get_ident()}.tmp")
    content_main = "\n".join(COOKIES_X_LINES) + ("\n" if COOKIES_X_LINES else "")
    tmp_main.write_text(content_main, encoding="utf-8")
    os.replace(tmp_main, FILE_COOKIES_X)

    if FILE_COOKIES_X_REPLACE.exists() or COOKIES_REPLACE_QUEUE:
        tmp_rep = FILE_COOKIES_X_REPLACE.with_name(f"Cookies-X-thay-the.{os.getpid()}.{threading.get_ident()}.tmp")
        content_rep = "\n".join(COOKIES_REPLACE_QUEUE) + ("\n" if COOKIES_REPLACE_QUEUE else "")
        tmp_rep.write_text(content_rep, encoding="utf-8")
        os.replace(tmp_rep, FILE_COOKIES_X_REPLACE)


def replace_suspended_cookie_x(idx):
    """
    Nhặt 1 cookie mới hợp lệ từ Cookies-X-thay-the.txt, xóa khỏi file thay thế,
    và thế chỗ vào đúng dòng idx trong Cookies-X.txt.
    """
    with cookie_lock:
        while COOKIES_REPLACE_QUEUE:
            new_raw = COOKIES_REPLACE_QUEUE.popleft().strip()
            auth_token, ct0 = parse_x_cookie(new_raw)
            if not auth_token:
                log(f"⚠️ Bỏ qua dòng không hợp lệ trong {FILE_COOKIES_X_REPLACE.name}: {new_raw[:30]}...")
                _save_cookies_files_locked()
                continue

            while len(COOKIES_X_LINES) <= idx:
                COOKIES_X_LINES.append("")

            COOKIES_X_LINES[idx] = new_raw
            _save_cookies_files_locked()
            return new_raw, auth_token, ct0

        return None, None, None


def load_2captcha_key():
    if FILE_2CAPTCHA.exists():
        lines = read_lines(FILE_2CAPTCHA, required=False)
        if lines:
            return lines[0]
    sys.exit("❌ Không tìm thấy API key trong 2captcha.txt")


def check_is_linked(xs_data):
    if not isinstance(xs_data, dict):
        return None
    for k in ("linked", "connected", "isLinked", "hasX"):
        if isinstance(xs_data.get(k), bool):
            return xs_data[k]
    if xs_data.get("username") or xs_data.get("xUsername") or xs_data.get("twitterUsername"):
        return True
    return None


def solve_turnstile_captcha(sess):
    """Giải Turnstile captcha bằng 2Captcha."""
    api_key = load_2captcha_key()
    try:
        task_data = {
            "key": api_key,
            "method": "turnstile",
            "sitekey": DEFAULT_SITEKEY,
            "pageurl": DEFAULT_PAGEURL,
            "json": 1
        }
        r = sess.post("https://2captcha.com/in.php", data=task_data, timeout=TIMEOUT)
        result = r.json()
        if result.get("status") != 1:
            return None, f"Captcha submission failed: {result}"

        task_id = result.get("request")
        max_wait = 120
        start_time = time.time()

        while (time.time() - start_time) < max_wait:
            time.sleep(5)
            poll_params = {"key": api_key, "action": "get", "id": task_id, "json": 1}
            r_poll = sess.get("https://2captcha.com/res.php", params=poll_params, timeout=TIMEOUT)
            poll_result = r_poll.json()

            if poll_result.get("status") == 1:
                token = poll_result.get("request")
                if token and isinstance(token, str):
                    return token, None
            elif poll_result.get("request") == "CAPCHA_NOT_READY":
                continue
            else:
                return None, f"Captcha failed: {poll_result}"

        return None, "Captcha timeout (quá 120s)"
    except Exception as e:
        return None, str(e)


def with_retry(fn):
    for n in range(1, MAX_RETRY + 1):
        try: return fn()
        except PermanentError: raise
        except Exception as e:
            if n == MAX_RETRY: raise
            time.sleep(8 * n if "429" in str(e) else min(0.7 * n, 3))


def check_or_refresh_token(sess, refresh):
    if not refresh:
        return None
    r = sess.post(
        f"{API}/api/session/refresh",
        json={"refreshToken": refresh},
        headers=HEADERS,
        timeout=TIMEOUT
    )
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"refresh HTTP {r.status_code}")
    if r.status_code == 200:
        try: d = r.json()
        except Exception: d = {}
        if d.get("accessToken"):
            return (d["accessToken"], d.get("refreshToken") or refresh)
    return None


def login_or_refresh(sess, idx, total, secret, address, force_login=False):
    # Đọc token mới nhất từ file thông qua tokenstore (phòng khi tool khác vừa gia hạn)
    cur = tokenstore.latest(FILE_TOKENS, address)
    access, refresh = cur if cur else ("", "")

    if not force_login:
        # Nếu access token còn hạn thì dùng luôn, KHÔNG gọi refresh thừa
        if access and tokenstore.access_valid(access):
            return (access, refresh), "Token còn hạn (dùng luôn)"

        # Nếu access token hết hạn nhưng có refresh token -> thử gia hạn
        if refresh:
            refreshed = check_or_refresh_token(sess, refresh)
            if refreshed:
                # Ghi ngay lập tức xuống tokens.txt (có khóa .lock chống đè)
                tokenstore.save(FILE_TOKENS, address, refreshed[0], refreshed[1])
                return refreshed, "Token còn sống (vừa gia hạn)"

            # Kiểm tra lại file 1 lần nữa phòng trường hợp tool khác vừa xoay token cùng lúc
            cur_after = tokenstore.latest(FILE_TOKENS, address)
            if cur_after and cur_after[0] != access and tokenstore.access_valid(cur_after[0]):
                return cur_after, "Token còn hạn (tool khác vừa gia hạn)"

            log(f"[{idx+1}/{total}] {short(address)} | ⚠️ Token cũ đã die -> Tiến hành login lại...")

    # 1. Lấy nonce
    r = sess.get(f"{API}/api/auth/wallet/nonce", params={"address": address}, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code in (429, 500, 502, 503, 504): raise RuntimeError(f"nonce HTTP {r.status_code}")
    if not r.ok: raise PermanentError(f"nonce HTTP {r.status_code}: {r.text[:80]}")
    
    message = r.json().get("message")
    if not message: raise PermanentError("nonce không trả về message")

    # 2. Ký message
    sig = Account.sign_message(encode_defunct(text=message), private_key=secret).signature.hex()
    if not sig.startswith("0x"): sig = "0x" + sig
    
    if DRY_RUN:
        return None, "DRY_RUN - đã lấy nonce và ký thử"

    verify_payload = {
        "address": address,
        "signature": sig
    }

    # 3. Giải Captcha
    if ALWAYS_SOLVE_CAPTCHA:
        log(f"[{idx+1}/{total}] {short(address)} | ⏳ Đang giải Turnstile Captcha...")
        captcha_token, captcha_error = solve_turnstile_captcha(sess)
        if not captcha_token:
            raise RuntimeError(f"Lỗi giải captcha: {captcha_error}")
        verify_payload["turnstileToken"] = captcha_token

    # 4. Gửi request Verify
    r = sess.post(f"{API}/api/auth/wallet/verify", json=verify_payload, headers=HEADERS, timeout=TIMEOUT)
    
    if not r.ok and not ALWAYS_SOLVE_CAPTCHA:
        error_text = r.text
        if "captcha" in error_text.lower() or "turnstile" in error_text.lower():
            log(f"[{idx+1}/{total}] {short(address)} | ⏳ Phát hiện yêu cầu Captcha - Đang giải...")
            captcha_token, captcha_error = solve_turnstile_captcha(sess)
            if captcha_token:
                verify_payload["turnstileToken"] = captcha_token
                r = sess.post(f"{API}/api/auth/wallet/verify", json=verify_payload, headers=HEADERS, timeout=TIMEOUT)
            else:
                raise RuntimeError(f"Lỗi giải captcha: {captcha_error}")

    if r.status_code in (429, 500, 502, 503, 504): raise RuntimeError(f"verify HTTP {r.status_code}")
    try: data = r.json()
    except Exception: data = {}
    
    if not r.ok or not data.get("accessToken"):
        err_detail = data.get("error") or data.get("message") or r.text
        raise PermanentError(f"verify bị từ chối HTTP {r.status_code}: {str(err_detail)[:80]}")
        
    new_tokens = (data["accessToken"], data.get("refreshToken") or "")
    # Ghi ngay token mới vào đúng dòng của ví trong tokens.txt (có khóa file .lock)
    tokenstore.save(FILE_TOKENS, address, new_tokens[0], new_tokens[1])
    return new_tokens, "Đăng nhập mới thành công"


def api_call_with_auth(sess, method, path, address, access_token, refresh_token, body=None, params=None):
    """Gọi API kèm token; nếu 401 thì đọc lại token mới nhất hoặc gia hạn ngay qua tokenstore."""
    def _send(tok):
        h = {**HEADERS, "Authorization": f"Bearer {tok}"}
        kwargs = {"headers": h, "timeout": TIMEOUT, "params": params}
        if method != "GET" and body is not None:
            kwargs["json"] = body
        return sess.request(method, API + path, **kwargs)

    r = _send(access_token)
    if r.status_code == 401:
        cur = tokenstore.latest(FILE_TOKENS, address)
        if cur and cur[0] and cur[0] != access_token and tokenstore.access_valid(cur[0]):
            access_token, refresh_token = cur
        else:
            ref = cur[1] if cur and cur[1] else refresh_token
            refreshed = check_or_refresh_token(sess, ref)
            if refreshed:
                access_token, refresh_token = refreshed
                tokenstore.save(FILE_TOKENS, address, access_token, refresh_token)
        r = _send(access_token)

    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"HTTP {r.status_code} {path}")
    try: data = r.json()
    except Exception: data = {}
    return r.status_code, data, access_token, refresh_token


def link_x_flow(sess, address, access_token, refresh_token, proxy, auth_token, ct0=None):
    code_start, d_start, access_token, refresh_token = api_call_with_auth(
        sess, "POST", "/api/x/link-start", address, access_token, refresh_token, body=None
    )
    if code_start != 200 or not isinstance(d_start, dict) or not d_start.get("url"):
        raise RuntimeError(f"link-start lỗi HTTP {code_start}: {str(d_start)[:80]}")
    oauth_url = d_start["url"]

    xsess = requests.Session()
    if proxy:
        xsess.proxies = {"http": proxy, "https": proxy}

    for dom in (".x.com", ".twitter.com", "api.x.com"):
        xsess.cookies.set("auth_token", auth_token, domain=dom)
        if ct0:
            xsess.cookies.set("ct0", ct0, domain=dom)

    ua = HEADERS["user-agent"]
    if not ct0:
        xsess.get("https://x.com", headers={"user-agent": ua}, timeout=TIMEOUT)
        ct0 = xsess.cookies.get("ct0", domain=".x.com") or xsess.cookies.get("ct0")
        if not ct0:
            raise BadCookieXError("Cookie X die hoặc không lấy được ct0")

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

    if r1.status_code in (400, 401, 403):
        raise BadCookieXError(f"Cookie X bị suspend/die (HTTP {r1.status_code})")
    if not r1.ok:
        raise RuntimeError(f"X OAuth2 GET lỗi {r1.status_code}: {r1.text[:80]}")

    d1 = r1.json()
    auth_code = d1.get("auth_code")
    if not auth_code:
        raise BadCookieXError("Cookie X bị suspend/khóa (không có auth_code)")

    r2 = xsess.post(
        api_url,
        headers={**x_headers, "content-type": "application/x-www-form-urlencoded"},
        data={"approval": "true", "code": auth_code, "consent_flow": "web_consent"},
        timeout=TIMEOUT
    )
    if r2.status_code in (400, 401, 403):
        raise BadCookieXError(f"Cookie X bị suspend/từ chối khi Approve (HTTP {r2.status_code})")
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

    cb_headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://sakurafields.com/",
        "origin": "https://sakurafields.com",
        "user-agent": ua,
    }
    r_cb = sess.get(
        f"{API}/api/auth/x/callback",
        params={"code": final_code, "state": final_state},
        headers=cb_headers,
        allow_redirects=False,
        timeout=TIMEOUT
    )

    code_st, xs, _, _ = api_call_with_auth(sess, "GET", "/api/x/status", address, access_token, refresh_token)
    if code_st == 200 and check_is_linked(xs) is True:
        return xs

    if r_cb.status_code in (200, 201, 204, 301, 302, 303, 307, 308):
        loc = r_cb.headers.get("Location", "")
        if "error" not in loc.lower():
            try: return r_cb.json()
            except Exception: return {}

    try: err_json = r_cb.json()
    except Exception: err_json = {}
    err_msg = str(err_json.get("error") or err_json.get("message") or r_cb.text[:80])

    if r_cb.status_code in (401, 403, 409) or any(k in err_msg.lower() for k in ("already_linked", "suspended", "locked", "denied")):
        raise BadCookieXError(f"Callback từ chối X ({err_msg[:50]})")

    raise RuntimeError(f"Callback HTTP {r_cb.status_code}: {err_msg}")


def run_wallet(idx, total, key, address, proxy, force_login=False):
    sess = requests.Session()
    if proxy:
        sess.proxies = {"http": proxy, "https": proxy}

    tokens, login_note = login_or_refresh(sess, idx, total, key, address, force_login=force_login)
    notes = [login_note]

    if not tokens:
        return notes, None, None

    access_token, refresh_token = tokens[0], tokens[1]

    code_st, xs, access_token, refresh_token = api_call_with_auth(
        sess, "GET", "/api/x/status", address, access_token, refresh_token
    )
    linked = check_is_linked(xs) if code_st == 200 else None

    if linked is True:
        uname = xs.get("username") or xs.get("xUsername") or ""
        notes.append(f"X: ĐÃ link ({'@' + uname if uname else 'OK'})")
    elif linked is False:
        if DO_LINK_X and not DRY_RUN:
            raw_cookie = cookie_x_for(idx)
            auth_token, ct0 = parse_x_cookie(raw_cookie)
            replaced_count = 0

            if not auth_token:
                log(f"[{idx+1}/{total}] {short(address)} | ⚠️ Dòng {idx+1} trong Cookies-X.txt trống/lỗi -> Đang lấy cookie từ {FILE_COOKIES_X_REPLACE.name}...")
                raw_cookie, auth_token, ct0 = replace_suspended_cookie_x(idx)
                if auth_token:
                    replaced_count += 1

            if not auth_token:
                notes.append(f"X: CHƯA link (dòng {idx+1} thiếu cookie & kho thay thế đã hết)")
            else:
                while auth_token:
                    try:
                        cb_data = link_x_flow(sess, address, access_token, refresh_token, proxy, auth_token, ct0)
                        linked = True
                        uname = (cb_data.get("username") or cb_data.get("xUsername") if isinstance(cb_data, dict) else "") or ""
                        rep_tag = f" [đã thay {replaced_count} cookie mới]" if replaced_count > 0 else ""
                        notes.append(f"X: Vừa link OK! {'(@' + uname + ')' if uname else ''}{rep_tag}".strip())
                        append(FILE_LINKED_X, f"{address}|{uname}|{raw_cookie}")
                        break
                    except BadCookieXError as bce:
                        log(
                            f"[{idx+1}/{total}] {short(address)} | ⚠️ Cookie X dòng {idx+1} lỗi ({bce}) "
                            f"-> Đang nhặt cookie mới từ {FILE_COOKIES_X_REPLACE.name}..."
                        )
                        raw_cookie, auth_token, ct0 = replace_suspended_cookie_x(idx)
                        if not auth_token:
                            notes.append(f"X: lỗi ({bce}) & ĐÃ HẾT cookie trong {FILE_COOKIES_X_REPLACE.name}")
                            break
                        replaced_count += 1
                        log(f"[{idx+1}/{total}] {short(address)} | 🔄 Đã thay thế cookie mới vào dòng {idx+1}, đang thử link X lại...")
                    except Exception as e:
                        notes.append(f"X: lỗi link ({str(e)[:75]})")
                        break
        else:
            notes.append("X: CHƯA link (đang tắt do_link_x)" if not DO_LINK_X else "X: CHƯA link")
    else:
        notes.append(f"X: không rõ (HTTP {code_st})")

    return notes, linked, (access_token, refresh_token)


def relogin_single_wallet(target_idx, target_address=None):
    """
    Hàm chuyên dụng để quest.py gọi sang khi 1 ví bị die token.
    Đọc đúng dòng ví trong wallet.txt, giải Captcha login mới, lưu tức thì qua tokenstore.save(),
    và trả về (new_access, new_refresh).
    """
    wallets = read_indexed_lines(FILE_WALLETS, required=True)
    ordered_addresses = []
    for w in wallets:
        if not w:
            ordered_addresses.append("")
            continue
        try:
            k = w if w.startswith("0x") else "0x" + w
            ordered_addresses.append(Account.from_key(k).address)
        except Exception:
            ordered_addresses.append("")

    idx = target_idx
    if target_address:
        for i, a in enumerate(ordered_addresses):
            if a and a.lower() == target_address.lower():
                idx = i
                break

    if idx < 0 or idx >= len(wallets) or not wallets[idx]:
        raise PermanentError(f"Không tìm thấy private key tại dòng {idx+1} trong wallet.txt")

    secret = wallets[idx]
    key = secret if secret.startswith("0x") else "0x" + secret
    address = Account.from_key(key).address
    proxy = proxy_for(idx)
    total = len(wallets)

    notes, _, tokens = with_retry(
        lambda: run_wallet(idx, total, key, address, proxy, force_login=True)
    )
    log(f"[{idx+1}/{total}] {short(address)} | 🔄 [LOGIN.PY] " + " | ".join(notes))
    return tokens


PROXY_OVERRIDE = {}   # idx -> proxy mới (khi chạy lại ví lỗi do proxy)
LAST_ERROR = {}       # idx -> nội dung lỗi lần chạy gần nhất
PROXY_ERR_KEYS = ("proxy", "tunnel", "connecttimeout", "connection", "timed out", "timeout",
                  "ssl", "max retries", "407", "502", "503", "504", "reset by peer", "remote end closed")


def is_proxy_error(msg):
    m = (msg or "").lower()
    return any(k in m for k in PROXY_ERR_KEYS)


PROXY_STATUS = {}     # proxy -> True (live) / False (chết), nhớ lại để khỏi test lặp
proxy_status_lock = threading.Lock()


def check_proxy_live(p, timeout=10):
    """Test proxy bằng cách gọi thử tới server Sakura; có phản hồi HTTP là proxy sống."""
    with proxy_status_lock:
        if p in PROXY_STATUS:
            return PROXY_STATUS[p]
    try:
        requests.get(API, proxies={"http": p, "https": p}, timeout=timeout)
        live = True
    except Exception:
        live = False
    with proxy_status_lock:
        PROXY_STATUS[p] = live
    return live


def pick_new_proxy(current):
    """Nhặt random 1 proxy khác đang LIVE; proxy chết bị đánh dấu và bỏ qua."""
    if current:
        with proxy_status_lock:
            PROXY_STATUS[current] = False
    candidates = [p for p in PROXIES if p != current and PROXY_STATUS.get(p) is not False]
    random.shuffle(candidates)
    for p in candidates:
        if check_proxy_live(p):
            return p
    return None


def process(idx, total, secret, force_login=False):
    if not secret:
        return False, None
    proxy = PROXY_OVERRIDE.get(idx) or proxy_for(idx)
    key = secret if secret.startswith("0x") else "0x" + secret
    try:
        address = Account.from_key(key).address
    except Exception:
        log(f"[{idx+1}/{total}] ❌ Dòng {idx+1} không phải private key hợp lệ")
        append(FILE_ERROR, secret)
        LAST_ERROR[idx] = "invalid key"
        return False, None

    try:
        notes, linked, _ = with_retry(
            lambda: run_wallet(idx, total, key, address, proxy, force_login=force_login)
        )
        summary = " | ".join(notes)
        log(f"[{idx+1}/{total}] {short(address)} | ✅ {summary} | {mask_proxy(proxy)}")
        append(FILE_RESULT, f"{now()} | {address} | OK | {summary}")
        return True, linked
    except Exception as e:
        LAST_ERROR[idx] = str(e)
        msg = str(e)[:150]
        log(f"[{idx+1}/{total}] {short(address)} | ❌ {msg}")
        append(FILE_RESULT, f"{now()} | {address} | LỖI | {msg}")
        append(FILE_ERROR, secret)
        return False, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--idx", type=int, default=None, help="Chỉ login lại đúng dòng ví (bắt đầu từ 0)")
    parser.add_argument("--force", action="store_true", help="Bắt buộc giải captcha login mới")
    args = parser.parse_args()

    if args.idx is not None:
        relogin_single_wallet(args.idx)
        return

    wallets = read_indexed_lines(FILE_WALLETS, required=True)
    if not any(wallets):
        sys.exit("❌ wallet.txt rỗng")
    if not DRY_RUN:
        load_2captcha_key()

    ordered_addresses = []
    for w in wallets:
        if not w:
            ordered_addresses.append("")
            continue
        try:
            k = w if w.startswith("0x") else "0x" + w
            ordered_addresses.append(Account.from_key(k).address)
        except Exception:
            ordered_addresses.append("")

    # Đồng bộ khung thứ tự dòng của tokens.txt theo wallet.txt ngay từ đầu (có khóa .lock, giữ nguyên các token sống)
    if not DRY_RUN:
        sync_wallet_order_in_tokenstore(ordered_addresses)

    if WRITE.get("error", False):
        FILE_ERROR.write_text("", encoding="utf-8")

    total = len(wallets)
    threads = max(1, min(THREADS, total))
    log(
        f"Bắt đầu LOGIN: {total} ví | {threads} luồng | "
        f"{len(PROXIES) or 'không'} proxy | {len(COOKIES_REPLACE_QUEUE)} cookie X dự phòng | "
        f"Link X: {'BẬT' if DO_LINK_X else 'TẮT'} | DRY_RUN={DRY_RUN}"
    )
    if not PROXIES:
        log("Không có proxy.txt -> dùng IP máy")

    def run_batch(indices, force):
        """Chạy các ví trong indices. Trả về (ok, no_x, danh sách idx lỗi)."""
        ok, no_x, failed = 0, 0, []
        try:
            with ThreadPoolExecutor(max_workers=threads) as ex:
                futs = {}
                for i in indices:
                    futs[ex.submit(process, i, total, wallets[i], force)] = i
                    if DELAY_BETWEEN:
                        time.sleep(DELAY_BETWEEN)
                for f in as_completed(futs):
                    success, linked = f.result()
                    if success:
                        ok += 1
                        if linked is False:
                            no_x += 1
                    else:
                        failed.append(futs[f])
        except KeyboardInterrupt:
            log("⚠️ Đã dừng bằng Ctrl+C")
        return ok, no_x, sorted(failed)

    t0 = time.time()
    all_idx = [i for i, w in enumerate(wallets) if w]
    ok, no_x, failed = run_batch(all_idx, args.force)

    # Token đã được ghi ngay lập tức vào tokens.txt sau mỗi lần gia hạn/login qua tokenstore.save()
    log(
        f"🎉 XONG: {ok}/{total} thành công, {len(failed)} lỗi, {no_x} ví chưa link X, "
        f"còn {len(COOKIES_REPLACE_QUEUE)} cookie dự phòng, {time.time()-t0:.1f}s"
    )

    # Hỏi chạy lại ví lỗi
    while failed:
        try:
            ans = input(f"\n👉 Nhấn Enter để chạy lại {len(failed)} ví lỗi (gõ n rồi Enter để thoát): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break
        if ans in ("n", "no", "q", "exit"):
            break

        proxy_fail = [i for i in failed if PROXIES and is_proxy_error(LAST_ERROR.get(i))]
        if proxy_fail:
            log(f"🔎 Đang tìm proxy live thay cho {len(proxy_fail)} ví lỗi proxy...")

        def swap(i):
            new = pick_new_proxy(PROXY_OVERRIDE.get(i) or proxy_for(i))
            if new:
                PROXY_OVERRIDE[i] = new
            return bool(new)

        with ThreadPoolExecutor(max_workers=threads) as ex:
            swapped = sum(ex.map(swap, proxy_fail))
        dead = sum(1 for v in PROXY_STATUS.values() if v is False)
        log(
            f"🔁 Chạy lại {len(failed)} ví lỗi ({swapped}/{len(proxy_fail)} ví đã đổi sang proxy live, "
            f"{dead}/{len(PROXIES)} proxy bị đánh dấu chết)"
        )
        if WRITE.get("error", False):
            FILE_ERROR.write_text("", encoding="utf-8")
        t1 = time.time()
        ok2, no_x2, failed = run_batch(failed, args.force)
        log(
            f"🎉 XONG LẦN CHẠY LẠI: +{ok2} thành công, {len(failed)} còn lỗi, {no_x2} ví chưa link X, "
            f"còn {len(COOKIES_REPLACE_QUEUE)} cookie dự phòng, {time.time()-t1:.1f}s"
        )


if __name__ == "__main__":
    main()