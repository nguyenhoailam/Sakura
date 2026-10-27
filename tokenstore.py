"""
Kho token dùng chung cho quest.py / balance.py / draw.py.
- save(): ghi NGAY token mới của 1 ví vào tokens.txt (đọc-sửa-ghi nguyên tử, có khóa giữa các luồng và giữa các tool).
- latest(): đọc token mới nhất của ví (phòng khi tool khác vừa gia hạn).
- access_valid(): kiểm tra access token còn hạn không (đọc exp trong JWT) để khỏi gia hạn thừa.
Không in token ra màn hình.
"""
import os, time, json, base64, threading
from contextlib import contextmanager
from pathlib import Path

_tlock = threading.Lock()


@contextmanager
def _file_lock(path, timeout=15.0, stale=30.0):
    lock_path = str(path) + ".lock"
    t0 = time.time()
    fd = None
    while fd is None:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:                                   # khóa bỏ quên quá lâu (tool trước bị tắt đột ngột) thì phá
                if time.time() - os.path.getmtime(lock_path) > stale: os.remove(lock_path)
            except OSError: pass
            if time.time() - t0 > timeout: raise TimeoutError("không lấy được khóa tokens.txt")
            time.sleep(0.05)
    try:
        yield
    finally:
        try: os.close(fd)
        except OSError: pass
        try: os.remove(lock_path)
        except OSError: pass


def _read(path):
    if not Path(path).exists(): return []
    return [l.rstrip("\r\n") for l in Path(path).read_text(encoding="utf-8-sig").splitlines()]


def latest(path, addr):
    """Trả về (access, refresh) mới nhất của ví trong file, hoặc None."""
    with _tlock:
        for l in _read(path):
            p = l.strip().split("|")
            if len(p) >= 3 and p[0].strip().lower() == addr.lower():
                return p[1].strip(), p[2].strip()
    return None


def save(path, addr, access, refresh):
    """Ghi ngay token mới của ví vào đúng dòng của nó, giữ nguyên các dòng khác."""
    with _tlock, _file_lock(path):
        lines, hit = _read(path), False
        for i, l in enumerate(lines):
            p = l.strip().split("|")
            if len(p) >= 3 and p[0].strip().lower() == addr.lower():
                lines[i] = f"{p[0].strip()}|{access}|{refresh}"; hit = True; break
        if not hit: lines.append(f"{addr}|{access}|{refresh}")
        tmp = Path(str(path) + ".tmp")
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(tmp, path)


def access_valid(token, margin=90):
    """True nếu access token (JWT) còn ít nhất `margin` giây."""
    try:
        p = token.split(".")[1]; p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p)).get("exp", 0) - time.time() > margin
    except Exception:
        return False
