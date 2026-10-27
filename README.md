# Sakura Fields Tool

Tool nhiều ví cho Sakura Fields: đăng nhập, link X, xem số dư, làm nhiệm vụ hằng ngày.

## Cách dùng
1. Tải cả thư mục này về, để `sakura-tool-v1.exe` và các file `.txt` **cùng một thư mục**.
2. Điền dữ liệu vào các file `.txt` (mỗi dòng một mục):

| File | Nội dung | Bắt buộc |
|---|---|---|
| `wallet.txt` | private key của từng ví | Có |
| `2captcha.txt` | API key 2captcha (dùng khi đăng nhập) | Có |
| `Cookies-X.txt` | cookie X, dòng i ứng với ví dòng i của `wallet.txt` | Chỉ khi link X |
| `Cookies-X-thay-the.txt` | cookie X dự phòng, tự thay khi cookie chính die | Không |
| `proxy.txt` | proxy dạng `host:port` hoặc `user:pass@host:port` | Không |
| `tokens.txt` | **để trống**, tool tự tạo và cập nhật | — |

3. Bấm đúp `sakura-tool-v1.exe` (hoặc `START.BAT`), chọn:
   - `1` login: tạo token cho mọi ví (**chạy lần đầu**)
   - `2` connect-x: kiểm tra login + link X
   - `3` balance: xem số dư, lịch sử bốc thăm (ghi `balance_report.txt`)
   - `4` daily: nhiệm vụ hằng ngày (mặc định nếu không bấm gì trong 30 giây)

Token chết sẽ tự đăng nhập lại; ví lỗi tự chạy lại. Cấu hình trong `sakura_config.json`.

**Không chia sẻ** `wallet.txt`, `tokens.txt`, `Cookies-X*.txt`, `2captcha.txt` cho người khác.
