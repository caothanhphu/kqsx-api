# Hướng dẫn chạy `run_scraper_range.py`

Tập lệnh `run_scraper_range.py` giúp tự động chạy `scraper.py` theo nhiều ngày và khu vực, đồng thời tải dữ liệu lên Supabase hoặc xuất ra file `.sql`. Tài liệu này hướng dẫn bạn chuẩn bị môi trường và các lệnh thường dùng.

## 1. Chuẩn bị môi trường
- **Yêu cầu:** Python 3.11 trở lên.
- Kích hoạt môi trường ảo (khuyến nghị) và cài đặt phụ thuộc:
  ```bash
  python -m venv venv
  venv\Scripts\activate         # Windows
  # source venv/bin/activate    # macOS / Linux
  pip install -r requirements.txt
  ```
- Đảm bảo file `.env` tồn tại ở thư mục gốc dự án và chứa các biến Supabase:
  ```
  VITE_SUPABASE_URL=...
  VITE_SUPABASE_SERVICE_ROLE_KEY=...
  VITE_SUPABASE_PROJECT_ID=...
  VITE_SUPABASE_PUBLISHABLE_KEY=...  # tùy chọn
  OLLAMA_HOST=http://127.0.0.1:11434 # cần khi dùng LLM để scrape fallback
  ```
  `scraper.py` sẽ tự tải các biến này khi chạy.

## 2. Cú pháp lệnh
Chạy từ thư mục gốc dự án:
```bash
python run_scraper_range.py --start YYYY-MM-DD [--end YYYY-MM-DD] [--region mb|mt|mn ...] [--no-supabase] [--out-dir PATH] [--strict]
```

| Tham số          | Mô tả                                                                                                  |
|------------------|---------------------------------------------------------------------------------------------------------|
| `--start`        | Ngày bắt đầu (bắt buộc) theo định dạng `YYYY-MM-DD`.                                                   |
| `--end`          | Ngày kết thúc (tùy chọn). Nếu bỏ qua, script dùng ngày hiện tại.                                       |
| `--region`       | Mã khu vực cần scrape (`mb`, `mt`, `mn`). Có thể lặp lại tham số để chọn nhiều khu vực. Mặc định lấy cả 3. |
| `--no-supabase`  | Nếu đặt cờ này, script sẽ bỏ qua bước upload Supabase.                                                 |
| `--out-dir`      | Thư mục để lưu các file `.sql` theo dạng `YYYY-MM-DD_region.sql`. Tự tạo thư mục nếu chưa tồn tại.     |
| `--strict`       | Dừng ngay ở lần scrape đầu tiên thất bại thay vì tiếp tục các ngày/khu vực còn lại.                    |

Script sẽ thông báo lỗi nếu `--end` sớm hơn `--start`, hoặc ngày không đúng định dạng.

## 3. Ví dụ lệnh thường dùng
- **Scrape 7 ngày gần nhất cho cả 3 miền, upload Supabase:**
  ```bash
  python run_scraper_range.py --start 2024-10-01 --end 2024-10-07
  ```

- **Chỉ scrape miền Nam và Trung, dừng nếu có lỗi:**
  ```bash
  python run_scraper_range.py --start 2024-10-01 --end 2024-10-07 --region mn --region mt --strict
  ```

- **Xuất SQL mà không upload Supabase:**
  ```bash
  python run_scraper_range.py --start 2024-10-01 --end 2024-10-07 --no-supabase --out-dir exports/sql
  ```
  Lưu ý: nếu dùng `--no-supabase` bạn nên chỉ định `--out-dir` để tránh chạy “chay”.

## 4. Nhật ký và xử lý lỗi
- Với mỗi ngày/khu vực, terminal sẽ log `Running scraper for YYYY-MM-DD (region)...`.
- Khi dùng `--out-dir`, file `.sql` được tạo sau mỗi lần chạy thành công.
- Nếu một lần scrape thất bại, thông báo `FAILED` sẽ xuất hiện kèm lý do. Khi **không** dùng `--strict`, script tiếp tục với lượt kế tiếp và tổng hợp các lỗi vào cuối phiên.
- Để điều tra lỗi chi tiết, bạn có thể chạy trực tiếp `scraper.py` cho ngày/khu vực cụ thể với các tham số tương tự.

## 5. Mẹo vận hành
- Chạy thử với một ngày/khu vực trước khi mở rộng phạm vi để kiểm tra quyền truy cập Supabase và website nguồn.
- Khi chạy batch dài ngày, cân nhắc lưu log ra file để dễ soát lỗi:
  ```bash
  python run_scraper_range.py --start 2024-09-01 --end 2024-09-30 > logs/september.log 2>&1
  ```
- Nếu cần dùng trên máy chủ hoặc áp dụng cron, hãy đảm bảo môi trường ảo và biến môi trường được nạp trong phiên chạy cron/systemd.

Chúc bạn vận hành scraper thuận lợi!
