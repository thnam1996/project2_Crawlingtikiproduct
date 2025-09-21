# project2_Crawlingtikiproduct
Project Python để crawl thông tin sản phẩm từ API Tiki, hỗ trợ:
- Crawl theo danh sách ID (input CSV).
- Lưu kết quả thành nhiều file JSON (theo 1000 id/batch).
- Checkpoint CSV để resume khi bị ngừng chạygiữa chừng.
- Retry lại các item fail

# Cấu trúc folder
project2/

├─ app.py             # main crawler

├─ config.py          # config (API, batch size, ...) => adjust các thông số ở đây

├─ io_utils.py        # đọc/lưu batch JSON, đọc CSV

├─ checkpoint.py      # quản lý checkpoint.csv

├─ extractor.py       # chuẩn hoá description, chọn field

├─ fetcher.py         # fetch API (async, retry)

├─ retry_job.py       # retry lại các ID fail

├─ raw

│  └─ productid.csv   # input ID list

├─ data               # output file json

└─ README.md

# Cách chạy
- Input đường dẫn file csv và các thông số khác ở config.py
- Chạy file app.py

# Tự động chạy lại khi có sự cố bằng Supervisor
Ở Terminal

Tạo file:
<pre> sudo nano /etc/supervisor/conf.d/tiki-scraper.conf </pre>

Config:
<pre>
[program:tiki-scraper]
directory=/home/thien-nam/data_engineer/project2
command=/bin/bash -lc '/home/thien-nam/data_engineer/project2/.venv/bin/python -u app.py'
user=thien-nam

autostart=false                
autorestart=unexpected         
exitcodes=0                     
startsecs=0                     

stdout_logfile=/home/thien-nam/data_engineer/project2/logs/out.log
stderr_logfile=/home/thien-nam/data_engineer/project2/logs/err.log
  
  </pre>

Apply config
<pre>
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start tiki_crawl
sudo supervisorctl status tiki_crawl</pre>

Xem tracklog
<pre>
sudo supervisorctl tail -f tiki-scraper
sudo supervisorctl tail -f tiki-scraper stderr
</pre>
