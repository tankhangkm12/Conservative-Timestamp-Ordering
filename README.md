# Project 27 – CTO Scheduler: Automated Manufacturing

## Mô tả

Hiện thực **Conservative Timestamp Ordering (CTO) Scheduler** cho bài toán
Automated Manufacturing trong môn **Hệ Cơ Sở Dữ Liệu Phân Tán**.

CTO Scheduler đóng vai trò **Global Transaction Manager** (middleware DBMS theo
mô hình Özsu & Valduriez), kiểm soát thứ tự thực thi transaction phân tán trên
nhiều Node Agent mà **không bao giờ abort** – đặc trưng then chốt cho môi trường
sản xuất yêu cầu tính nhất quán tuyệt đối. BTO (Basic Timestamp Ordering, có
abort + restart) được dùng làm baseline để so sánh.

Toàn bộ benchmark được điều khiển bằng **Streamlit** – nhập tham số, bấm nút,
không cần dòng lệnh.

**Đỗ Thanh Tân – N23DCCN122**

---

## Kiến trúc

```
┌─────────────────────────────────────────┐
│   Streamlit Dashboard (analysis/)       │  ← nhập tham số + xem kết quả/log
│   ▶️ Chạy Benchmark                     │
└───────────────┬─────────────────────────┘
                │ gọi workload/orchestrator.py
                ▼  (sinh dataset · khởi động · dọn dẹp tiến trình)
┌─────────────────────────────────────────┐
│   Workload Generator                    │  sinh T transaction đồng thời
└───────────────┬─────────────────────────┘
                │ WebSocket ws://localhost:8765
                ▼
┌─────────────────────────────────────────┐
│   CTO/BTO Scheduler   (port 8765)       │
│   ├── clock_manager.py  (Lamport clock) │
│   ├── cto_scheduler.py  (wait_queue)    │
│   └── bto_scheduler.py  (abort/restart) │
└───────┬──────────┬──────────┬───────────┘
        ▼          ▼          ▼      …  N node (chọn động)
   ┌────────┐ ┌────────┐ ┌────────┐
   │ node1  │ │ node2  │ │ node N │   mỗi node = 1 process, 1 file JSON
   └────────┘ └────────┘ └────────┘   dataset chia đều theo step_id
                       │
                       ▼ logs/result.json → Dashboard vẽ biểu đồ
```

**CTO** (Conservative Timestamp Ordering):
- Mỗi transaction nhận Lamport timestamp khi arrive.
- Buffer trong `wait_queue` đến khi Generator gửi `session_end`, rồi dispatch
  theo timestamp tăng dần → **Abort Rate = 0%** (đổi lại latency cao hơn).

**BTO** (Basic Timestamp Ordering, baseline):
- Dispatch ngay nếu `tx.timestamp ≥ last_committed[step_id]`.
- Nếu vi phạm thứ tự → **abort + restart** với timestamp mới lớn hơn lần commit
  gần nhất trên step đó. `abort_rate` = số lần restart / tổng số tx.

**Định tuyến động:** dataset gồm `dataset_size` step được chia đều cho `num_nodes`
node; `machineID = số node` sở hữu step đó nên Scheduler định tuyến trực tiếp
`node{machineID}` — không phụ thuộc số node cố định.

---

## Cấu trúc thư mục

```
project-27-cto/
├── pyproject.toml
├── README.md
│
├── scheduler/
│   ├── main.py             # WebSocket server, điều phối + watchdog
│   ├── cto_scheduler.py    # CTO: wait_queue, try_dispatch
│   ├── bto_scheduler.py    # BTO: abort/restart
│   └── clock_manager.py    # Lamport clock + min_clock
│
├── node_agent/
│   └── agent.py            # WebSocket client, đọc/ghi JSON, delay tuỳ chọn
│
├── workload/
│   ├── generator.py        # sinh transaction (tham số hoá)
│   └── orchestrator.py     # sinh dataset + chạy cả stack (Streamlit gọi)
│
├── analysis/
│   └── dashboard.py        # Streamlit: điều khiển + visualization + log
│
├── data/
│   ├── generate_data.py    # (script dataset gốc, tuỳ chọn)
│   └── generated/          # dataset chia đều cho N node (tự sinh)
│
├── logs/                   # tự tạo khi chạy
│   ├── result.json         # kết quả phiên gần nhất
│   └── runtime/*.log       # log từng tiến trình con (hiện trên dashboard)
│
└── tests/
    └── test_scheduler.py   # 29 pytest test cases
```

---

## Yêu cầu & cài đặt

| Công cụ | Phiên bản |
|---------|-----------|
| Python  | ≥ 3.11    |
| [uv](https://docs.astral.sh/uv/) | latest |

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # cài uv
uv sync                                            # cài dependencies
```

---

## Cách chạy

```bash
uv run streamlit run analysis/dashboard.py
```

Mở `http://localhost:8501`. Tại thanh bên **⚙️ Điều khiển Benchmark**, chỉnh mọi
tham số rồi bấm **▶️ Chạy Benchmark**:

| Tham số | Ý nghĩa |
|---------|---------|
| **Chế độ** | `both` (CTO + BTO) / `cto` / `bto` |
| **Số node** | Số node trong hệ (1–8). Dataset chia đều cho từng node. |
| **Bật/tắt node** | Mỗi node là 1 process – tắt để xem hệ hoạt động với ít node hơn. |
| **Delay (ms)** | Độ trễ nhân tạo mỗi node (mô phỏng node chậm). |
| **Kích thước dataset** | Tổng số step, chia đều cho các node. |
| **Số transaction** | Số transaction Generator sinh ra mỗi lần chạy. |
| **Concurrency** | Số transaction gửi song song. |
| **Mức tranh chấp** | % transaction dồn vào nhóm "hot step" → BTO abort nhiều. |
| **Số hot step** | Kích thước vùng tranh chấp. |

Dashboard tự sinh dataset, khởi động Scheduler + các Node Agent đang bật +
Generator, chờ xong rồi nạp kết quả, vẽ biểu đồ và hiển thị **log từng tiến
trình con** ngay trên trang.

### Chạy không cần Streamlit (tuỳ chọn)

```bash
uv run python workload/orchestrator.py \
  --num-nodes 3 --active-nodes 1,2,3 --dataset-size 3000 \
  --num-transactions 1000 --contention 0.5 --hot-steps 10 --concurrency 50
```

---

## Dashboard hiển thị

- **Tham số phiên chạy**: số node, node bật, dataset, transactions, contention…
- **Metrics** CTO vs BTO: avg latency, abort rate, std latency.
- **Bar charts**: average latency & abort rate.
- **Histogram**: phân phối latency overlay CTO vs BTO.
- **Line chart**: latency timeline.
- **Kết luận tự động** + **log tiến trình con** (scheduler / node / generator).

---

## Chạy test

```bash
uv run pytest tests/ -v
```

29 test cases: `ClockManager` (10), `CTOScheduler` (9), `BTOScheduler` (7,
gồm abort + restart), Integration (3).

---

## Kết quả kỳ vọng

| Điều kiện | CTO Abort | BTO Abort | CTO Latency so BTO |
|-----------|:---------:|:---------:|:------------------:|
| Tranh chấp thấp | **0%** | ≈ 0% | cao hơn |
| Tranh chấp cao (contention lớn, ít hot step) | **0%** | cao | cao hơn rõ rệt |
| Có node delay | **0%** | ≥ 0% | cao hơn nhiều |

**Kết luận:** CTO đảm bảo Abort Rate = 0% trong mọi cấu hình, phù hợp môi trường
Automated Manufacturing không được mất transaction. Trade-off là latency cao hơn
BTO vì phải buffer transaction đến khi nguồn gửi đóng.

---

## Quyết định thiết kế

- **Storage = plain JSON files**: mỗi node 1 file, chỉ 1 agent truy cập → CTO
  Scheduler là thành phần *duy nhất* kiểm soát concurrency.
- **Giao tiếp = Python `websockets`** thuần (asyncio).
- **Timestamp = Lamport** (số nguyên); **latency = milliseconds** (`time.perf_counter()`).
- **Package manager = uv**; **Visualization = Streamlit**.
