"""
Workload Generator – sinh transaction đồng thời gửi đến Scheduler.

Tất cả tham số được truyền vào (thường từ Streamlit qua orchestrator), không còn
khái niệm "scenario" cố định. Phân vùng dữ liệu được tính động theo số node:

    block_size       = ceil(dataset_size / num_nodes)
    node(step_id)    = (step_id - 1) // block_size + 1     (kẹp trong 1..num_nodes)
    machine_id       = node(step_id)                       (machineID == số node)

Transaction chỉ nhắm vào các node đang BẬT (active). Mức tranh chấp (contention)
điều khiển tỉ lệ transaction dồn vào một nhóm nhỏ "hot steps" – nguyên nhân khiến
BTO phải abort/restart, trong khi CTO vẫn giữ abort = 0.

Chạy độc lập (chủ yếu để debug):
  uv run python workload/generator.py --num-nodes 3 --dataset-size 3000 \
      --num-transactions 1000 --contention 0.5 --hot-steps 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
import sys
import time

# pyrefly: ignore [missing-import]
import websockets

logger = logging.getLogger("workload.generator")

STATUSES = ["running", "done", "failed"]


# ---------------------------------------------------------------------------
# Phân vùng động
# ---------------------------------------------------------------------------


def block_size_of(dataset_size: int, num_nodes: int) -> int:
    """Số step mỗi node nắm giữ (chia đều, node cuối có thể ít hơn)."""
    return max(1, math.ceil(dataset_size / max(1, num_nodes)))


def node_of_step(step_id: int, dataset_size: int, num_nodes: int) -> int:
    """Trả về node id (1..num_nodes) sở hữu step_id."""
    bs = block_size_of(dataset_size, num_nodes)
    node = (step_id - 1) // bs + 1
    return min(node, num_nodes)


def active_steps(dataset_size: int, num_nodes: int, active: list[int]) -> list[int]:
    """Danh sách step_id thuộc các node đang bật."""
    active_set = set(active)
    return [
        s for s in range(1, dataset_size + 1)
        if node_of_step(s, dataset_size, num_nodes) in active_set
    ]


# ---------------------------------------------------------------------------
# Sinh transaction
# ---------------------------------------------------------------------------


def build_transactions(
    *,
    dataset_size: int,
    num_nodes: int,
    active: list[int],
    num_transactions: int,
    contention: float,
    hot_steps: int,
    mode: str,
) -> list[dict]:
    """
    Tạo danh sách message ``submit``.

    contention ∈ [0,1]: xác suất một transaction nhắm vào "hot set".
    hot_steps: số step trong hot set (lấy từ đầu danh sách active steps).
    """
    steps = active_steps(dataset_size, num_nodes, active)
    if not steps:
        logger.error("Không có step nào thuộc node đang bật – kiểm tra cấu hình.")
        return []

    hot = steps[: max(1, min(hot_steps, len(steps)))]

    txs: list[dict] = []
    for i in range(num_transactions):
        if random.random() < contention:
            step_id = random.choice(hot)
        else:
            step_id = random.choice(steps)
        machine_id = node_of_step(step_id, dataset_size, num_nodes)

        txs.append({
            "type": "submit",
            "mode": mode,
            # Timestamp gán tại CLIENT theo thứ tự tạo (logic clock tăng đều).
            # Gửi theo thứ tự xáo trộn → BTO thấy tx "đến trễ" và abort, còn CTO
            # sắp lại trong wait_queue nên abort = 0.
            "timestamp": i + 1,
            "step_id": step_id,
            "machine_id": machine_id,
            "operation": "WRITE",
            "data": {"status": random.choice(STATUSES)},
        })
    return txs


# ---------------------------------------------------------------------------
# Gửi
# ---------------------------------------------------------------------------


async def _send(ws, tx: dict, sem: asyncio.Semaphore) -> None:
    async with sem:
        await ws.send(json.dumps(tx))


async def run_generator(args: argparse.Namespace) -> None:
    uri = f"ws://{args.host}:{args.port}"
    sem = asyncio.Semaphore(args.concurrency)

    active = [int(x) for x in str(args.active_nodes).split(",") if x.strip()]
    if not active:
        active = list(range(1, args.num_nodes + 1))

    txs = build_transactions(
        dataset_size=args.dataset_size,
        num_nodes=args.num_nodes,
        active=active,
        num_transactions=args.num_transactions,
        contention=args.contention,
        hot_steps=args.hot_steps,
        mode=args.mode,
    )

    logger.info(
        "Kết nối %s | nodes=%d active=%s dataset=%d tx=%d contention=%.2f hot=%d mode=%s",
        uri, args.num_nodes, active, args.dataset_size, len(txs),
        args.contention, args.hot_steps, args.mode,
    )
    if not txs:
        return

    t_start = time.perf_counter()
    async with websockets.connect(uri) as ws:
        logger.info("Đã kết nối – gửi %d transaction...", len(txs))

        send_order = list(txs)
        random.shuffle(send_order)   # mô phỏng độ trễ mạng → tạo late-arrival
        await asyncio.gather(*(_send(ws, tx, sem) for tx in send_order))

        elapsed = time.perf_counter() - t_start
        logger.info(
            "Đã gửi %d tx trong %.2fs (%.0f tx/s)",
            len(txs), elapsed, len(txs) / elapsed if elapsed else 0,
        )

        await ws.send(json.dumps({
            "type": "session_end",
            "total_sent": len(txs),
        }))
        logger.info("SESSION_END gửi xong – chờ Scheduler lưu kết quả...")

        try:
            async with asyncio.timeout(120):
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("type") in ("done", "error"):
                        break
        except (TimeoutError, websockets.exceptions.ConnectionClosedOK):
            pass

    logger.info("Generator hoàn tất sau %.2fs.", time.perf_counter() - t_start)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CTO/BTO Workload Generator (tham số hoá hoàn toàn)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["cto", "bto", "both"], default="both")
    p.add_argument("--num-nodes", type=int, default=3)
    p.add_argument("--active-nodes", default="", help="VD: '1,2,3' (mặc định: tất cả)")
    p.add_argument("--dataset-size", type=int, default=3000, help="Tổng số step")
    p.add_argument("--num-transactions", type=int, default=1000)
    p.add_argument("--contention", type=float, default=0.5, help="0.0–1.0")
    p.add_argument("--hot-steps", type=int, default=10, help="Số step trong hot set")
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    a = parse_args()
    if a.seed is not None:
        random.seed(a.seed)
        logger.info("Random seed = %d", a.seed)
    try:
        asyncio.run(run_generator(a))
    except KeyboardInterrupt:
        logger.info("Dừng bởi người dùng.")
    except OSError as exc:
        logger.error("Không thể kết nối Scheduler: %s", exc)
        sys.exit(1)
