"""
Scheduler WebSocket Server – điều phối toàn bộ hệ thống CTO/BTO.

Cổng: 8765

Luồng message:
  Node Agent → REGISTER   {"type": "register",  "node_id": "1"|"2"|"3"}
  Node Agent → ACK        {"type": "ack",        "tx_id", "node_id", "clock", "commit_time"}
  Generator  → SUBMIT     {"type": "submit",     "mode": "cto"|"bto"|"both",
                                                  "step_id", "machine_id",
                                                  "operation", "data", "scenario"?}
  Generator  → SESSION_END {"type": "session_end", "scenario": ...}

  Scheduler  → EXECUTE    {"type": "execute",    "transaction": {...}}
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from pathlib import Path

import websockets
from websockets.asyncio.server import ServerConnection, serve

# Thêm thư mục scheduler vào path để import local modules
sys.path.insert(0, str(Path(__file__).parent))

from clock_manager import ClockManager
from cto_scheduler import CTOScheduler, Transaction
from bto_scheduler import BTOScheduler

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HOST: str = os.environ.get("SCHEDULER_HOST", "0.0.0.0")
PORT: int = int(os.environ.get("SCHEDULER_PORT", "8765"))
LOGS_DIR: Path = Path(os.environ.get("LOGS_DIR", "/app/logs"))
DISPATCH_INTERVAL: float = 0.02   # 20ms – tần suất vòng lặp dispatch CTO
# Sau session_end, nếu không hoàn tất trong khoảng này → buộc kết thúc (chống treo).
BENCH_TIMEOUT: float = float(os.environ.get("BENCH_TIMEOUT", "120"))

logger = logging.getLogger("scheduler.main")


# ---------------------------------------------------------------------------
# Server state (singleton)
# ---------------------------------------------------------------------------


class SchedulerState:
    """Toàn bộ trạng thái chạy của Scheduler."""

    def __init__(self) -> None:
        self.clock_mgr = ClockManager()
        self.cto = CTOScheduler(self.clock_mgr)
        self.bto = BTOScheduler(self.clock_mgr)

        # node_id ("node1","node2","node3") → websocket connection
        self.connected_nodes: dict[str, ServerConnection] = {}

        # tx_id → submit_time (cho record_commit của BTO)
        self._submit_times: dict[str, float] = {}

        # Theo dõi tiến độ để biết khi nào tất cả tx xong
        self._submitted: int = 0
        self._completed: int = 0          # committed + aborted
        self._session_ended: bool = False
        self._done_event = asyncio.Event()

        # Tham số phiên chạy (do orchestrator truyền qua env RUN_PARAMS) – chỉ để
        # ghi vào result.json cho dashboard hiển thị lại.
        try:
            self._params: dict = json.loads(os.environ.get("RUN_PARAMS", "{}"))
        except json.JSONDecodeError:
            self._params = {}

    # ------------------------------------------------------------------
    # Node routing
    # ------------------------------------------------------------------

    @staticmethod
    def determine_node(machine_id: int) -> str:
        """
        machineID chính là số node (1..N).

        Phân vùng được chia đều theo step_id ở phía Generator, machineID của mỗi
        transaction = node sở hữu step đó → định tuyến trực tiếp, không phụ thuộc
        số node cố định.
        """
        return f"node{machine_id}"

    # ------------------------------------------------------------------
    # send_to_node
    # ------------------------------------------------------------------

    async def send_to_node(self, tx: Transaction) -> None:
        """Gửi transaction EXECUTE đến Node Agent tương ứng."""
        node_key = self.determine_node(tx.machine_id)
        ws = self.connected_nodes.get(node_key)
        if ws is None:
            logger.error(
                "send_to_node: %s chưa kết nối – bỏ qua tx=%s",
                node_key, tx.tx_id[:8],
            )
            return

        payload = json.dumps({"type": "execute", "transaction": tx.to_dict()})
        try:
            await ws.send(payload)
            logger.debug("→ EXECUTE tx=%s → %s", tx.tx_id[:8], node_key)
        except websockets.exceptions.WebSocketException as exc:
            logger.error("send_to_node %s thất bại: %s", node_key, exc)

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def handle_register(self, ws: ServerConnection, msg: dict) -> None:
        """Lưu connection của Node Agent vừa kết nối."""
        node_id = msg.get("node_id", "")
        node_key = f"node{node_id}"
        self.connected_nodes[node_key] = ws
        logger.info(
            "REGISTER %s (port=%s) – nodes online: %s",
            node_key, msg.get("port", "?"),
            list(self.connected_nodes.keys()),
        )

    async def handle_ack(self, msg: dict) -> None:
        """
        Nhận ACK từ Node Agent:
          1. Cập nhật Lamport clock của node.
          2. Gọi record_commit trên cả CTO và BTO nếu tx thuộc về.
          3. Trigger try_dispatch để giải phóng tx đang chờ trong CTO.
          4. Kiểm tra điều kiện hoàn tất.
        """
        node_id_raw: str = msg.get("node_id", "")
        node_key = f"node{node_id_raw}"
        clock_val: int = int(msg.get("clock", 0))
        tx_id: str = msg.get("tx_id", "")
        commit_time: float = float(msg.get("commit_time", time.perf_counter()))

        # 1. Cập nhật clock
        self.clock_mgr.update(node_key, clock_val)
        logger.debug(
            "ACK tx=%s %s clock=%d | min_clock=%d",
            tx_id[:8], node_key, clock_val, self.clock_mgr.min_clock(),
        )

        # 2. record_commit (CTO và BTO đều tra theo tx_id)
        self.cto.record_commit(tx_id, commit_time)

        # BTO: cần submit_time riêng vì results đã ghi latency khi execute()
        # Cập nhật lại latency chính xác nếu entry tồn tại
        submit_time = self._submit_times.pop(tx_id, None)
        if submit_time is not None:
            latency_ms = (commit_time - submit_time) * 1000
            for entry in self.bto.results:
                if entry["tx_id"] == tx_id and entry["status"] == "committed":
                    entry["latency_ms"] = round(latency_ms, 4)
                    break

        # 3. Dispatch CTO ngay sau khi clock được nâng
        await self.cto.try_dispatch(self.send_to_node)

        # 4. Đếm hoàn thành
        self._completed += 1
        self._check_done()

    async def handle_submit(self, msg: dict) -> None:
        """
        Nhận transaction từ Workload Generator:
          - Gán Lamport timestamp.
          - Submit vào CTO hoặc BTO (hoặc cả hai nếu mode="both").
        """
        mode: str = msg.get("mode", "cto").lower()

        # Timestamp được gán tại Generator (client) lúc tạo transaction.
        # Vì client gửi theo thứ tự xáo trộn (mô phỏng độ trễ mạng), thứ tự
        # đến scheduler ≠ thứ tự timestamp → BTO gặp tx "đến trễ" và abort.
        # Vẫn tick() đồng hồ scheduler để giữ Lamport advance; nếu client
        # không gửi timestamp thì fallback về tick().
        scheduler_tick = self.clock_mgr.tick()
        ts = int(msg.get("timestamp", scheduler_tick))

        def _make_tx() -> Transaction:
            return Transaction.new(
                step_id=int(msg["step_id"]),
                machine_id=int(msg["machine_id"]),
                operation=str(msg.get("operation", "WRITE")),
                data=dict(msg.get("data", {})),
                timestamp=ts,
            )

        if mode in ("cto", "both"):
            tx = _make_tx()
            self._submit_times[tx.tx_id] = tx.submit_time
            await self.cto.submit(tx)
            self._submitted += 1
            logger.debug("SUBMIT CTO tx=%s ts=%d step=%d", tx.tx_id[:8], ts, tx.step_id)

        if mode in ("bto", "both"):
            tx = _make_tx()
            self._submit_times[tx.tx_id] = tx.submit_time
            # BTO execute ngay lập tức (không có hàng đợi)
            await self.bto.execute(tx, self.send_to_node)
            self._submitted += 1

    async def handle_session_end(self, msg: dict) -> None:
        """Generator báo đã gửi xong tất cả transaction."""
        self._session_ended = True
        self.cto.close_input()
        logger.info(
            "SESSION_END | submitted=%d completed=%d",
            self._submitted, self._completed,
        )

        # Trigger dispatch loop thêm vài lần để giải phóng queue CTO còn lại
        for _ in range(10):
            await self.cto.try_dispatch(self.send_to_node)
            await asyncio.sleep(DISPATCH_INTERVAL)

        self._check_done()

        # Watchdog: nếu sau BENCH_TIMEOUT vẫn còn tx chưa hoàn tất (node mất ACK,
        # treo mạng...) thì buộc kết thúc để Scheduler vẫn lưu kết quả và thoát.
        asyncio.create_task(self._watchdog())

    async def _watchdog(self) -> None:
        await asyncio.sleep(BENCH_TIMEOUT)
        if not self._done_event.is_set():
            logger.warning(
                "Watchdog: quá %.0fs sau session_end – buộc hoàn tất "
                "(completed=%d/%d)",
                BENCH_TIMEOUT, self._completed, self._submitted,
            )
            self._done_event.set()

    # ------------------------------------------------------------------
    # Completion check
    # ------------------------------------------------------------------

    def _check_done(self) -> None:
        if (
            self._session_ended
            and self._submitted > 0
            and self._completed >= self._submitted
        ):
            if not self._done_event.is_set():
                logger.info(
                    "Tất cả %d transaction hoàn tất – lưu kết quả",
                    self._submitted,
                )
                self._done_event.set()

    # ------------------------------------------------------------------
    # Save results
    # ------------------------------------------------------------------

    def save_results(self) -> None:
        """Ghi logs/result.json sau khi benchmark hoàn tất."""
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = LOGS_DIR / "result.json"

        def _stats(results: list[dict], abort_rate: float) -> dict:
            committed = [r for r in results if r.get("status") == "committed"]
            latencies = [
                r["latency_ms"] for r in committed
                if r.get("latency_ms") is not None
            ]
            n = len(latencies)
            avg = sum(latencies) / n if n else 0.0
            variance = sum((x - avg) ** 2 for x in latencies) / n if n else 0.0
            std = math.sqrt(variance)
            return {
                "transactions": results,
                "avg_latency_ms": round(avg, 4),
                "abort_rate": round(abort_rate, 4),
                "std_latency_ms": round(std, 4),
            }

        output = {
            "cto": _stats(self.cto.get_results(), 0.0),
            "bto": _stats(self.bto.get_results(), self.bto.get_abort_rate()),
            "params": self._params,
            "summary": {
                "total_submitted": self._submitted,
                "total_completed": self._completed,
                "cto_committed": sum(
                    1 for r in self.cto.get_results() if r.get("status") == "committed"
                ),
                "bto_committed": sum(
                    1 for r in self.bto.get_results() if r.get("status") == "committed"
                ),
                "bto_aborted": self.bto.abort_count,
                "bto_restarted": self.bto.restart_count,
            },
        }

        tmp = out_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        tmp.replace(out_path)

        logger.info(
            "Đã lưu kết quả → %s  "
            "(CTO avg=%.2fms | BTO avg=%.2fms abort_rate=%.1f%%)",
            out_path,
            output["cto"]["avg_latency_ms"],
            output["bto"]["avg_latency_ms"],
            output["bto"]["abort_rate"],
        )


# ---------------------------------------------------------------------------
# Connection handler
# ---------------------------------------------------------------------------

async def connection_handler(ws: ServerConnection, state: SchedulerState) -> None:
    """Xử lý một WebSocket connection (node agent hoặc workload generator)."""
    peer = ws.remote_address
    logger.info("Kết nối mới từ %s", peer)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Message không hợp lệ từ %s: %r", peer, raw[:80])
                continue

            msg_type = msg.get("type", "")

            if msg_type == "register":
                await state.handle_register(ws, msg)

            elif msg_type == "ack":
                await state.handle_ack(msg)

            elif msg_type == "submit":
                await state.handle_submit(msg)

            elif msg_type == "session_end":
                await state.handle_session_end(msg)

            elif msg_type == "pong":
                pass  # keepalive

            else:
                logger.warning("Type không xử lý: %s từ %s", msg_type, peer)

    except websockets.exceptions.ConnectionClosedOK:
        logger.info("Kết nối đóng bình thường: %s", peer)
    except websockets.exceptions.ConnectionClosedError as exc:
        logger.warning("Kết nối đóng lỗi: %s – %s", peer, exc)
    finally:
        # Xoá node khỏi connected_nodes nếu là node agent
        for key, val in list(state.connected_nodes.items()):
            if val is ws:
                del state.connected_nodes[key]
                logger.info("Node %s ngắt kết nối", key)
                break


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def dispatch_loop(state: SchedulerState) -> None:
    """Gọi try_dispatch định kỳ sau khi nguồn gửi đã đóng."""
    while True:
        await asyncio.sleep(DISPATCH_INTERVAL)

        n = await state.cto.try_dispatch(state.send_to_node)
        if n:
            logger.debug("dispatch_loop: dispatched %d tx", n)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    state = SchedulerState()

    # Khởi động vòng lặp dispatch CTO trong nền
    dispatch_task = asyncio.create_task(dispatch_loop(state))

    handler = lambda ws: connection_handler(ws, state)

    logger.info("Scheduler khởi động tại ws://%s:%d", HOST, PORT)

    async with serve(handler, HOST, PORT) as server:
        logger.info("Đang lắng nghe – chờ Node Agent và Workload Generator kết nối...")

        # Chờ cho đến khi tất cả tx hoàn tất
        try:
            await asyncio.wait_for(state._done_event.wait(), timeout=None)
        except asyncio.CancelledError:
            pass
        finally:
            dispatch_task.cancel()

        # Thêm thời gian nhỏ để các ACK cuối cùng được xử lý
        await asyncio.sleep(0.5)
        state.save_results()

        logger.info("Benchmark xong. Scheduler tắt sau 2 giây...")
        await asyncio.sleep(2)
        server.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(main())
