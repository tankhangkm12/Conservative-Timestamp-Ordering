"""
CTO Scheduler – Conservative Timestamp Ordering.

Thuật toán: Özsu & Valduriez, "Principles of Distributed Database Systems"
  - Mỗi transaction được gán Lamport timestamp khi arrive tại Scheduler.
  - Transaction chỉ được dispatch sau khi nguồn gửi báo đã gửi xong,
    đảm bảo không còn transaction nào có timestamp nhỏ hơn đang trên đường.
  - Không bao giờ abort → Abort Rate = 0% (đặc trưng của CTO).
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
import uuid
from collections import defaultdict
from typing import Awaitable, Callable

# pyrefly: ignore [missing-import]
from clock_manager import ClockManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transaction dataclass
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Transaction:
    """Đơn vị công việc trong hệ thống CTO."""

    tx_id: str                   # uuid4
    timestamp: int               # Lamport clock lúc Scheduler nhận tx
    step_id: int
    machine_id: int
    operation: str               # "READ" | "WRITE"
    data: dict                   # {"status": "running"} v.v.
    submit_time: float           # time.perf_counter() – dùng tính latency

    @staticmethod
    def new(
        step_id: int,
        machine_id: int,
        operation: str,
        data: dict,
        timestamp: int,
    ) -> "Transaction":
        """Factory method tạo Transaction mới với tx_id tự sinh."""
        return Transaction(
            tx_id=str(uuid.uuid4()),
            timestamp=timestamp,
            step_id=step_id,
            machine_id=machine_id,
            operation=operation,
            data=data,
            submit_time=time.perf_counter(),
        )

    def to_dict(self) -> dict:
        """Serialize sang dict để gửi qua WebSocket."""
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

SendFn = Callable[[Transaction], Awaitable[None]]


# ---------------------------------------------------------------------------
# CTOScheduler
# ---------------------------------------------------------------------------


class CTOScheduler:
    """
    Conservative Timestamp Ordering Scheduler.

    Bất biến (invariant):
      Chỉ dispatch sau close_input(), khi nguồn gửi đã xác nhận không còn
      transaction "trễ" (late-arriving) nào đang trên đường.
    """

    def __init__(self, clock_manager: ClockManager) -> None:
        self.clock_manager = clock_manager

        # key = step_id, value = list transaction đang chờ, sắp xếp theo timestamp
        self.wait_queue: dict[int, list[Transaction]] = defaultdict(list)

        # tx_id → log entry (điền dần khi submit + commit)
        self.pending_results: dict[str, dict] = {}

        # bảo vệ wait_queue khi nhiều coroutine cùng gọi submit/try_dispatch
        self._lock = asyncio.Lock()
        self._input_closed = False

    # ------------------------------------------------------------------
    # submit
    # ------------------------------------------------------------------

    async def submit(self, tx: Transaction) -> None:
        """
        Thêm transaction vào wait_queue[step_id] và giữ thứ tự timestamp tăng dần.

        Ghi log entry sơ bộ để record_commit() có thể tra submit_time sau.
        """
        async with self._lock:
            self.wait_queue[tx.step_id].append(tx)
            self.wait_queue[tx.step_id].sort(key=lambda t: t.timestamp)

            self.pending_results[tx.tx_id] = {
                "tx_id": tx.tx_id,
                "timestamp": tx.timestamp,
                "step_id": tx.step_id,
                "machine_id": tx.machine_id,
                "operation": tx.operation,
                "submit_time": tx.submit_time,
                "status": "waiting",
                "latency_ms": None,
            }

        logger.info(
            "SUBMIT tx=%s ts=%d step=%d machine=%d op=%s | queue_size=%d",
            tx.tx_id[:8],
            tx.timestamp,
            tx.step_id,
            tx.machine_id,
            tx.operation,
            len(self.wait_queue[tx.step_id]),
        )

    # ------------------------------------------------------------------
    # try_dispatch
    # ------------------------------------------------------------------

    def close_input(self) -> None:
        """Cho phép dispatch sau khi nguồn gửi xác nhận không còn transaction mới."""
        self._input_closed = True

    async def try_dispatch(self, send_to_node_fn: SendFn) -> int:
        """
        Sau close_input(), dispatch transaction nhỏ nhất của mỗi hàng đợi.

        Trả về số transaction đã dispatch trong lần gọi này.
        """
        if not self._input_closed:
            return 0

        dispatched = 0

        async with self._lock:
            # Lấy snapshot các step_id hiện có để tránh dict-size-change
            step_ids = list(self.wait_queue.keys())

        for step_id in step_ids:
            async with self._lock:
                queue = self.wait_queue.get(step_id)
                if not queue:
                    continue
                tx = queue[0]  # tx có timestamp nhỏ nhất trong step này

            async with self._lock:
                queue = self.wait_queue.get(step_id, [])
                if queue and queue[0].tx_id == tx.tx_id:
                    queue.pop(0)
                    self.pending_results[tx.tx_id]["status"] = "dispatched"

            logger.info("DISPATCH tx=%s ts=%d step=%d", tx.tx_id[:8], tx.timestamp, tx.step_id)
            await send_to_node_fn(tx)
            dispatched += 1

        return dispatched

    # ------------------------------------------------------------------
    # record_commit
    # ------------------------------------------------------------------

    def record_commit(self, tx_id: str, commit_time: float) -> None:
        """
        Ghi nhận thời điểm commit và tính latency.

          latency_ms = (commit_time – submit_time) × 1000  [đơn vị ms]
        """
        entry = self.pending_results.get(tx_id)
        if entry is None:
            logger.warning("record_commit: tx_id=%s không tìm thấy", tx_id)
            return

        latency_ms = (commit_time - entry["submit_time"]) * 1000
        entry["status"] = "committed"
        entry["latency_ms"] = round(latency_ms, 4)

        logger.info(
            "COMMIT tx=%s latency=%.2f ms",
            tx_id[:8],
            latency_ms,
        )

    # ------------------------------------------------------------------
    # get_results
    # ------------------------------------------------------------------

    def get_results(self) -> list[dict]:
        """Trả về danh sách tất cả kết quả transaction đã ghi nhận."""
        return list(self.pending_results.values())

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def queue_depth(self) -> dict[int, int]:
        """Trả về số tx đang chờ theo từng step_id (dùng để monitor)."""
        return {sid: len(q) for sid, q in self.wait_queue.items() if q}

    def pending_count(self) -> int:
        """Tổng số transaction chưa commit."""
        return sum(
            1
            for e in self.pending_results.values()
            if e["status"] != "committed"
        )

    def __repr__(self) -> str:
        return (
            f"CTOScheduler("
            f"queued={sum(len(q) for q in self.wait_queue.values())}, "
            f"results={len(self.pending_results)})"
        )


# ---------------------------------------------------------------------------
# Chạy độc lập – smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async def _smoke_test() -> None:
        print("=== CTOScheduler smoke test ===")

        cm = ClockManager()
        cto = CTOScheduler(cm)

        dispatched_txs: list[Transaction] = []

        async def mock_send(tx: Transaction) -> None:
            dispatched_txs.append(tx)

        # ----------------------------------------------------------------
        # Test 1: submit và kiểm tra wait_queue
        # ----------------------------------------------------------------
        tx1 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "running"}, timestamp=cm.tick())
        tx2 = Transaction.new(step_id=2, machine_id=40, operation="WRITE",
                              data={"status": "done"}, timestamp=cm.tick())
        tx3 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "failed"}, timestamp=cm.tick())

        await cto.submit(tx1)
        await cto.submit(tx2)
        await cto.submit(tx3)

        # step_id=1 có 2 tx, step_id=2 có 1 tx
        assert len(cto.wait_queue[1]) == 2
        assert len(cto.wait_queue[2]) == 1
        # tx trong step_id=1 phải sắp xếp đúng thứ tự timestamp
        assert cto.wait_queue[1][0].tx_id == tx1.tx_id
        assert cto.wait_queue[1][1].tx_id == tx3.tx_id
        print("Test 1 (submit + sort): PASSED ✓")

        # ----------------------------------------------------------------
        # Test 2: nguồn gửi chưa đóng → KHÔNG dispatch (conservative wait)
        # ----------------------------------------------------------------
        n = await cto.try_dispatch(mock_send)
        assert n == 0, f"Mong đợi 0 dispatch, nhận {n}"
        assert len(dispatched_txs) == 0
        print("Test 2 (conservative wait): PASSED ✓")

        # ----------------------------------------------------------------
        # Test 3: nguồn gửi đóng → dispatch
        # ----------------------------------------------------------------
        cto.close_input()

        n = await cto.try_dispatch(mock_send)
        # Mỗi step_id chỉ dispatch 1 tx đầu hàng mỗi lần gọi,
        # nhưng vì step_id=1 có 2 tx ta gọi thêm 1 lần nữa
        n += await cto.try_dispatch(mock_send)

        assert len(dispatched_txs) == 3, f"Mong đợi 3, nhận {len(dispatched_txs)}"
        print(f"Test 3 (dispatch khi nguồn gửi đóng): PASSED ✓  (dispatched={len(dispatched_txs)})")

        # ----------------------------------------------------------------
        # Test 4: record_commit + latency
        # ----------------------------------------------------------------
        commit_time = time.perf_counter()
        cto.record_commit(tx1.tx_id, commit_time)
        entry = cto.pending_results[tx1.tx_id]
        assert entry["status"] == "committed"
        assert entry["latency_ms"] is not None
        assert entry["latency_ms"] >= 0
        print(f"Test 4 (record_commit): PASSED ✓  latency={entry['latency_ms']} ms")

        # ----------------------------------------------------------------
        # Test 5: get_results
        # ----------------------------------------------------------------
        results = cto.get_results()
        assert len(results) == 3
        print(f"Test 5 (get_results): PASSED ✓  count={len(results)}")

        print(cto)
        print("=== Tất cả test passed ===")

    asyncio.run(_smoke_test())
