
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


@dataclasses.dataclass
class Transaction:

    tx_id: str
    timestamp: int
    step_id: int
    machine_id: int
    operation: str
    data: dict
    submit_time: float

    @staticmethod
    def new(
        step_id: int,
        machine_id: int,
        operation: str,
        data: dict,
        timestamp: int,
    ) -> "Transaction":
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
        return dataclasses.asdict(self)


SendFn = Callable[[Transaction], Awaitable[None]]


class CTOScheduler:

    def __init__(self, clock_manager: ClockManager) -> None:
        self.clock_manager = clock_manager

        self.wait_queue: dict[int, list[Transaction]] = defaultdict(list)

        self.pending_results: dict[str, dict] = {}

        self._lock = asyncio.Lock()
        self._input_closed = False


    async def submit(self, tx: Transaction) -> None:
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


    def close_input(self) -> None:
        self._input_closed = True

    async def try_dispatch(self, send_to_node_fn: SendFn) -> int:
        # CTO chỉ dispatch khi biết nguồn gửi không còn transaction cũ đến trễ.
        if not self._input_closed:
            return 0

        dispatched = 0

        async with self._lock:
            step_ids = list(self.wait_queue.keys())

        for step_id in step_ids:
            async with self._lock:
                queue = self.wait_queue.get(step_id)
                if not queue:
                    continue
                tx = queue[0]

            async with self._lock:
                queue = self.wait_queue.get(step_id, [])
                if queue and queue[0].tx_id == tx.tx_id:
                    queue.pop(0)
                    self.pending_results[tx.tx_id]["status"] = "dispatched"

            logger.info("DISPATCH tx=%s ts=%d step=%d", tx.tx_id[:8], tx.timestamp, tx.step_id)
            await send_to_node_fn(tx)
            dispatched += 1

        return dispatched


    def record_commit(self, tx_id: str, commit_time: float) -> None:
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


    def get_results(self) -> list[dict]:
        return list(self.pending_results.values())


    def queue_depth(self) -> dict[int, int]:
        return {sid: len(q) for sid, q in self.wait_queue.items() if q}

    def pending_count(self) -> int:
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

        tx1 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "running"}, timestamp=cm.tick())
        tx2 = Transaction.new(step_id=2, machine_id=40, operation="WRITE",
                              data={"status": "done"}, timestamp=cm.tick())
        tx3 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "failed"}, timestamp=cm.tick())

        await cto.submit(tx1)
        await cto.submit(tx2)
        await cto.submit(tx3)

        assert len(cto.wait_queue[1]) == 2
        assert len(cto.wait_queue[2]) == 1
        assert cto.wait_queue[1][0].tx_id == tx1.tx_id
        assert cto.wait_queue[1][1].tx_id == tx3.tx_id
        print("Test 1 (submit + sort): PASSED ✓")

        n = await cto.try_dispatch(mock_send)
        assert n == 0, f"Mong đợi 0 dispatch, nhận {n}"
        assert len(dispatched_txs) == 0
        print("Test 2 (conservative wait): PASSED ✓")

        cto.close_input()

        n = await cto.try_dispatch(mock_send)
        n += await cto.try_dispatch(mock_send)

        assert len(dispatched_txs) == 3, f"Mong đợi 3, nhận {len(dispatched_txs)}"
        print(f"Test 3 (dispatch khi nguồn gửi đóng): PASSED ✓  (dispatched={len(dispatched_txs)})")

        commit_time = time.perf_counter()
        cto.record_commit(tx1.tx_id, commit_time)
        entry = cto.pending_results[tx1.tx_id]
        assert entry["status"] == "committed"
        assert entry["latency_ms"] is not None
        assert entry["latency_ms"] >= 0
        print(f"Test 4 (record_commit): PASSED ✓  latency={entry['latency_ms']} ms")

        results = cto.get_results()
        assert len(results) == 3
        print(f"Test 5 (get_results): PASSED ✓  count={len(results)}")

        print(cto)
        print("=== Tất cả test passed ===")

    asyncio.run(_smoke_test())
