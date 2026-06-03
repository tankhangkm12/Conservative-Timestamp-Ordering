
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

# pyrefly: ignore [missing-import]
from clock_manager import ClockManager
# pyrefly: ignore [missing-import]
from cto_scheduler import Transaction

logger = logging.getLogger(__name__)

SendFn = Callable[[Transaction], Awaitable[None]]


class BTOScheduler:

    def __init__(self, clock_manager: ClockManager) -> None:
        self.clock_manager = clock_manager

        self.committed: dict[int, int] = {}

        self.results: list[dict] = []
        self.abort_count: int = 0
        self.restart_count: int = 0
        self.total_count: int = 0

        self._lock = asyncio.Lock()


    async def execute(self, tx: Transaction, send_to_node_fn: SendFn) -> bool:
        self.total_count += 1
        while True:
            async with self._lock:
                last_committed_ts = self.committed.get(tx.step_id, -1)
                if tx.timestamp >= last_committed_ts:
                    self.committed[tx.step_id] = tx.timestamp
                    break

                # BTO abort khi timestamp nhỏ hơn commit mới nhất của cùng step.
                old_ts = tx.timestamp
                self.abort_count += 1
                self.restart_count += 1
                self.results.append({
                    "tx_id": tx.tx_id,
                    "timestamp": old_ts,
                    "step_id": tx.step_id,
                    "machine_id": tx.machine_id,
                    "operation": tx.operation,
                    "status": "aborted",
                    "latency_ms": round((time.perf_counter() - tx.submit_time) * 1000, 4),
                    "abort_reason": f"ts={old_ts} < last_committed={last_committed_ts}",
                })
                tx.timestamp = max(self.clock_manager.tick(), max(self.committed.values()) + 1)
                logger.warning("ABORT tx=%s ts=%d → RESTART ts=%d", tx.tx_id[:8], old_ts, tx.timestamp)

        await send_to_node_fn(tx)

        async with self._lock:
            latency_ms = (time.perf_counter() - tx.submit_time) * 1000
            self.results.append({
                "tx_id": tx.tx_id,
                "timestamp": tx.timestamp,
                "step_id": tx.step_id,
                "machine_id": tx.machine_id,
                "operation": tx.operation,
                "status": "committed",
                "latency_ms": round(latency_ms, 4),
                "abort_reason": None,
            })

        logger.info(
            "COMMIT tx=%s ts=%d step=%d  latency=%.2f ms",
            tx.tx_id[:8],
            tx.timestamp,
            tx.step_id,
            latency_ms,
        )
        return True


    def record_commit(self, tx_id: str, commit_time: float) -> None:
        for entry in self.results:
            if entry["tx_id"] == tx_id and entry["status"] == "committed":
                logger.debug("record_commit BTO tx=%s (latency sẽ được ghi đè nếu cần)", tx_id[:8])
                break


    def get_abort_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return round(self.abort_count / self.total_count * 100, 4)


    def get_results(self) -> list[dict]:
        return list(self.results)


    def stats(self) -> dict:
        committed = [r for r in self.results if r["status"] == "committed"]
        latencies = [r["latency_ms"] for r in committed if r["latency_ms"] is not None]
        avg = round(sum(latencies) / len(latencies), 4) if latencies else 0.0
        return {
            "total": self.total_count,
            "committed": len(committed),
            "aborted": self.abort_count,
            "restarted": self.restart_count,
            "abort_rate_pct": self.get_abort_rate(),
            "avg_latency_ms": avg,
        }

    def __repr__(self) -> str:
        return (
            f"BTOScheduler("
            f"total={self.total_count}, "
            f"aborted={self.abort_count}, "
            f"abort_rate={self.get_abort_rate():.1f}%)"
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async def _smoke_test() -> None:
        print("=== BTOScheduler smoke test ===")

        cm = ClockManager()
        bto = BTOScheduler(cm)

        sent: list[Transaction] = []

        async def mock_send(tx: Transaction) -> None:
            sent.append(tx)

        tx1 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "running"}, timestamp=3)
        ok = await bto.execute(tx1, mock_send)
        assert ok is True
        assert bto.committed[1] == 3
        assert len(sent) == 1
        print("Test 1 (commit bình thường): PASSED ✓")

        tx2 = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                              data={"status": "done"}, timestamp=2)
        ok = await bto.execute(tx2, mock_send)
        assert ok is True
        assert bto.abort_count == 1
        assert bto.restart_count == 1
        assert len(sent) == 2
        print("Test 2 (abort + restart vì ts cũ): PASSED ✓")

        tx3 = Transaction.new(step_id=2, machine_id=40, operation="WRITE",
                              data={"status": "done"}, timestamp=5)
        ok = await bto.execute(tx3, mock_send)
        assert ok is True
        assert bto.committed[2] == 5
        print("Test 3 (step khác, commit bình thường): PASSED ✓")

        rate = bto.get_abort_rate()
        assert abs(rate - 33.3333) < 0.01, f"Mong đợi ≈33.33, nhận {rate}"
        print(f"Test 4 (get_abort_rate): PASSED ✓  rate={rate}%")

        cm2 = ClockManager()
        bto2 = BTOScheduler(cm2)

        async def noop(_tx: Transaction) -> None:
            pass

        for i in range(7):
            t = Transaction.new(99, 50, "WRITE", {}, timestamp=i * 10)
            await bto2.execute(t, noop)

        for i in range(3):
            t = Transaction.new(99, 50, "WRITE", {}, timestamp=i)
            await bto2.execute(t, noop)

        assert bto2.abort_count == 3
        assert bto2.total_count == 10
        assert abs(bto2.get_abort_rate() - 30.0) < 0.01
        print(f"Test 5 (abort_rate 30%): PASSED ✓  {bto2.stats()}")

        results = bto.get_results()
        assert len(results) == 4
        statuses = {r["status"] for r in results}
        assert "committed" in statuses and "aborted" in statuses
        print(f"Test 6 (get_results): PASSED ✓  statuses={statuses}")

        print(bto)
        print("=== Tất cả test passed ===")

    asyncio.run(_smoke_test())
