
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

_SCHEDULER_DIR = Path(__file__).parent.parent / "scheduler"
if str(_SCHEDULER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCHEDULER_DIR))

# pyrefly: ignore [missing-import]
from clock_manager import ClockManager
# pyrefly: ignore [missing-import]
from cto_scheduler import CTOScheduler, Transaction
# pyrefly: ignore [missing-import]
from bto_scheduler import BTOScheduler


class TestClockManager:

    def test_lamport_tick(self):
        clock = ClockManager()
        assert clock.tick() == 1
        assert clock.tick() == 2

    def test_lamport_tick_monotone(self):
        clock = ClockManager()
        prev = 0
        for _ in range(20):
            val = clock.tick()
            assert val == prev + 1
            prev = val

    def test_clock_update(self):
        clock = ClockManager()
        clock.update("node1", 5)
        assert clock.node_clocks["node1"] == 6

    def test_clock_update_idempotent_lower(self):
        clock = ClockManager()
        clock.update("node1", 10)
        clock.update("node1", 3)
        assert clock.node_clocks["node1"] == 12

    def test_clock_update_larger(self):
        clock = ClockManager()
        clock.update("node2", 100)
        assert clock.node_clocks["node2"] == 101

    def test_min_clock(self):
        clock = ClockManager()
        clock.update("node1", 3)
        clock.update("node2", 7)
        clock.update("node3", 1)
        assert clock.min_clock() == min(4, 8, 2)

    def test_min_clock_initial(self):
        clock = ClockManager()
        assert clock.min_clock() == 0

    def test_min_clock_all_equal(self):
        clock = ClockManager()
        for node in ("node1", "node2", "node3"):
            clock.update(node, 9)
        assert clock.min_clock() == 10

    def test_get_all_returns_copy(self):
        clock = ClockManager()
        clock.update("node1", 5)
        snapshot = clock.get_all()
        snapshot["node1"] = 9999
        assert clock.node_clocks["node1"] != 9999

    def test_update_unknown_node_ignored(self):
        clock = ClockManager()
        clock.update("nodeX", 100)
        assert "nodeX" not in clock.node_clocks


class TestCTOScheduler:

    def test_submit_adds_to_queue(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)
        tx = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                             data={"status": "running"}, timestamp=cm.tick())
        asyncio.run(cto.submit(tx))
        assert len(cto.wait_queue[1]) == 1

    def test_submit_sorts_by_timestamp(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)
        tx3 = Transaction.new(1, 5, "WRITE", {}, timestamp=3)
        tx1 = Transaction.new(1, 5, "WRITE", {}, timestamp=1)
        tx2 = Transaction.new(1, 5, "WRITE", {}, timestamp=2)

        async def _submit_all():
            await cto.submit(tx3)
            await cto.submit(tx1)
            await cto.submit(tx2)

        asyncio.run(_submit_all())
        timestamps = [t.timestamp for t in cto.wait_queue[1]]
        assert timestamps == sorted(timestamps)

    def test_cto_wait_condition_no_dispatch(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        dispatched: list[Transaction] = []

        async def _test():
            tx = Transaction.new(step_id=1, machine_id=5,
                                 operation="WRITE", data={}, timestamp=5)
            await cto.submit(tx)

            n = await cto.try_dispatch(lambda t: dispatched.append(t) or asyncio.sleep(0))
            assert n == 0
            assert len(dispatched) == 0

        asyncio.run(_test())

    def test_cto_wait_condition_dispatch(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        dispatched: list[Transaction] = []

        async def _test():
            tx = Transaction.new(step_id=1, machine_id=5,
                                 operation="WRITE", data={}, timestamp=5)
            await cto.submit(tx)

            cto.close_input()

            async def mock_send(t: Transaction):
                dispatched.append(t)

            n = await cto.try_dispatch(mock_send)
            assert n == 1
            assert len(dispatched) == 1
            assert dispatched[0].tx_id == tx.tx_id

        asyncio.run(_test())

    def test_cto_dispatch_clears_queue(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        async def _test():
            tx = Transaction.new(1, 5, "WRITE", {}, timestamp=1)
            await cto.submit(tx)

            cto.close_input()

            await cto.try_dispatch(lambda t: asyncio.sleep(0))
            assert len(cto.wait_queue.get(1, [])) == 0

        asyncio.run(_test())

    def test_cto_abort_rate_zero(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        async def _test():
            for i in range(5):
                tx = Transaction.new(i, i + 1, "WRITE", {}, timestamp=cm.tick())
                await cto.submit(tx)

            cto.close_input()

            await cto.try_dispatch(lambda t: asyncio.sleep(0))

            results = cto.get_results()
            aborted = [r for r in results if r.get("status") == "aborted"]
            assert len(aborted) == 0

        asyncio.run(_test())

    def test_cto_record_commit(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        async def _test():
            tx = Transaction.new(1, 5, "WRITE", {}, timestamp=cm.tick())
            await cto.submit(tx)

            commit_time = tx.submit_time + 0.050
            cto.record_commit(tx.tx_id, commit_time)

            entry = cto.pending_results[tx.tx_id]
            assert entry["status"] == "committed"
            assert abs(entry["latency_ms"] - 50.0) < 1.0

        asyncio.run(_test())

    def test_cto_get_results(self):
        cm = ClockManager()
        cto = CTOScheduler(cm)

        async def _test():
            for i in range(3):
                tx = Transaction.new(i, i + 1, "WRITE", {}, timestamp=cm.tick())
                await cto.submit(tx)

            results = cto.get_results()
            assert len(results) == 3

        asyncio.run(_test())

    def test_cto_waits_for_late_arrival(self):
        cto = CTOScheduler(ClockManager())

        async def _test():
            sent: list[int] = []

            async def send(tx): sent.append(tx.timestamp)

            await cto.submit(Transaction.new(1, 5, "WRITE", {}, timestamp=10))
            assert await cto.try_dispatch(send) == 0
            await cto.submit(Transaction.new(1, 5, "WRITE", {}, timestamp=5))
            cto.close_input()
            await cto.try_dispatch(send)
            await cto.try_dispatch(send)
            assert sent == [5, 10]

        asyncio.run(_test())


class TestBTOScheduler:

    def test_bto_abort(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            sent: list[Transaction] = []

            async def mock_send(t: Transaction):
                sent.append(t)

            tx1 = Transaction.new(1, 5, "WRITE", {"status": "running"}, timestamp=5)
            ok1 = await bto.execute(tx1, mock_send)
            assert ok1 is True
            assert bto.committed[1] == 5

            tx2 = Transaction.new(1, 5, "WRITE", {"status": "done"}, timestamp=3)
            ok2 = await bto.execute(tx2, mock_send)
            assert ok2 is True
            assert bto.abort_count == 1
            assert bto.restart_count == 1
            assert len(sent) == 2
            assert sent[-1].timestamp > 5

        asyncio.run(_test())

    def test_bto_no_abort_higher_timestamp(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            async def noop(t): pass
            tx1 = Transaction.new(1, 5, "WRITE", {}, timestamp=3)
            tx2 = Transaction.new(1, 5, "WRITE", {}, timestamp=7)
            await bto.execute(tx1, noop)
            ok = await bto.execute(tx2, noop)
            assert ok is True
            assert bto.abort_count == 0

        asyncio.run(_test())

    def test_bto_abort_rate(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            async def noop(t): pass

            for i in range(7):
                tx = Transaction.new(99, 50, "WRITE", {}, timestamp=i * 10)
                await bto.execute(tx, noop)

            for i in range(3):
                tx = Transaction.new(99, 50, "WRITE", {}, timestamp=i)
                await bto.execute(tx, noop)

            assert bto.abort_count == 3
            assert bto.total_count == 10
            assert abs(bto.get_abort_rate() - 30.0) < 0.01

        asyncio.run(_test())

    def test_bto_abort_rate_zero(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            async def noop(t): pass
            for i in range(5):
                tx = Transaction.new(i, i + 1, "WRITE", {}, timestamp=i * 10)
                await bto.execute(tx, noop)
            assert bto.get_abort_rate() == 0.0

        asyncio.run(_test())

    def test_bto_abort_rate_empty(self):
        bto = BTOScheduler(ClockManager())
        assert bto.get_abort_rate() == 0.0

    def test_bto_different_step_ids_independent(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            sent: list[Transaction] = []

            async def mock_send(t): sent.append(t)

            tx1 = Transaction.new(1, 5, "WRITE", {}, timestamp=10)
            await bto.execute(tx1, mock_send)

            tx2 = Transaction.new(2, 40, "WRITE", {}, timestamp=2)
            ok = await bto.execute(tx2, mock_send)
            assert ok is True
            assert len(sent) == 2

        asyncio.run(_test())

    def test_bto_get_results_contains_aborted(self):
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            async def noop(t): pass
            tx_ok = Transaction.new(1, 5, "WRITE", {}, timestamp=10)
            tx_ab = Transaction.new(1, 5, "WRITE", {}, timestamp=3)
            await bto.execute(tx_ok, noop)
            await bto.execute(tx_ab, noop)

            results = bto.get_results()
            statuses = {r["status"] for r in results}
            assert "committed" in statuses
            assert "aborted" in statuses

        asyncio.run(_test())


class TestCTOvsBTO:

    def test_cto_zero_abort_vs_bto_nonzero(self):
        cm_cto = ClockManager()
        cm_bto = ClockManager()
        cto = CTOScheduler(cm_cto)
        bto = BTOScheduler(cm_bto)

        async def _test():
            cto_dispatched: list[Transaction] = []
            bto_dispatched: list[Transaction] = []

            async def send_cto(t): cto_dispatched.append(t)
            async def send_bto(t): bto_dispatched.append(t)

            timestamps = [3, 1, 5, 2, 4]
            for ts in timestamps:
                cto_tx = Transaction.new(1, 5, "WRITE", {}, timestamp=ts)
                bto_tx = Transaction.new(1, 5, "WRITE", {}, timestamp=ts)
                await cto.submit(cto_tx)
                await bto.execute(bto_tx, send_bto)

            cto.close_input()

            for _ in range(5):
                await cto.try_dispatch(send_cto)

            cto_results = cto.get_results()
            assert all(r.get("status") != "aborted" for r in cto_results)

            assert bto.abort_count > 0
            assert bto.get_abort_rate() > 0.0

        asyncio.run(_test())

    def test_transaction_serializable(self):
        tx = Transaction.new(step_id=7, machine_id=42,
                             operation="WRITE", data={"status": "done"},
                             timestamp=99)
        d = tx.to_dict()
        for key in ("tx_id", "timestamp", "step_id", "machine_id",
                    "operation", "data", "submit_time"):
            assert key in d, f"Thiếu key: {key}"
        assert d["step_id"] == 7
        assert d["machine_id"] == 42
        assert d["timestamp"] == 99

    def test_transaction_unique_ids(self):
        ids = {
            Transaction.new(1, 1, "WRITE", {}, timestamp=i).tx_id
            for i in range(100)
        }
        assert len(ids) == 100
