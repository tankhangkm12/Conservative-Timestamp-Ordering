"""
Test suite – CTO Scheduler: Automated Manufacturing (Đề tài #27).

Chạy:
  uv run pytest tests/ -v
  uv run pytest tests/ -v --tb=short

Không cần pytest-asyncio: các test async dùng asyncio.run() trực tiếp.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup – chạy được từ project root: pytest tests/ -v
# ---------------------------------------------------------------------------
# Thêm scheduler/ vào path để các import dạng 'from clock_manager import ...'
# (dùng trong bto_scheduler, main) hoạt động song song với
# 'from scheduler.clock_manager import ...' (cto_scheduler sau khi linter đổi).
_SCHEDULER_DIR = Path(__file__).parent.parent / "scheduler"
if str(_SCHEDULER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCHEDULER_DIR))

# pyrefly: ignore [missing-import]
from clock_manager import ClockManager
# pyrefly: ignore [missing-import]
from cto_scheduler import CTOScheduler, Transaction
# pyrefly: ignore [missing-import]
from bto_scheduler import BTOScheduler


# ===========================================================================
# ClockManager tests
# ===========================================================================


class TestClockManager:

    def test_lamport_tick(self):
        """tick() tăng dần từ 0, trả về giá trị mới."""
        clock = ClockManager()
        assert clock.tick() == 1
        assert clock.tick() == 2

    def test_lamport_tick_monotone(self):
        """Nhiều lần tick liên tiếp luôn tăng dần."""
        clock = ClockManager()
        prev = 0
        for _ in range(20):
            val = clock.tick()
            assert val == prev + 1
            prev = val

    def test_clock_update(self):
        """update(node, value) → node_clocks[node] = max(current, value) + 1."""
        clock = ClockManager()
        clock.update("node1", 5)
        assert clock.node_clocks["node1"] == 6

    def test_clock_update_idempotent_lower(self):
        """Cập nhật giá trị nhỏ hơn không làm giảm clock."""
        clock = ClockManager()
        clock.update("node1", 10)   # → 11
        clock.update("node1", 3)    # max(11, 3) + 1 = 12
        assert clock.node_clocks["node1"] == 12

    def test_clock_update_larger(self):
        """Cập nhật giá trị lớn hơn → nhảy lên đúng."""
        clock = ClockManager()
        clock.update("node2", 100)
        assert clock.node_clocks["node2"] == 101

    def test_min_clock(self):
        """min_clock() = min(tất cả node_clocks)."""
        clock = ClockManager()
        clock.update("node1", 3)   # → 4
        clock.update("node2", 7)   # → 8
        clock.update("node3", 1)   # → 2
        assert clock.min_clock() == min(4, 8, 2)   # = 2

    def test_min_clock_initial(self):
        """Ban đầu tất cả node_clocks = 0 → min_clock() = 0."""
        clock = ClockManager()
        assert clock.min_clock() == 0

    def test_min_clock_all_equal(self):
        """Khi tất cả node_clocks bằng nhau, min_clock = giá trị đó."""
        clock = ClockManager()
        for node in ("node1", "node2", "node3"):
            clock.update(node, 9)   # → 10
        assert clock.min_clock() == 10

    def test_get_all_returns_copy(self):
        """get_all() trả về bản sao, chỉnh sửa không ảnh hưởng state bên trong."""
        clock = ClockManager()
        clock.update("node1", 5)
        snapshot = clock.get_all()
        snapshot["node1"] = 9999
        assert clock.node_clocks["node1"] != 9999

    def test_update_unknown_node_ignored(self):
        """update() với node_id không hợp lệ không làm crash."""
        clock = ClockManager()
        clock.update("nodeX", 100)   # không có trong node_clocks
        assert "nodeX" not in clock.node_clocks


# ===========================================================================
# CTOScheduler tests
# ===========================================================================


class TestCTOScheduler:

    def test_submit_adds_to_queue(self):
        """submit() thêm tx vào wait_queue[step_id]."""
        cm = ClockManager()
        cto = CTOScheduler(cm)
        tx = Transaction.new(step_id=1, machine_id=5, operation="WRITE",
                             data={"status": "running"}, timestamp=cm.tick())
        asyncio.run(cto.submit(tx))
        assert len(cto.wait_queue[1]) == 1

    def test_submit_sorts_by_timestamp(self):
        """submit() giữ hàng đợi sắp xếp tăng dần theo timestamp."""
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
        """
        Nguồn gửi chưa đóng → KHÔNG dispatch (conservative wait).
        """
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
        """
        Nguồn gửi đã đóng → dispatch thành công.
        """
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
        """Sau khi dispatch, tx bị xóa khỏi wait_queue."""
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
        """CTO không bao giờ abort → abort_rate luôn là 0 trong results."""
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
        """record_commit() tính latency_ms đúng và đổi status sang committed."""
        cm = ClockManager()
        cto = CTOScheduler(cm)

        async def _test():
            tx = Transaction.new(1, 5, "WRITE", {}, timestamp=cm.tick())
            await cto.submit(tx)

            commit_time = tx.submit_time + 0.050   # giả lập 50ms sau
            cto.record_commit(tx.tx_id, commit_time)

            entry = cto.pending_results[tx.tx_id]
            assert entry["status"] == "committed"
            assert abs(entry["latency_ms"] - 50.0) < 1.0   # ±1ms tolerance

        asyncio.run(_test())

    def test_cto_get_results(self):
        """get_results() trả về list chứa tất cả entries đã submit."""
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
        """CTO không dispatch tx trẻ trước khi biết tx già không còn đến trễ."""
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


# ===========================================================================
# BTOScheduler tests
# ===========================================================================


class TestBTOScheduler:

    def test_bto_abort(self):
        """
        tx1 timestamp=5 commit trước.
        tx2 timestamp=3 đến sau → ABORT (3 < 5).
        """
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
            assert len(sent) == 2   # tx2 được restart rồi gửi lại
            assert sent[-1].timestamp > 5

        asyncio.run(_test())

    def test_bto_no_abort_higher_timestamp(self):
        """tx2 timestamp > committed → không abort."""
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
        """Sau 10 tx có 3 abort → abort_rate = 30%."""
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            async def noop(t): pass

            # 7 tx commit với timestamp tăng dần (0, 10, 20, ..., 60)
            for i in range(7):
                tx = Transaction.new(99, 50, "WRITE", {}, timestamp=i * 10)
                await bto.execute(tx, noop)

            # 3 tx abort: timestamp nhỏ hơn committed gần nhất (60)
            for i in range(3):
                tx = Transaction.new(99, 50, "WRITE", {}, timestamp=i)
                await bto.execute(tx, noop)

            assert bto.abort_count == 3
            assert bto.total_count == 10
            assert abs(bto.get_abort_rate() - 30.0) < 0.01

        asyncio.run(_test())

    def test_bto_abort_rate_zero(self):
        """Không có abort → abort_rate = 0%."""
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
        """Chưa có tx nào → get_abort_rate() trả về 0.0 (không chia-cho-0)."""
        bto = BTOScheduler(ClockManager())
        assert bto.get_abort_rate() == 0.0

    def test_bto_different_step_ids_independent(self):
        """Abort trên step_id=1 không ảnh hưởng đến step_id=2."""
        cm = ClockManager()
        bto = BTOScheduler(cm)

        async def _test():
            sent: list[Transaction] = []

            async def mock_send(t): sent.append(t)

            # step_id=1: commit ts=10
            tx1 = Transaction.new(1, 5, "WRITE", {}, timestamp=10)
            await bto.execute(tx1, mock_send)

            # step_id=2: commit ts=2 (hợp lệ vì step khác)
            tx2 = Transaction.new(2, 40, "WRITE", {}, timestamp=2)
            ok = await bto.execute(tx2, mock_send)
            assert ok is True
            assert len(sent) == 2

        asyncio.run(_test())

    def test_bto_get_results_contains_aborted(self):
        """get_results() trả về cả tx committed lẫn aborted."""
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


# ===========================================================================
# Integration: CTO vs BTO so sánh
# ===========================================================================


class TestCTOvsBTO:

    def test_cto_zero_abort_vs_bto_nonzero(self):
        """
        Dưới high contention, CTO giữ abort_rate=0 còn BTO có abort.
        Kịch bản: 5 tx cùng step_id, timestamp không tăng đều.
        """
        cm_cto = ClockManager()
        cm_bto = ClockManager()
        cto = CTOScheduler(cm_cto)
        bto = BTOScheduler(cm_bto)

        async def _test():
            cto_dispatched: list[Transaction] = []
            bto_dispatched: list[Transaction] = []

            async def send_cto(t): cto_dispatched.append(t)
            async def send_bto(t): bto_dispatched.append(t)

            # 5 tx cùng step_id=1, timestamp không theo thứ tự gửi
            timestamps = [3, 1, 5, 2, 4]
            for ts in timestamps:
                cto_tx = Transaction.new(1, 5, "WRITE", {}, timestamp=ts)
                bto_tx = Transaction.new(1, 5, "WRITE", {}, timestamp=ts)
                await cto.submit(cto_tx)
                await bto.execute(bto_tx, send_bto)

            cto.close_input()

            # CTO cần gọi try_dispatch nhiều lần (1 tx mỗi lần)
            for _ in range(5):
                await cto.try_dispatch(send_cto)

            # CTO: không abort, dispatch đúng thứ tự timestamp
            cto_results = cto.get_results()
            assert all(r.get("status") != "aborted" for r in cto_results)

            # BTO: phải có abort do timestamp "lộn xộn"
            assert bto.abort_count > 0
            assert bto.get_abort_rate() > 0.0

        asyncio.run(_test())

    def test_transaction_serializable(self):
        """Transaction.to_dict() phải serialize được và có đủ keys."""
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
        """Mỗi Transaction.new() sinh tx_id duy nhất."""
        ids = {
            Transaction.new(1, 1, "WRITE", {}, timestamp=i).tx_id
            for i in range(100)
        }
        assert len(ids) == 100
