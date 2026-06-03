"""
Lamport Clock Manager – quản lý Lamport timestamp cho CTO Scheduler.

Theo mô hình Conservative Timestamp Ordering (Özsu & Valduriez):
- lamport_clock: đồng hồ toàn cục của Scheduler
- node_clocks: theo dõi clock cuối cùng nhận được từ mỗi Node Agent
- min_clock(): thống kê clock nhỏ nhất của các node
"""

import logging
import threading

logger = logging.getLogger(__name__)


class ClockManager:
    """
    Quản lý Lamport clock cho hệ thống phân tán 3 node.

    Thread-safe: dùng lock vì nhiều coroutine/thread có thể gọi đồng thời
    qua WebSocket handler.
    """

    def __init__(self) -> None:
        self.lamport_clock: int = 0
        self.node_clocks: dict[str, int] = {
            "node1": 0,
            "node2": 0,
            "node3": 0,
        }
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Tăng lamport_clock lên 1, trả về giá trị mới."""
        with self._lock:
            self.lamport_clock += 1
            logger.debug("Lamport tick → %d", self.lamport_clock)
            return self.lamport_clock

    def update(self, node_id: str, value: int) -> None:
        """
        Cập nhật clock của node khi nhận ACK: node_clocks[node_id] = max(current, value) + 1.

        Quy tắc Lamport: khi nhận message với timestamp T,
        đặt clock = max(local_clock, T) + 1.
        """
        with self._lock:
            if node_id not in self.node_clocks:
                logger.warning("node_id không hợp lệ: %s", node_id)
                return
            old = self.node_clocks[node_id]
            self.node_clocks[node_id] = max(old, value) + 1
            logger.debug(
                "update %s: %d → %d (nhận value=%d)",
                node_id,
                old,
                self.node_clocks[node_id],
                value,
            )

    def min_clock(self) -> int:
        """
        Trả về min của tất cả node_clocks values.

        Hữu ích để quan sát tiến độ logic của node.
        """
        with self._lock:
            result = min(self.node_clocks.values())
            logger.debug("min_clock() = %d  |  clocks = %s", result, self.node_clocks)
            return result

    def get_all(self) -> dict[str, int]:
        """Trả về bản sao của toàn bộ node_clocks."""
        with self._lock:
            return dict(self.node_clocks)

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"ClockManager(lamport={self.lamport_clock}, nodes={self.node_clocks})"
        )


# ---------------------------------------------------------------------------
# Chạy độc lập để smoke-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cm = ClockManager()
    print("=== ClockManager smoke test ===")

    # tick
    t1 = cm.tick()
    t2 = cm.tick()
    assert t1 == 1, f"Mong đợi 1, nhận {t1}"
    assert t2 == 2, f"Mong đợi 2, nhận {t2}"
    print(f"tick(): {t1}, {t2}  ✓")

    # update
    cm.update("node1", 5)
    assert cm.node_clocks["node1"] == 6, f"Mong đợi 6, nhận {cm.node_clocks['node1']}"
    print(f"update('node1', 5) → node1={cm.node_clocks['node1']}  ✓")

    # min_clock sau khi cập nhật nhiều node
    cm.update("node2", 7)   # node2 = 8
    cm.update("node3", 1)   # node3 = 2
    # node1=6, node2=8, node3=2 → min=2
    m = cm.min_clock()
    assert m == 2, f"Mong đợi 2, nhận {m}"
    print(f"min_clock() = {m}  ✓")

    # get_all
    all_clocks = cm.get_all()
    assert isinstance(all_clocks, dict)
    assert set(all_clocks.keys()) == {"node1", "node2", "node3"}
    print(f"get_all() = {all_clocks}  ✓")

    print(cm)
    print("=== Tất cả test passed ===")
