
import logging
import threading

logger = logging.getLogger(__name__)


class ClockManager:

    def __init__(self) -> None:
        self.lamport_clock: int = 0
        self.node_clocks: dict[str, int] = {
            "node1": 0,
            "node2": 0,
            "node3": 0,
        }
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self.lamport_clock += 1
            logger.debug("Lamport tick → %d", self.lamport_clock)
            return self.lamport_clock

    def update(self, node_id: str, value: int) -> None:
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
        with self._lock:
            result = min(self.node_clocks.values())
            logger.debug("min_clock() = %d  |  clocks = %s", result, self.node_clocks)
            return result

    def get_all(self) -> dict[str, int]:
        with self._lock:
            return dict(self.node_clocks)

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"ClockManager(lamport={self.lamport_clock}, nodes={self.node_clocks})"
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cm = ClockManager()
    print("=== ClockManager smoke test ===")

    t1 = cm.tick()
    t2 = cm.tick()
    assert t1 == 1, f"Mong đợi 1, nhận {t1}"
    assert t2 == 2, f"Mong đợi 2, nhận {t2}"
    print(f"tick(): {t1}, {t2}  ✓")

    cm.update("node1", 5)
    assert cm.node_clocks["node1"] == 6, f"Mong đợi 6, nhận {cm.node_clocks['node1']}"
    print(f"update('node1', 5) → node1={cm.node_clocks['node1']}  ✓")

    cm.update("node2", 7)
    cm.update("node3", 1)
    m = cm.min_clock()
    assert m == 2, f"Mong đợi 2, nhận {m}"
    print(f"min_clock() = {m}  ✓")

    all_clocks = cm.get_all()
    assert isinstance(all_clocks, dict)
    assert set(all_clocks.keys()) == {"node1", "node2", "node3"}
    print(f"get_all() = {all_clocks}  ✓")

    print(cm)
    print("=== Tất cả test passed ===")
