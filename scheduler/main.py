
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

sys.path.insert(0, str(Path(__file__).parent))

from clock_manager import ClockManager
from cto_scheduler import CTOScheduler, Transaction
from bto_scheduler import BTOScheduler


HOST: str = os.environ.get("SCHEDULER_HOST", "0.0.0.0")
PORT: int = int(os.environ.get("SCHEDULER_PORT", "8765"))
LOGS_DIR: Path = Path(os.environ.get("LOGS_DIR", "/app/logs"))
DISPATCH_INTERVAL: float = 0.02
BENCH_TIMEOUT: float = float(os.environ.get("BENCH_TIMEOUT", "120"))

logger = logging.getLogger("scheduler.main")


class SchedulerState:

    def __init__(self) -> None:
        self.clock_mgr = ClockManager()
        self.cto = CTOScheduler(self.clock_mgr)
        self.bto = BTOScheduler(self.clock_mgr)

        self.connected_nodes: dict[str, ServerConnection] = {}

        self._submit_times: dict[str, float] = {}

        self._submitted: int = 0
        self._completed: int = 0
        self._session_ended: bool = False
        self._done_event = asyncio.Event()

        try:
            self._params: dict = json.loads(os.environ.get("RUN_PARAMS", "{}"))
        except json.JSONDecodeError:
            self._params = {}


    @staticmethod
    def determine_node(machine_id: int) -> str:
        return f"node{machine_id}"


    async def send_to_node(self, tx: Transaction) -> None:
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


    async def handle_register(self, ws: ServerConnection, msg: dict) -> None:
        node_id = msg.get("node_id", "")
        node_key = f"node{node_id}"
        self.connected_nodes[node_key] = ws
        logger.info(
            "REGISTER %s (port=%s) – nodes online: %s",
            node_key, msg.get("port", "?"),
            list(self.connected_nodes.keys()),
        )

    async def handle_ack(self, msg: dict) -> None:
        node_id_raw: str = msg.get("node_id", "")
        node_key = f"node{node_id_raw}"
        clock_val: int = int(msg.get("clock", 0))
        tx_id: str = msg.get("tx_id", "")
        commit_time: float = float(msg.get("commit_time", time.perf_counter()))

        self.clock_mgr.update(node_key, clock_val)
        logger.debug(
            "ACK tx=%s %s clock=%d | min_clock=%d",
            tx_id[:8], node_key, clock_val, self.clock_mgr.min_clock(),
        )

        self.cto.record_commit(tx_id, commit_time)

        submit_time = self._submit_times.pop(tx_id, None)
        if submit_time is not None:
            latency_ms = (commit_time - submit_time) * 1000
            for entry in self.bto.results:
                if entry["tx_id"] == tx_id and entry["status"] == "committed":
                    entry["latency_ms"] = round(latency_ms, 4)
                    break

        await self.cto.try_dispatch(self.send_to_node)

        self._completed += 1
        self._check_done()

    async def handle_submit(self, msg: dict) -> None:
        mode: str = msg.get("mode", "cto").lower()

        scheduler_tick = self.clock_mgr.tick()
        # Generator gán timestamp trước khi xáo trộn thứ tự gửi.
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
            await self.bto.execute(tx, self.send_to_node)
            self._submitted += 1

    async def handle_session_end(self, msg: dict) -> None:
        self._session_ended = True
        self.cto.close_input()
        logger.info(
            "SESSION_END | submitted=%d completed=%d",
            self._submitted, self._completed,
        )

        for _ in range(10):
            await self.cto.try_dispatch(self.send_to_node)
            await asyncio.sleep(DISPATCH_INTERVAL)

        self._check_done()

        asyncio.create_task(self._watchdog())

    async def _watchdog(self) -> None:
        await asyncio.sleep(BENCH_TIMEOUT)
        if not self._done_event.is_set():
            # Tránh treo benchmark nếu node mất ACK hoặc connection lỗi.
            logger.warning(
                "Watchdog: quá %.0fs sau session_end – buộc hoàn tất "
                "(completed=%d/%d)",
                BENCH_TIMEOUT, self._completed, self._submitted,
            )
            self._done_event.set()


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


    def save_results(self) -> None:
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


async def connection_handler(ws: ServerConnection, state: SchedulerState) -> None:
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
                pass

            else:
                logger.warning("Type không xử lý: %s từ %s", msg_type, peer)

    except websockets.exceptions.ConnectionClosedOK:
        logger.info("Kết nối đóng bình thường: %s", peer)
    except websockets.exceptions.ConnectionClosedError as exc:
        logger.warning("Kết nối đóng lỗi: %s – %s", peer, exc)
    finally:
        for key, val in list(state.connected_nodes.items()):
            if val is ws:
                del state.connected_nodes[key]
                logger.info("Node %s ngắt kết nối", key)
                break


async def dispatch_loop(state: SchedulerState) -> None:
    while True:
        await asyncio.sleep(DISPATCH_INTERVAL)

        n = await state.cto.try_dispatch(state.send_to_node)
        if n:
            logger.debug("dispatch_loop: dispatched %d tx", n)


async def main() -> None:
    state = SchedulerState()

    dispatch_task = asyncio.create_task(dispatch_loop(state))

    handler = lambda ws: connection_handler(ws, state)

    logger.info("Scheduler khởi động tại ws://%s:%d", HOST, PORT)

    async with serve(handler, HOST, PORT) as server:
        logger.info("Đang lắng nghe – chờ Node Agent và Workload Generator kết nối...")

        try:
            await asyncio.wait_for(state._done_event.wait(), timeout=None)
        except asyncio.CancelledError:
            pass
        finally:
            dispatch_task.cancel()

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
