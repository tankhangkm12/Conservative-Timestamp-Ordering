
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path

# pyrefly: ignore [missing-import]
import websockets
# pyrefly: ignore [missing-import]
from websockets.asyncio.client import ClientConnection

logger = logging.getLogger(__name__)


NODE_ID: str = os.environ.get("NODE_ID", "1")
NODE_PORT: str = os.environ.get("NODE_PORT", "9001")
SCHEDULER_HOST: str = os.environ.get("SCHEDULER_HOST", "localhost")
SCHEDULER_PORT: str = os.environ.get("SCHEDULER_PORT", "8765")
DATA_FILE: str = os.environ.get("DATA_FILE", "/app/data.json")

SLOW_DELAY_SEC: float = float(os.environ.get("SLOW_DELAY_MS", "0")) / 1000.0

SCHEDULER_URI: str = f"ws://{SCHEDULER_HOST}:{SCHEDULER_PORT}"

RECONNECT_DELAY_SEC: float = 2.0


class NodeAgent:

    def __init__(self) -> None:
        self.node_id: str = NODE_ID
        self.data_file: Path = Path(DATA_FILE)
        self.records: list[dict] = []
        self.local_clock: int = 0
        self._dirty: bool = False


    def load_data(self) -> None:
        if not self.data_file.exists():
            logger.warning("DATA_FILE không tồn tại: %s – khởi tạo danh sách rỗng", self.data_file)
            self.records = []
            return

        with self.data_file.open("r", encoding="utf-8") as f:
            self.records = json.load(f)

        logger.info(
            "Node%s: đã load %d records từ %s",
            self.node_id, len(self.records), self.data_file,
        )

    def save_data(self) -> None:
        tmp = self.data_file.with_suffix(".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2)
            # Atomic replace để tránh file JSON dở dang khi process bị ngắt.
            tmp.replace(self.data_file)
        except OSError as exc:
            logger.warning(
                "Node%s: atomic rename thất bại (%s) – ghi in-place",
                self.node_id, exc,
            )
            tmp.unlink(missing_ok=True)
            with self.data_file.open("w", encoding="utf-8") as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2)
        self._dirty = False
        logger.debug("Node%s: đã ghi %d records xuống %s", self.node_id, len(self.records), self.data_file)

    def find_by_step_id(self, step_id: int) -> dict | None:
        for record in self.records:
            if record.get("stepID") == step_id:
                return record
        return None


    def advance_clock(self, received: int | None = None) -> int:
        if received is not None:
            self.local_clock = max(self.local_clock, received) + 1
        else:
            self.local_clock += 1
        return self.local_clock


    async def handle_execute(self, ws: ClientConnection, message: dict) -> None:
        tx = message["transaction"]

        if SLOW_DELAY_SEC > 0:
            await asyncio.sleep(SLOW_DELAY_SEC)

        step_id: int = tx["step_id"]
        operation: str = tx["operation"]
        tx_id: str = tx["tx_id"]
        tx_timestamp: int = tx.get("timestamp", 0)

        self.advance_clock(received=tx_timestamp)

        record = self.find_by_step_id(step_id)

        if record is None:
            logger.warning(
                "Node%s: step_id=%d không tìm thấy trong data",
                self.node_id, step_id,
            )
        else:
            if operation == "WRITE":
                old_status = record.get("status", "?")
                record["status"] = tx["data"]["status"]
                self._dirty = True
                logger.info(
                    "Node%s: WRITE step=%d machine=%d  %s → %s",
                    self.node_id, step_id, record.get("machineID", "?"),
                    old_status, record["status"],
                )
                self.save_data()

            elif operation == "READ":
                logger.info(
                    "Node%s: READ step=%d status=%s",
                    self.node_id, step_id, record.get("status"),
                )

        clock_val = self.advance_clock()
        commit_time = time.perf_counter()

        ack = {
            "type": "ack",
            "tx_id": tx_id,
            "node_id": self.node_id,
            "clock": clock_val,
            "commit_time": commit_time,
        }
        await ws.send(json.dumps(ack))
        logger.debug("Node%s: ACK tx=%s clock=%d", self.node_id, tx_id[:8], clock_val)


    async def run(self) -> None:
        self.load_data()

        while True:
            try:
                logger.info(
                    "Node%s: kết nối đến Scheduler %s ...",
                    self.node_id, SCHEDULER_URI,
                )
                async with websockets.connect(SCHEDULER_URI) as ws:
                    await self._session(ws)

            except (OSError, websockets.exceptions.WebSocketException) as exc:
                logger.warning(
                    "Node%s: mất kết nối (%s) – thử lại sau %.1fs",
                    self.node_id, exc, RECONNECT_DELAY_SEC,
                )
                await asyncio.sleep(RECONNECT_DELAY_SEC)

    async def _session(self, ws: ClientConnection) -> None:
        register_msg = {
            "type": "register",
            "node_id": self.node_id,
            "port": NODE_PORT,
        }
        await ws.send(json.dumps(register_msg))
        logger.info(
            "Node%s: đã gửi REGISTER (port=%s, records=%d, delay=%.0fms)",
            self.node_id, NODE_PORT, len(self.records), SLOW_DELAY_SEC * 1000,
        )

        async for raw in ws:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.error("Node%s: nhận được message không hợp lệ: %r", self.node_id, raw)
                continue

            msg_type = message.get("type")

            if msg_type == "execute":
                await self.handle_execute(ws, message)

            elif msg_type == "ping":
                await ws.send(json.dumps({"type": "pong", "node_id": self.node_id}))

            else:
                logger.warning("Node%s: message type không xử lý: %s", self.node_id, msg_type)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] node%(NODE_ID)s %(name)s: %(message)s".replace(
            "%(NODE_ID)s", NODE_ID
        ),
    )

    if SLOW_DELAY_SEC > 0:
        logger.warning("Node%s: delay %.0fms mỗi transaction", NODE_ID, SLOW_DELAY_SEC * 1000)

    agent = NodeAgent()
    asyncio.run(agent.run())
