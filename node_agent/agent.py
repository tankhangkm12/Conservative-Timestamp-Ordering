"""
Node Agent – WebSocket client đại diện cho một node lưu trữ.

Biến môi trường bắt buộc:
  NODE_ID   : "1" | "2" | "3"
  NODE_PORT : "9001" | "9002" | "9003"

Biến môi trường tuỳ chọn:
  SCHEDULER_HOST : host của Scheduler (mặc định "scheduler", hoặc "localhost" khi dev)
  SCHEDULER_PORT : cổng WebSocket Scheduler (mặc định "8765")
  DATA_FILE      : đường dẫn file JSON dữ liệu (mặc định "/app/data.json")
  SLOW_NODE      : "true" để kích hoạt delay 300ms (dùng cho kịch bản slow_node)

Protocol với Scheduler:
  → REGISTER   {"type": "register",   "node_id": NODE_ID}
  ← EXECUTE    {"type": "execute",    "transaction": {...}}
  → ACK        {"type": "ack",        "tx_id", "node_id", "clock", "commit_time"}
"""

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

# ---------------------------------------------------------------------------
# Config từ môi trường
# ---------------------------------------------------------------------------

NODE_ID: str = os.environ.get("NODE_ID", "1")
NODE_PORT: str = os.environ.get("NODE_PORT", "9001")
SCHEDULER_HOST: str = os.environ.get("SCHEDULER_HOST", "localhost")
SCHEDULER_PORT: str = os.environ.get("SCHEDULER_PORT", "8765")
DATA_FILE: str = os.environ.get("DATA_FILE", "/app/data.json")

# Độ trễ nhân tạo của node này (ms) – chỉnh được từ Streamlit để mô phỏng node chậm.
SLOW_DELAY_SEC: float = float(os.environ.get("SLOW_DELAY_MS", "0")) / 1000.0

SCHEDULER_URI: str = f"ws://{SCHEDULER_HOST}:{SCHEDULER_PORT}"

# Thời gian chờ trước khi reconnect (giây)
RECONNECT_DELAY_SEC: float = 2.0


# ---------------------------------------------------------------------------
# NodeAgent
# ---------------------------------------------------------------------------


class NodeAgent:
    """
    Quản lý toàn bộ vòng đời của một Node Agent:
    load dữ liệu, kết nối Scheduler, xử lý transaction.
    """

    def __init__(self) -> None:
        self.node_id: str = NODE_ID
        self.data_file: Path = Path(DATA_FILE)
        self.records: list[dict] = []          # list record trong memory
        self.local_clock: int = 0              # Lamport clock cục bộ
        self._dirty: bool = False              # cần flush xuống file không

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Đọc toàn bộ JSON data vào memory."""
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
        """
        Ghi records hiện tại xuống file JSON.

        Ưu tiên atomic write qua temp file + rename. Nếu data_file là một
        single-file bind mount trong Docker, rename sẽ thất bại với
        ``[Errno 16] Device or resource busy`` (không thể rename đè lên mount
        point). Trong trường hợp đó fallback sang ghi in-place để node không
        crash. (Khuyến nghị: mount cả thư mục ./data thay vì từng file.)
        """
        tmp = self.data_file.with_suffix(".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2)
            tmp.replace(self.data_file)   # atomic rename
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
        """Tìm record theo step_id (linear scan – dataset nhỏ ~3400 records)."""
        for record in self.records:
            if record.get("stepID") == step_id:
                return record
        return None

    # ------------------------------------------------------------------
    # Clock
    # ------------------------------------------------------------------

    def advance_clock(self, received: int | None = None) -> int:
        """
        Cập nhật local_clock theo quy tắc Lamport:
          - Nếu nhận giá trị từ ngoài: clock = max(local, received) + 1
          - Nếu sự kiện nội bộ: clock += 1
        Trả về giá trị mới.
        """
        if received is not None:
            self.local_clock = max(self.local_clock, received) + 1
        else:
            self.local_clock += 1
        return self.local_clock

    # ------------------------------------------------------------------
    # Message handlers
    # ------------------------------------------------------------------

    async def handle_execute(self, ws: ClientConnection, message: dict) -> None:
        """Xử lý transaction EXECUTE từ Scheduler."""
        tx = message["transaction"]

        # Độ trễ nhân tạo (mô phỏng node chậm) – cấu hình qua SLOW_DELAY_MS.
        if SLOW_DELAY_SEC > 0:
            await asyncio.sleep(SLOW_DELAY_SEC)

        step_id: int = tx["step_id"]
        operation: str = tx["operation"]
        tx_id: str = tx["tx_id"]
        tx_timestamp: int = tx.get("timestamp", 0)

        # Cập nhật clock theo timestamp của transaction
        self.advance_clock(received=tx_timestamp)

        record = self.find_by_step_id(step_id)

        if record is None:
            # Không tìm thấy record – vẫn gửi ACK để không block Scheduler
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

        # Tăng clock cho sự kiện gửi ACK
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

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Vòng lặp chính: kết nối → register → nhận & xử lý message."""
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
        """Một phiên kết nối WebSocket đã mở: register rồi xử lý message."""
        # Đăng ký với Scheduler
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

        # Nhận message liên tục
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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
