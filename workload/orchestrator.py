
from __future__ import annotations

import argparse
import contextlib
import json
import logging
import math
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

logger = logging.getLogger("workload.orchestrator")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
RUNTIME_DIR = LOGS_DIR / "runtime"
DATA_DIR = PROJECT_ROOT / "data" / "generated"

DEFAULT_PORT = 8765
STATUSES = ["pending"]

EventFn = Callable[[str], None]


def generate_dataset(num_nodes: int, dataset_size: int, out_dir: Path) -> dict[int, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    block = max(1, math.ceil(dataset_size / max(1, num_nodes)))

    paths: dict[int, Path] = {}
    for node_id in range(1, num_nodes + 1):
        lo = (node_id - 1) * block + 1
        hi = min(node_id * block, dataset_size)
        records = [
            {"stepID": s, "machineID": node_id, "status": "pending"}
            for s in range(lo, hi + 1)
        ]
        path = out_dir / f"node{node_id}_data.json"
        path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        paths[node_id] = path
    return paths


def _python() -> str:
    return sys.executable


def _wait_port(port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.2)
    return False


def _terminate_all(procs: dict[str, tuple[subprocess.Popen, object]]) -> None:
    for _name, (p, _logf) in procs.items():
        if p.poll() is None:
            with contextlib.suppress(Exception):
                p.terminate()
    time.sleep(0.7)
    for _name, (p, _logf) in procs.items():
        if p.poll() is None:
            with contextlib.suppress(Exception):
                p.kill()
    for _name, (_p, logf) in procs.items():
        with contextlib.suppress(Exception):
            logf.close()


def read_runtime_logs() -> dict[str, str]:
    logs: dict[str, str] = {}
    if not RUNTIME_DIR.exists():
        return logs
    def sort_key(p: Path):
        n = p.stem
        order = {"scheduler": 0, "generator": 9}.get(n, 5)
        return (order, n)

    for path in sorted(RUNTIME_DIR.glob("*.log"), key=sort_key):
        with contextlib.suppress(Exception):
            logs[path.stem] = path.read_text(encoding="utf-8", errors="replace")
    return logs


def run_benchmark(
    *,
    mode: str = "both",
    num_nodes: int = 3,
    active_nodes: list[int] | None = None,
    node_delays_ms: dict[int, float] | None = None,
    dataset_size: int = 3000,
    num_transactions: int = 1000,
    concurrency: int = 50,
    contention: float = 0.5,
    hot_steps: int = 10,
    seed: int | None = None,
    port: int = DEFAULT_PORT,
    timeout: float = 180.0,
    on_event: EventFn | None = None,
) -> dict | None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    active = sorted(active_nodes) if active_nodes else list(range(1, num_nodes + 1))
    delays = node_delays_ms or {}

    def emit(msg: str) -> None:
        logger.info(msg)
        if on_event is not None:
            on_event(msg)

    for old in RUNTIME_DIR.glob("*.log"):
        with contextlib.suppress(Exception):
            old.unlink()

    procs: dict[str, tuple[subprocess.Popen, object]] = {}

    def spawn(name: str, args: list[str], env: dict[str, str]) -> subprocess.Popen:
        logf = (RUNTIME_DIR / f"{name}.log").open("w", encoding="utf-8")
        p = subprocess.Popen(args, cwd=str(PROJECT_ROOT), env=env,
                             stdout=logf, stderr=subprocess.STDOUT)
        procs[name] = (p, logf)
        return p

    base_env = dict(os.environ)
    bench_timeout = max(30.0, timeout - 20.0)
    run_params = {
        "mode": mode,
        "num_nodes": num_nodes,
        "active_nodes": active,
        "node_delays_ms": {str(k): v for k, v in delays.items() if v},
        "dataset_size": dataset_size,
        "num_transactions": num_transactions,
        "concurrency": concurrency,
        "contention": contention,
        "hot_steps": hot_steps,
    }

    try:
        emit(f"🗂️  Sinh dataset {dataset_size} step chia đều cho {num_nodes} node...")
        data_paths = generate_dataset(num_nodes, dataset_size, DATA_DIR)

        emit("🚀 Khởi động Scheduler...")
        sched_env = {
            **base_env,
            "SCHEDULER_HOST": "0.0.0.0",
            "SCHEDULER_PORT": str(port),
            "LOGS_DIR": str(LOGS_DIR),
            "BENCH_TIMEOUT": str(bench_timeout),
            "NUM_NODES": str(num_nodes),
            "RUN_PARAMS": json.dumps(run_params, ensure_ascii=False),
        }
        spawn("scheduler", [_python(), str(PROJECT_ROOT / "scheduler" / "main.py")], sched_env)
        if not _wait_port(port, timeout=15.0):
            emit("❌ Scheduler không mở được cổng – xem logs/runtime/scheduler.log")
            return None
        emit(f"✅ Scheduler đang lắng nghe ws://localhost:{port}")

        emit(f"🖥️  Khởi động {len(active)} node đang bật: {active}")
        for nid in active:
            delay = float(delays.get(nid, 0) or 0)
            node_env = {
                **base_env,
                "NODE_ID": str(nid),
                "NODE_PORT": str(9000 + nid),
                "SCHEDULER_HOST": "localhost",
                "SCHEDULER_PORT": str(port),
                "DATA_FILE": str(data_paths[nid]),
                "SLOW_DELAY_MS": str(delay),
            }
            spawn(f"node{nid}", [_python(), str(PROJECT_ROOT / "node_agent" / "agent.py")], node_env)
            if delay > 0:
                emit(f"   • node{nid}: delay {delay:.0f}ms/tx")
        time.sleep(2.0)

        emit(f"📤 Gửi {num_transactions} transaction (mode={mode}, contention={contention:.0%})...")
        gen_args = [
            _python(), str(PROJECT_ROOT / "workload" / "generator.py"),
            "--mode", mode,
            "--num-nodes", str(num_nodes),
            "--active-nodes", ",".join(str(n) for n in active),
            "--dataset-size", str(dataset_size),
            "--num-transactions", str(num_transactions),
            "--contention", str(contention),
            "--hot-steps", str(hot_steps),
            "--concurrency", str(concurrency),
            "--host", "localhost",
            "--port", str(port),
        ]
        if seed is not None:
            gen_args += ["--seed", str(seed)]
        spawn("generator", gen_args, base_env)

        emit("⏳ Đang xử lý transaction & tính latency...")
        try:
            procs["scheduler"][0].wait(timeout=timeout)
            emit("✅ Scheduler đã lưu kết quả và kết thúc.")
        except subprocess.TimeoutExpired:
            emit(f"⚠️ Quá {timeout:.0f}s – dừng tiến trình (kết quả có thể chưa đủ).")
    finally:
        _terminate_all(procs)

    result_path = LOGS_DIR / "result.json"
    if result_path.exists():
        try:
            return json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            emit("❌ result.json lỗi định dạng.")
            return None
    emit("❌ Không tìm thấy result.json.")
    return None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CTO/BTO Benchmark Orchestrator")
    p.add_argument("--mode", choices=["cto", "bto", "both"], default="both")
    p.add_argument("--num-nodes", type=int, default=3)
    p.add_argument("--active-nodes", default="", help="VD '1,2,3' (mặc định: tất cả)")
    p.add_argument("--dataset-size", type=int, default=3000)
    p.add_argument("--num-transactions", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--contention", type=float, default=0.5)
    p.add_argument("--hot-steps", type=int, default=10)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--timeout", type=float, default=180.0)
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    a = _parse_args()
    active = [int(x) for x in a.active_nodes.split(",") if x.strip()] or None
    result = run_benchmark(
        mode=a.mode,
        num_nodes=a.num_nodes,
        active_nodes=active,
        dataset_size=a.dataset_size,
        num_transactions=a.num_transactions,
        concurrency=a.concurrency,
        contention=a.contention,
        hot_steps=a.hot_steps,
        seed=a.seed,
        port=a.port,
        timeout=a.timeout,
        on_event=lambda m: None,
    )
    if result is None:
        sys.exit(1)
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
