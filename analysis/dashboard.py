"""
Streamlit Dashboard – CTO vs BTO, điều khiển benchmark hoàn toàn bằng tham số.

Chạy:
  uv run streamlit run analysis/dashboard.py

Mọi thứ điều khiển từ đây (không dùng dòng lệnh, không còn scenario cố định):
  - Chọn số node, bật/tắt từng node, đặt delay riêng cho mỗi node.
  - Chọn kích thước dataset (chia đều cho các node), số transaction, concurrency,
    mức tranh chấp (contention), số hot step.
  - Bấm "Chạy Benchmark" → tự khởi động Scheduler + Node Agent + Generator.
  - Xem kết quả + log của từng tiến trình con ngay trên trang.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
RESULT_PATH = LOGS_DIR / "result.json"

sys.path.insert(0, str(PROJECT_ROOT / "workload"))
# pyrefly: ignore [missing-import]
import orchestrator  # noqa: E402

import json  # noqa: E402

MODE_LABELS = {"both": "Cả hai (CTO + BTO)", "cto": "Chỉ CTO", "bto": "Chỉ BTO"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_result() -> dict | None:
    if not RESULT_PATH.exists():
        return None
    try:
        return json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def latencies(transactions: list[dict]) -> list[float]:
    return [
        t["latency_ms"] for t in transactions
        if t.get("status") == "committed" and t.get("latency_ms") is not None
    ]


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.set_page_config(page_title="CTO vs BTO – Đề tài #27", page_icon="📊", layout="wide")
st.title("CTO vs BTO – Phân tích thực nghiệm Đề tài #27")
st.caption("Conservative Timestamp Ordering · Automated Manufacturing · Đỗ Thanh Tân – N23DCCN122")


# ---------------------------------------------------------------------------
# Sidebar – điều khiển benchmark
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("⚙️ Điều khiển Benchmark")

    mode = st.selectbox("Chế độ", ["both", "cto", "bto"],
                        format_func=lambda m: MODE_LABELS[m])

    num_nodes = st.slider("Số node", min_value=1, max_value=8, value=3)

    st.markdown("**Node** (bật/tắt · delay ms)")
    active_nodes: list[int] = []
    node_delays: dict[int, float] = {}
    for nid in range(1, num_nodes + 1):
        c_en, c_delay = st.columns([1, 1.3])
        enabled = c_en.checkbox(f"node{nid}", value=True, key=f"node_en_{nid}")
        delay = c_delay.number_input(
            "delay", min_value=0, max_value=5000, value=0, step=50,
            key=f"node_delay_{nid}", label_visibility="collapsed",
            disabled=not enabled,
        )
        if enabled:
            active_nodes.append(nid)
            node_delays[nid] = float(delay)
    if not active_nodes:
        st.warning("Cần bật ít nhất 1 node.")

    st.divider()
    dataset_size = st.number_input("Kích thước dataset (số step, chia đều)",
                                   min_value=num_nodes, max_value=1_000_000,
                                   value=3000, step=500)
    num_transactions = st.number_input("Số transaction sinh ra",
                                       min_value=1, max_value=200_000,
                                       value=1000, step=100)
    concurrency = st.number_input("Concurrency (gửi song song)",
                                  min_value=1, max_value=2000, value=50, step=10)
    contention = st.slider("Mức tranh chấp (% dồn vào hot step)",
                           min_value=0, max_value=100, value=50, step=5,
                           help="0% = phân phối đều · cao = nhiều tx cùng step → BTO abort nhiều")
    hot_steps = st.number_input("Số hot step (vùng tranh chấp)",
                                min_value=1, max_value=10_000, value=10, step=1)

    with st.expander("Tuỳ chọn nâng cao"):
        use_seed = st.checkbox("Cố định random seed", value=False)
        seed = st.number_input("Seed", min_value=0, value=42, step=1, disabled=not use_seed)
        timeout = st.number_input("Timeout tổng (giây)", min_value=30, max_value=3600,
                                  value=180, step=30)

    run_btn = st.button("▶️ Chạy Benchmark", type="primary",
                        width="stretch", disabled=not active_nodes)

    if run_btn:
        with st.status("Đang chạy benchmark...", expanded=True) as status:
            try:
                result = orchestrator.run_benchmark(
                    mode=mode,
                    num_nodes=num_nodes,
                    active_nodes=active_nodes,
                    node_delays_ms=node_delays,
                    dataset_size=int(dataset_size),
                    num_transactions=int(num_transactions),
                    concurrency=int(concurrency),
                    contention=contention / 100.0,
                    hot_steps=int(hot_steps),
                    seed=int(seed) if use_seed else None,
                    timeout=float(timeout),
                    on_event=lambda m: status.write(m),
                )
            except Exception as exc:  # noqa: BLE001
                status.update(label=f"Lỗi: {exc}", state="error")
                result = None

            if result is not None:
                s = result.get("summary", {})
                status.write(
                    f"submitted={s.get('total_submitted')} · "
                    f"completed={s.get('total_completed')} · "
                    f"BTO abort/restart={s.get('bto_aborted')}"
                )
                status.update(label="✅ Hoàn tất!", state="complete")
            else:
                status.update(label="❌ Thất bại – xem log bên dưới", state="error")

        st.rerun()


# ---------------------------------------------------------------------------
# Kết quả
# ---------------------------------------------------------------------------

data = load_result()

if data is None:
    st.info("👈 Nhập tham số ở thanh bên rồi bấm **▶️ Chạy Benchmark** để bắt đầu.")
else:
    params = data.get("params", {})
    summary = data.get("summary", {})
    cto_data, bto_data = data["cto"], data["bto"]
    cto_lat, bto_lat = latencies(cto_data.get("transactions", [])), latencies(bto_data.get("transactions", []))

    # Tham số đã dùng
    if params:
        st.subheader("Tham số phiên chạy")
        pc = st.columns(6)
        pc[0].metric("Số node", params.get("num_nodes", "?"))
        pc[1].metric("Node bật", len(params.get("active_nodes", [])))
        pc[2].metric("Dataset", params.get("dataset_size", "?"))
        pc[3].metric("Transactions", params.get("num_transactions", "?"))
        pc[4].metric("Contention", f"{params.get('contention', 0) * 100:.0f}%")
        pc[5].metric("Hot steps", params.get("hot_steps", "?"))
        if params.get("node_delays_ms"):
            st.caption("Delay node: " + ", ".join(
                f"node{k}={v:.0f}ms" for k, v in params["node_delays_ms"].items()))

    st.divider()
    st.subheader("Kết quả CTO vs BTO")
    col_cto, col_bto = st.columns(2)
    with col_cto:
        st.markdown("### CTO")
        m = st.columns(3)
        m[0].metric("Avg Latency (ms)", f"{cto_data['avg_latency_ms']:.2f}")
        m[1].metric("Abort Rate (%)", f"{cto_data['abort_rate']:.2f}")
        m[2].metric("Std Latency (ms)", f"{cto_data['std_latency_ms']:.2f}")
    with col_bto:
        st.markdown("### BTO")
        m = st.columns(3)
        m[0].metric("Avg Latency (ms)", f"{bto_data['avg_latency_ms']:.2f}",
                    delta=f"{bto_data['avg_latency_ms'] - cto_data['avg_latency_ms']:.2f} vs CTO",
                    delta_color="inverse")
        m[1].metric("Abort Rate (%)", f"{bto_data['abort_rate']:.2f}")
        m[2].metric("Std Latency (ms)", f"{bto_data['std_latency_ms']:.2f}")

    st.divider()

    # Biểu đồ
    cc1, cc2 = st.columns(2)
    with cc1:
        fig = px.bar(
            pd.DataFrame({"Scheduler": ["CTO", "BTO"],
                          "Avg Latency (ms)": [cto_data["avg_latency_ms"], bto_data["avg_latency_ms"]]}),
            x="Scheduler", y="Avg Latency (ms)", color="Scheduler",
            color_discrete_map={"CTO": "#2196F3", "BTO": "#FF9800"},
            text_auto=".2f", title="Average Transaction Latency (ms)")
        fig.update_layout(showlegend=False, height=380)
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, width="stretch")
    with cc2:
        fig = px.bar(
            pd.DataFrame({"Scheduler": ["CTO", "BTO"],
                          "Abort Rate (%)": [cto_data["abort_rate"], bto_data["abort_rate"]]}),
            x="Scheduler", y="Abort Rate (%)", color="Scheduler",
            color_discrete_map={"CTO": "#2196F3", "BTO": "#FF9800"},
            text_auto=".2f", title="Abort / Restart Rate (%)")
        fig.update_layout(showlegend=False, height=380)
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, width="stretch")

    if cto_lat or bto_lat:
        rows = ([{"latency_ms": v, "Scheduler": "CTO"} for v in cto_lat]
                + [{"latency_ms": v, "Scheduler": "BTO"} for v in bto_lat])
        fig = px.histogram(pd.DataFrame(rows), x="latency_ms", color="Scheduler",
                           barmode="overlay", nbins=50, opacity=0.7,
                           color_discrete_map={"CTO": "#2196F3", "BTO": "#FF9800"},
                           labels={"latency_ms": "Latency (ms)"}, title="Latency Distribution")
        fig.update_layout(height=400)
        st.plotly_chart(fig, width="stretch")

        timeline = ([{"index": i, "latency_ms": v, "Scheduler": "CTO"} for i, v in enumerate(cto_lat)]
                    + [{"index": i, "latency_ms": v, "Scheduler": "BTO"} for i, v in enumerate(bto_lat)])
        fig = px.line(pd.DataFrame(timeline), x="index", y="latency_ms", color="Scheduler",
                      color_discrete_map={"CTO": "#2196F3", "BTO": "#FF9800"},
                      labels={"index": "Transaction Index", "latency_ms": "Latency (ms)"},
                      title="Latency theo thời gian (Timeline)")
        fig.update_layout(height=400)
        st.plotly_chart(fig, width="stretch")

    # Kết luận tự động
    st.divider()
    st.subheader("Kết luận")
    cto_abort, bto_abort = cto_data["abort_rate"], bto_data["abort_rate"]
    diff = round(cto_data["avg_latency_ms"] - bto_data["avg_latency_ms"], 2)
    if cto_abort == 0.0 and bto_abort > 0:
        st.markdown("✅ **CTO: Abort Rate = 0%** – phù hợp Automated Manufacturing. "
                    f"BTO phải abort/restart **{summary.get('bto_aborted', 0)}** lần.")
    elif cto_abort == 0.0 and bto_abort == 0.0:
        st.markdown("ℹ️ Cả CTO và BTO đều Abort = 0% (tranh chấp thấp). "
                    "Tăng **contention** hoặc giảm **hot steps** để thấy BTO abort.")
    if diff > 0:
        st.markdown(f"⏱ **Trade-off:** CTO cao hơn BTO **{diff} ms** "
                    f"(CTO={cto_data['avg_latency_ms']:.2f}, BTO={bto_data['avg_latency_ms']:.2f}) "
                    "– chi phí buffer transaction đến khi nguồn gửi đóng.")
    elif diff < 0:
        st.markdown(f"⏱ CTO nhanh hơn BTO **{abs(diff)} ms** (BTO tốn thời gian abort/restart).")


# ---------------------------------------------------------------------------
# Log tiến trình con
# ---------------------------------------------------------------------------

st.divider()
with st.expander("🪵 Log tiến trình con (lần chạy gần nhất)", expanded=False):
    logs = orchestrator.read_runtime_logs()
    if not logs:
        st.caption("Chưa có log. Chạy một benchmark để xem log của Scheduler, Node, Generator.")
    else:
        tabs = st.tabs(list(logs.keys()))
        for tab, (name, text) in zip(tabs, logs.items()):
            with tab:
                st.code(text or "(rỗng)", language="log", line_numbers=False)
