
import json
from pathlib import Path


NODE_SPECS = [
    (1,   33,  3_241),
    (34,  66,  3_342),
    (67, 100,  3_417),
]

OUTPUT_DIR = Path(__file__).parent


def generate() -> None:
    step_id = 1

    for node_idx, (m_start, m_end, total) in enumerate(NODE_SPECS, start=1):
        machines = list(range(m_start, m_end + 1))
        n_machines = len(machines)

        base      = total // n_machines
        remainder = total % n_machines

        records: list[dict] = []
        for i, machine_id in enumerate(machines):
            n_steps = base + (1 if i < remainder else 0)
            for _ in range(n_steps):
                records.append({
                    "stepID":    step_id,
                    "machineID": machine_id,
                    "status":    "pending",
                })
                step_id += 1

        assert len(records) == total, \
            f"node{node_idx}: expected {total}, got {len(records)}"

        out_path = OUTPUT_DIR / f"node{node_idx}_data.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(
            f"node{node_idx}_data.json  "
            f"machineID {m_start:3d}–{m_end:3d}  "
            f"{len(records):5d} records  "
            f"stepID {records[0]['stepID']}–{records[-1]['stepID']}"
        )

    last_step = step_id - 1
    total_records = sum(spec[2] for spec in NODE_SPECS)
    print(f"\nTổng: {total_records} records  stepID 1–{last_step}")
    assert last_step == total_records == 10_000, "Tổng stepID không đúng 10 000!"
    print("OK – data đã sẵn sàng.")


if __name__ == "__main__":
    generate()
