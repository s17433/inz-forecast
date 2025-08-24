from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
FINAL = DATA / "final"
REPORTS = ROOT / "reports"
PLOTS = REPORTS / "plots"

for d in [RAW, PROCESSED, FINAL, REPORTS, PLOTS]:
    d.mkdir(parents=True, exist_ok=True)
