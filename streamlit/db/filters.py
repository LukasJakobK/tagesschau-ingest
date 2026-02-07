import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "filter_fields.json"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = json.load(f)

FIELDS_CFG = _cfg["fields"]     # enthält operators + ui
FIELD_SQL = _cfg["field_sql"]

# Für UI
FIELD_NAMES = list(FIELDS_CFG.keys())

def get_operators(field: str) -> list[str]:
    return FIELDS_CFG[field]["operators"]

def get_ui_type(field: str) -> str:
    return FIELDS_CFG[field].get("ui", "text")

def build_where(filters: list[dict]) -> str:
    clauses = []

    for f in filters:
        field = f["field"]
        col = FIELD_SQL[field]
        op = f["operator"]
        val = str(f["value"]).replace("'", "''")

        if op == "LIKE":
            clauses.append(f"{col} LIKE '%{val}%'")

        elif op == "BETWEEN":
            parts = [p.strip() for p in val.split(",")]
            if len(parts) == 2:
                clauses.append(f"{col} BETWEEN '{parts[0]}' AND '{parts[1]}'")

        else:
            clauses.append(f"{col} {op} '{val}'")

    return " AND ".join(clauses) if clauses else "1=1"
