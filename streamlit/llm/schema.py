# rag/schema.py

def get_columns_by_role(schema: dict, role: str) -> list[str]:
    return [col for col, spec in schema["columns"].items() if spec["role"] == role]


def build_embedding_text(row, schema: dict) -> str:
    parts = []
    for col in get_columns_by_role(schema, "embedding"):
        val = row.get(col)
        if val:
            parts.append(str(val))
    return "\n\n".join(parts)


def build_payload(row, schema: dict) -> dict:
    payload = {}
    for col in get_columns_by_role(schema, "payload"):
        if col in row and row[col] is not None:
            payload[col] = row[col]
    return payload
