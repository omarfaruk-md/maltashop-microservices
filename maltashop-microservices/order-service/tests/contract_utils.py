import json
from pathlib import Path

TYPES = {"string": str, "integer": int}


def load_contract(name):
    return json.loads((Path(__file__).resolve().parents[2] / "contracts" / name).read_text())


def violations(required, message, path=""):
    """Return a list of contract violations (missing fields or wrong types)."""
    problems = []
    for key, expected in required.items():
        if key not in message:
            problems.append(f"missing {path}{key}")
        elif isinstance(expected, dict):
            problems += violations(expected, message[key], f"{path}{key}.")
        elif not isinstance(message[key], TYPES[expected]):
            problems.append(f"{path}{key} should be {expected}")
    return problems
