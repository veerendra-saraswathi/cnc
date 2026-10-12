"""Fail if a module defines a domain type outside the eleven ontology names."""

import ast
from pathlib import Path

from src.kernel.models import (
    Alert,
    Asset,
    Command,
    Detection,
    Event,
    Mission,
    OperatorAction,
    Position,
    Telemetry,
    Track,
    TrackHistoryPoint,
    Zone,
)
from src.kernel.ontology import KERNEL_OBJECTS

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "src" / "kernel" / "models.py"
SKIP_DIRS = {
    ".venv",
    "venv",
    "data",
    "__pycache__",
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "site-packages",
}
ALLOWLIST = {
    "TrackHistoryPoint",
    "SensorObservation",
    "CommandRequest",
    "CreateMissionRequest",
    "AssignAssetsRequest",
    "Session",
    "AuditEntry",
}
BY_NAME = {
    "Asset": Asset,
    "Position": Position,
    "Telemetry": Telemetry,
    "Command": Command,
    "Mission": Mission,
    "Track": Track,
    "Detection": Detection,
    "Alert": Alert,
    "Event": Event,
    "Zone": Zone,
    "OperatorAction": OperatorAction,
}


def _bases_include_model(node: ast.ClassDef) -> bool:
    for base in node.bases:
        if isinstance(base, ast.Name) and base.id == "BaseModel":
            return True
        if isinstance(base, ast.Attribute) and base.attr == "BaseModel":
            return True
    return False


def _module_classes(path: Path) -> list[ast.ClassDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [node for node in tree.body if isinstance(node, ast.ClassDef)]


def test_kernel_defines_each_ontology_name_once():
    classes = _module_classes(MODELS)
    basemodels = [node.name for node in classes if _bases_include_model(node)]
    expected = set(KERNEL_OBJECTS) | {"TrackHistoryPoint"}
    assert set(basemodels) == expected
    assert len(basemodels) == len(expected)
    assert len(KERNEL_OBJECTS) == 11
    for name in KERNEL_OBJECTS:
        assert BY_NAME[name].__name__ == name
    assert TrackHistoryPoint.__name__ == "TrackHistoryPoint"


def test_no_domain_type_outside_the_eleven_names():
    offenders: list[str] = []
    names = set(KERNEL_OBJECTS)
    for path in ROOT.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.resolve() == MODELS.resolve():
            continue
        try:
            classes = _module_classes(path)
        except SyntaxError as exc:
            offenders.append(f"{path}: {exc}")
            continue
        for node in classes:
            if node.name in names:
                offenders.append(f"{path}:{node.lineno} class {node.name}")
            elif _bases_include_model(node) and node.name not in ALLOWLIST:
                offenders.append(f"{path}:{node.lineno} BaseModel {node.name}")
    assert offenders == []
