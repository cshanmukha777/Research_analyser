"""Non-networked research registry. This package never authenticates to BRAIN."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[2]
BASELINE_PATH = ROOT / "config" / "baseline.json"

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS settings_profiles (
 profile_id TEXT PRIMARY KEY, settings_json TEXT NOT NULL, source TEXT NOT NULL,
 verified_at TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS field_inventory (
 scope_id TEXT NOT NULL, field_id TEXT NOT NULL, dataset_id TEXT, category TEXT,
 field_type TEXT, unit TEXT, cadence TEXT, coverage REAL,
 availability TEXT NOT NULL DEFAULT 'UNVERIFIED'
   CHECK (availability IN ('UNVERIFIED','AVAILABLE','UNAVAILABLE')),
 source_ref TEXT, verified_at TEXT, notes TEXT,
 PRIMARY KEY (scope_id, field_id)
);
CREATE TABLE IF NOT EXISTS operator_inventory (
 scope_id TEXT NOT NULL, operator TEXT NOT NULL, category TEXT, signature TEXT,
 source_ref TEXT, verified_at TEXT, notes TEXT,
 PRIMARY KEY (scope_id, operator)
);
CREATE TABLE IF NOT EXISTS hypotheses (
 hypothesis_id TEXT PRIMARY KEY, title TEXT NOT NULL, family TEXT NOT NULL,
 mechanism TEXT NOT NULL, expected_sign TEXT, horizon TEXT, kill_criteria TEXT,
 source_ref TEXT, status TEXT NOT NULL DEFAULT 'DRAFT', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS candidates (
 candidate_id TEXT PRIMARY KEY, hypothesis_id TEXT REFERENCES hypotheses(hypothesis_id),
 family TEXT NOT NULL, expression TEXT NOT NULL, normalized_expression TEXT NOT NULL,
 expression_hash TEXT NOT NULL,
 settings_profile_id TEXT NOT NULL REFERENCES settings_profiles(profile_id),
 settings_hash TEXT NOT NULL, fields_json TEXT NOT NULL, operators_json TEXT NOT NULL,
 verification_status TEXT NOT NULL DEFAULT 'UNVERIFIED'
   CHECK (verification_status IN ('UNVERIFIED','STATIC_REVIEWED','PLATFORM_VALIDATED')),
 status TEXT NOT NULL DEFAULT 'DRAFT_UNVERIFIED'
   CHECK (status IN ('DRAFT_UNVERIFIED','STATIC_REVIEWED','QUEUED_MANUAL',
                     'SIMULATED','FAILED','PASSED','ARCHIVED')),
 variant_of TEXT REFERENCES candidates(candidate_id), notes TEXT, created_at TEXT NOT NULL,
 UNIQUE (expression_hash, settings_hash)
);
CREATE TABLE IF NOT EXISTS simulation_queue (
 queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
 candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
 priority INTEGER NOT NULL DEFAULT 100, reason TEXT NOT NULL,
 run_policy TEXT NOT NULL DEFAULT 'manual-only' CHECK (run_policy = 'manual-only'),
 status TEXT NOT NULL DEFAULT 'READY_FOR_MANUAL_REVIEW'
   CHECK (status IN ('READY_FOR_MANUAL_REVIEW','RUN_MANUALLY','DONE','CANCELLED')),
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS simulations (
 simulation_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
 settings_json TEXT NOT NULL, result_source TEXT NOT NULL CHECK (result_source IN ('BRAIN_UI','BRAIN_API')),
 outcome TEXT NOT NULL CHECK (outcome IN ('PASS','NEAR_PASS','FAIL','UNCLASSIFIED')),
 sharpe REAL, fitness REAL, turnover REAL, returns REAL, drawdown REAL,
 sub_universe_sharpe REAL, margin REAL, checks_json TEXT, warnings_json TEXT,
 evidence_ref TEXT, recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS correlations (
 correlation_id INTEGER PRIMARY KEY AUTOINCREMENT,
 candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
 comparison_kind TEXT NOT NULL CHECK (comparison_kind IN ('PAIRWISE','SELF','PRODUCTION','POWER_POOL')),
 compared_to TEXT, value REAL,
 status TEXT NOT NULL CHECK (status IN ('MEASURED','PENDING','UNAVAILABLE','NOT_APPLICABLE')),
 observations INTEGER, window_start TEXT, window_end TEXT, source_ref TEXT,
 measured_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS experiment_log (
 experiment_id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id TEXT REFERENCES candidates(candidate_id),
 action TEXT NOT NULL, result TEXT NOT NULL, learning TEXT, next_step TEXT,
 source_ref TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS submissions (
 submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
 candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id), brain_alpha_id TEXT,
 status TEXT NOT NULL CHECK (status IN ('NOT_SUBMITTED','SUBMITTED','REJECTED','UNKNOWN')),
 submitted_at TEXT, evidence_ref TEXT, notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_candidates_family_status ON candidates(family, status);
CREATE INDEX IF NOT EXISTS idx_candidates_expression_hash ON candidates(expression_hash);
CREATE INDEX IF NOT EXISTS idx_simulations_candidate ON simulations(candidate_id);
CREATE INDEX IF NOT EXISTS idx_correlations_candidate_kind ON correlations(candidate_id, comparison_kind);
CREATE VIEW IF NOT EXISTS failed_candidates AS SELECT * FROM candidates WHERE status = 'FAILED';
CREATE VIEW IF NOT EXISTS successful_candidates AS SELECT * FROM candidates WHERE status = 'PASSED';
"""

TOKEN_RE = re.compile(
    r"""'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|"""
    r"""(?:\d+(?:\.\d*)?|\.\d+)|[A-Za-z_]\w*|<=|>=|==|!=|&&|\|\||[^\s]"""
)
CALL_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def database(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_baseline() -> dict[str, Any]:
    if not BASELINE_PATH.exists():
        raise ValueError(
            "missing local config/baseline.json; copy config/baseline.example.json "
            "and enter the requested settings. This local file is git-ignored."
        )
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def initialize(db_path: str | Path) -> None:
    existed = Path(db_path).expanduser().exists()
    profile = load_baseline()
    with database(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            """INSERT OR IGNORE INTO settings_profiles
               (profile_id, settings_json, source, created_at) VALUES (?, ?, ?, ?)""",
            (profile["profile_id"], json.dumps(profile, sort_keys=True),
             profile.get("source", "local configuration"), now()),
        )
    print(f"{'Opened' if existed else 'Created'} research database: {Path(db_path)}")


def initialize_command(args: argparse.Namespace) -> None:
    initialize(args.db)


def normalize_expression(expression: str) -> str:
    tokens = TOKEN_RE.findall(expression)
    if not tokens:
        raise ValueError("expression is empty")
    return "\x1f".join(tokens)


def add_hypothesis(args: argparse.Namespace) -> None:
    with database(args.db) as conn:
        conn.execute(
            """INSERT INTO hypotheses
               (hypothesis_id, title, family, mechanism, expected_sign, horizon,
                kill_criteria, source_ref, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (args.id, args.title, args.family, args.mechanism, args.sign, args.horizon,
             args.kill_criteria, args.source_ref, now()),
        )
    print(f"Recorded hypothesis {args.id}")


def add_candidate(args: argparse.Namespace) -> None:
    expression = args.expression.strip()
    normalized = normalize_expression(expression)
    settings = load_baseline()
    settings_json = json.dumps(settings, sort_keys=True)
    settings_hash = hashlib.sha256(settings_json.encode()).hexdigest()
    expression_hash = hashlib.sha256(normalized.encode()).hexdigest()
    fields = sorted({item.strip() for item in args.fields.split(",") if item.strip()})
    operators = sorted(set(CALL_RE.findall(expression)))
    with database(args.db) as conn:
        conn.execute(
            """INSERT INTO candidates
               (candidate_id, hypothesis_id, family, expression, normalized_expression,
                expression_hash, settings_profile_id, settings_hash, fields_json,
                operators_json, notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (args.id, args.hypothesis_id, args.family, expression, normalized,
             expression_hash, settings["profile_id"], settings_hash,
             json.dumps(fields), json.dumps(operators), args.notes, now()),
        )
    print(f"Recorded unverified candidate {args.id}")
    print(f"Detected {len(fields)} declared fields and {len(operators)} operator calls.")
    print("These counts are local hints; confirm them against BRAIN and account rules.")


def import_inventory(args: argparse.Namespace) -> None:
    required = ("scope_id", "field_id", "availability", "source_ref", "verified_at") \
        if args.kind == "fields" else ("scope_id", "operator", "source_ref", "verified_at")
    with open(args.file, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("inventory file has no data rows")
    for index, row in enumerate(rows, start=2):
        missing = [key for key in required if not row.get(key)]
        if missing:
            raise ValueError(f"row {index} is missing required columns/values: {missing}")
    with database(args.db) as conn:
        for row in rows:
            if args.kind == "fields":
                if row["availability"] not in {"AVAILABLE", "UNAVAILABLE", "UNVERIFIED"}:
                    raise ValueError("field availability must be AVAILABLE, UNAVAILABLE, or UNVERIFIED")
                conn.execute(
                    """INSERT INTO field_inventory
                       (scope_id, field_id, dataset_id, category, field_type, unit, cadence,
                        coverage, availability, source_ref, verified_at, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(scope_id, field_id) DO UPDATE SET
                        dataset_id=excluded.dataset_id, category=excluded.category,
                        field_type=excluded.field_type, unit=excluded.unit, cadence=excluded.cadence,
                        coverage=excluded.coverage, availability=excluded.availability,
                        source_ref=excluded.source_ref, verified_at=excluded.verified_at,
                        notes=excluded.notes""",
                    (row["scope_id"], row["field_id"], row.get("dataset_id"), row.get("category"),
                     row.get("field_type"), row.get("unit"), row.get("cadence"),
                     float(row["coverage"]) if row.get("coverage") else None,
                     row["availability"], row["source_ref"], row["verified_at"], row.get("notes")),
                )
            else:
                conn.execute(
                    """INSERT INTO operator_inventory
                       (scope_id, operator, category, signature, source_ref, verified_at, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(scope_id, operator) DO UPDATE SET
                        category=excluded.category, signature=excluded.signature,
                        source_ref=excluded.source_ref, verified_at=excluded.verified_at,
                        notes=excluded.notes""",
                    (row["scope_id"], row["operator"], row.get("category"), row.get("signature"),
                     row["source_ref"], row["verified_at"], row.get("notes")),
                )
    print(f"Imported {len(rows)} {args.kind} inventory rows from {args.file}")


def queue_candidate(args: argparse.Namespace) -> None:
    with database(args.db) as conn:
        row = conn.execute(
            "SELECT * FROM candidates WHERE candidate_id = ?", (args.candidate_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown candidate: {args.candidate_id}")
        settings_row = conn.execute(
            "SELECT settings_json FROM settings_profiles WHERE profile_id = ?",
            (row["settings_profile_id"],),
        ).fetchone()
        settings = json.loads(settings_row["settings_json"])
        scope = (f"{settings['instrument_type']}/{settings['region']}/"
                 f"D{settings['delay']}/{settings['universe']}")
        missing_fields = []
        for field in json.loads(row["fields_json"]):
            found = conn.execute(
                """SELECT 1 FROM field_inventory
                   WHERE scope_id = ? AND field_id = ? AND availability = 'AVAILABLE'""",
                (scope, field),
            ).fetchone()
            if found is None:
                missing_fields.append(field)
        missing_ops = []
        for operator in json.loads(row["operators_json"]):
            found = conn.execute(
                """SELECT 1 FROM operator_inventory WHERE scope_id = ? AND operator = ?""",
                (scope, operator),
            ).fetchone()
            if found is None:
                missing_ops.append(operator)
        if missing_fields or missing_ops:
            raise ValueError(
                f"not queued for {scope}: inventory evidence missing; "
                f"fields={missing_fields}; operators={missing_ops}"
            )
        conn.execute(
            """INSERT INTO simulation_queue
               (candidate_id, priority, reason, run_policy, created_at)
               VALUES (?, ?, ?, 'manual-only', ?)""",
            (args.candidate_id, args.priority, args.reason, now()),
        )
        conn.execute(
            "UPDATE candidates SET status = 'QUEUED_MANUAL' WHERE candidate_id = ?",
            (args.candidate_id,),
        )
    print(f"Queued {args.candidate_id} for manual BRAIN simulation only.")


def record_result(args: argparse.Namespace) -> None:
    checks = json.loads(args.checks_json) if args.checks_json else None
    warnings = json.loads(args.warnings_json) if args.warnings_json else None
    with database(args.db) as conn:
        candidate = conn.execute(
            "SELECT settings_profile_id FROM candidates WHERE candidate_id = ?",
            (args.candidate_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError(f"unknown candidate: {args.candidate_id}")
        profile = conn.execute(
            "SELECT settings_json FROM settings_profiles WHERE profile_id = ?",
            (candidate["settings_profile_id"],),
        ).fetchone()
        conn.execute(
            """INSERT INTO simulations
               (simulation_id, candidate_id, settings_json, result_source, outcome,
                sharpe, fitness, turnover, returns, drawdown, sub_universe_sharpe,
                margin, checks_json, warnings_json, evidence_ref, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (args.simulation_id, args.candidate_id, profile["settings_json"], args.source,
             args.outcome, args.sharpe, args.fitness, args.turnover, args.returns,
             args.drawdown, args.sub_universe_sharpe, args.margin,
             json.dumps(checks) if checks is not None else None,
             json.dumps(warnings) if warnings is not None else None,
             args.evidence_ref, now()),
        )
        status = {"PASS": "PASSED", "FAIL": "FAILED"}.get(args.outcome, "SIMULATED")
        conn.execute("UPDATE candidates SET status = ? WHERE candidate_id = ?",
                     (status, args.candidate_id))
        conn.execute(
            """INSERT INTO experiment_log
               (candidate_id, action, result, learning, next_step, source_ref, created_at)
               VALUES (?, 'record_simulation', ?, ?, ?, ?, ?)""",
            (args.candidate_id, args.outcome, args.learning, args.next_step,
             args.evidence_ref, now()),
        )
    print(f"Recorded supplied BRAIN result {args.simulation_id}; confirm its evidence reference.")


def summary(args: argparse.Namespace) -> None:
    with database(args.db) as conn:
        for table in ("field_inventory", "operator_inventory", "hypotheses", "candidates",
                      "simulation_queue", "simulations", "correlations", "submissions"):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count}")
    print("Queue execution policy: manual-only")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="brain-research")
    commands = root.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="create or open the local research database")
    init.add_argument("--db", default="data/research.sqlite")
    init.set_defaults(func=initialize_command)

    show = commands.add_parser("summary", help="show local research ledger counts")
    show.add_argument("--db", default="data/research.sqlite")
    show.set_defaults(func=summary)

    inventory = commands.add_parser("import-inventory")
    inventory.add_argument("--db", default="data/research.sqlite")
    inventory.add_argument("--kind", choices=("fields", "operators"), required=True)
    inventory.add_argument("--file", required=True)
    inventory.set_defaults(func=import_inventory)

    hypothesis = commands.add_parser("add-hypothesis")
    hypothesis.add_argument("--db", default="data/research.sqlite")
    hypothesis.add_argument("--id", required=True)
    hypothesis.add_argument("--title", required=True)
    hypothesis.add_argument("--family", required=True)
    hypothesis.add_argument("--mechanism", required=True)
    hypothesis.add_argument("--sign")
    hypothesis.add_argument("--horizon")
    hypothesis.add_argument("--kill-criteria")
    hypothesis.add_argument("--source-ref")
    hypothesis.set_defaults(func=add_hypothesis)

    candidate = commands.add_parser("add-candidate")
    candidate.add_argument("--db", default="data/research.sqlite")
    candidate.add_argument("--id", required=True)
    candidate.add_argument("--hypothesis-id")
    candidate.add_argument("--family", required=True)
    candidate.add_argument("--expression", required=True)
    candidate.add_argument("--fields", default="")
    candidate.add_argument("--notes", default="")
    candidate.set_defaults(func=add_candidate)

    queue = commands.add_parser("queue")
    queue.add_argument("--db", default="data/research.sqlite")
    queue.add_argument("--candidate-id", required=True)
    queue.add_argument("--priority", type=int, default=100)
    queue.add_argument("--reason", required=True)
    queue.set_defaults(func=queue_candidate)

    result = commands.add_parser("record-result")
    result.add_argument("--db", default="data/research.sqlite")
    result.add_argument("--candidate-id", required=True)
    result.add_argument("--simulation-id", required=True)
    result.add_argument("--source", choices=("BRAIN_UI", "BRAIN_API"), required=True)
    result.add_argument("--outcome", choices=("PASS", "NEAR_PASS", "FAIL", "UNCLASSIFIED"),
                        required=True)
    for name in ("sharpe", "fitness", "turnover", "returns", "drawdown",
                 "sub-universe-sharpe", "margin"):
        result.add_argument("--" + name, dest=name.replace("-", "_"), type=float)
    result.add_argument("--checks-json")
    result.add_argument("--warnings-json")
    result.add_argument("--evidence-ref")
    result.add_argument("--learning")
    result.add_argument("--next-step")
    result.set_defaults(func=record_result)
    return root


def main() -> None:
    args = parser().parse_args()
    try:
        args.func(args)
    except (sqlite3.IntegrityError, sqlite3.OperationalError, ValueError,
            json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
