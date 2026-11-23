# WorldQuant BRAIN Research Analyser

A local-first research ledger for hypotheses, verified BRAIN fields and operators, candidate expressions, manual simulation queues, actual results, correlations, experiment notes, and submission tracking.

This project does not authenticate to BRAIN or submit alphas. Queue entries are manual-only. It contains no BRAIN credentials and starts with an empty field/operator inventory.

## Setup

Requires Python 3.11+ and SQLite from the standard library.

From the repository root:

    python -m pip install -e .
    cp config/baseline.example.json config/baseline.json
    # Edit the ignored local config/baseline.json with your chosen baseline.
    brain-research init
    brain-research summary

The default database is data/research.sqlite. Initialization is additive and non-destructive. Back up a database before any future schema change.

## Workflow

1. Record a hypothesis and its mechanism before writing an expression.
2. Add only account-verified fields and operators, with exact scope and source evidence.
3. Register the exact candidate expression and settings snapshot. New candidates are unverified.
4. Review exact and near-duplicates. Confirm account-specific availability before queueing.
5. Queue entries are manual-only. Run them in BRAIN yourself and record the returned simulation ID, settings, metrics, checks, and warnings.
6. Keep failed candidates and their diagnosis in the permanent registry.
7. Record pairwise, self, production, and PowerPool correlations as distinct measures.

See docs/operating_workflow.md. No formulas or performance claims are included as examples.

Historical runs should be imported from source exports with exact expressions and settings before candidate generation resumes.
