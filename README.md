# README (updated)

## What is Spagbot?

Spagbot is an AI forecasting assistant. Its job is to read forecasting questions (like those on Metaculus), gather relevant evidence, and combine the judgments of different models into one final forecast. It is designed to run automatically on GitHub, saving its forecasts and learning from past performance over time.

## How it works (conceptually)

### Understand the question

Spagbot reads a forecasting question (binary "yes/no," numeric, or multiple-choice).

It builds a tailored prompt to guide models toward giving a probability or range.

### Gather research

It can query external news/search APIs (AskNews, Serper) to pull in up-to-date information.

This gives context to the models so they are not just reasoning in a vacuum.

### Ask multiple models

Several large language models (like GPT-4o, Claude, Gemini, Grok) are asked the same question.

Each model returns its best estimate (e.g., "There’s a 35% chance…").

For strategic, political questions, Spagbot can also run a game-theoretic simulation (GTMC1) that models actors, their interests, and possible bargaining outcomes.

### Combine forecasts

All these signals are combined using a Bayesian Monte Carlo method.

This ensures the final probability or distribution is mathematically consistent and reflects the strength of evidence from each source.

### Calibrate and improve

Spagbot logs every forecast in a CSV (configurable via `FORECASTS_CSV_PATH`; default is `forecast_logs/forecasts.csv`).

When questions resolve, it compares its predictions to reality.

A calibration script then adjusts the weights of different models, giving more influence to those that proved more accurate in similar situations.

This creates a closed learning loop so Spagbot improves over time.

### Autonomous operation

In GitHub, Spagbot runs on a schedule (for test questions, the tournament, and the Metaculus Cup).

It automatically commits its logs and calibration updates back to the repository, so no human intervention is needed.

### The big picture

Think of Spagbot as a forecasting orchestra conductor:

- The models are the musicians, each with their own instrument (style of reasoning).
- GTMC1 is the percussion section for power politics.
- Bayesian Monte Carlo is the conductor, blending them into harmony.
- And the calibration loop is like rehearsals, helping everyone play in tune with reality over time.

## Quick Start

### Local run (one question / test mode)

(Recommended) use Python 3.11 and Poetry

```bash
poetry install
```

### Environment variables (see "Configuration" below)

The only must-have for submission is `METACULUS_TOKEN`

```bash
export METACULUS_TOKEN=your_token_here
```

### Run a single test question (no submit)

```bash
poetry run python run_spagbot.py --mode test_questions --limit 1
```

### Run a single question by post ID (no submit)

```bash
poetry run python run_spagbot.py --pid 22427
```

### Submit (be careful!)

```bash
poetry run python run_spagbot.py --mode test_questions --limit 1 --submit
```

## Autonomous runs on GitHub

This repo includes workflows for:

- `test_bot_fresh.yaml` (fresh research),
- `test_bot_cached.yaml` (use cache),
- `run_bot_on_tournament.yaml`,
- `run_bot_on_metaculus_cup.yaml`,
- `calibration_refresh.yml` (periodic update of calibration weights).

Add secrets (see below), push the repo, and Actions will:

- run forecasts,
- write logs to the repo,
- update calibration weights on a schedule,
- commit/push changes automatically.

### Resolver artifact hygiene

Nightly resolver runs can generate sizeable exports. The CI workflow calls `run_bot_on_metaculus_cup.yaml`.

Include `--submit` in those workflows if you want automatic submissions.

## Logging & Files Written

- Machine CSV: configurable (`FORECASTS_CSV_PATH`, default `forecast_logs/forecasts.csv`)
  - Contains per-question details, per-model parsed outputs, final aggregation, timestamps, etc.
- Human log: `forecast_logs/runs/<timestamp>_<run_id>.md`
  - Friendly narrative summary: which models ran, research snippets, final forecast.
  - These are committed automatically from GitHub Actions unless disabled.

## Calibration (Autonomous Loop)

- Input: the forecast CSV
- Logic:
  - Keep resolved questions only.
  - Keep only forecasts made before resolution time.
  - Compute per-model loss by question type and class (e.g., binary vs numeric; topical classes if present).
  - Build class-conditional weights with shrinkage toward global weights.
- Output:
  - `calibration_weights.json`
  - `data/calibration_advice.txt` (friendly notes)
- Use: At the start of a run, Spagbot loads `calibration_weights.json` to weight ensemble members.

**Important:** Ensure the `data/` folder exists in the repo so `calibration_advice.txt` can be written and committed.

## Game-Theoretic Module (GTMC1)

- Accepts an actor table with columns: `name`, `position`, `capability`, `salience`, `risk_threshold` (scales 0–100 for the first three).
- Runs a deterministic Monte Carlo bargaining process (PCG64 RNG seeded from a fingerprint of the table + params).
- Outputs:
  - `exceedance_ge_50` (preferred probability-like signal when higher axis = “YES”),
  - `median_of_final_medians`,
  - `coalition_rate`,
  - `dispersion`,
  - and logs a per-run CSV under `gtmc_logs/` (optional to commit).

You can enable/disable GTMC1 by question type or flags in `cli.py`.

## Aggregation Math (Plain English)

- **Binary:** Treat each model’s p(yes) (and GTMC1’s exceedance) as evidence with a confidence weight. Update a Beta prior; take the posterior mean as final probability (with quantile summaries P10/P50/P90).
- **MCQ:** Treat each model’s probability vector as evidence and update a Dirichlet prior; take the posterior mean vector.
- **Numeric:** Fit a Normal from each model’s P10/P50/P90, sample proportionally to confidence weights, and compute final P10/P50/P90 from the mixture.

Weights come from `calibration_weights.json`, updated by the calibration job.

## GitHub Actions (What’s Included)

- Fresh test runs vs cached test runs
- Tournament and Metaculus Cup runners
- Calibration refresh (scheduled)

Each workflow:

- Sets the environment (paths, commit behavior),
- Runs the bot (and/or `update_calibration.py`),
- Auto-commits any modified files under:
  - `forecast_logs/`,
  - `calibration_weights.json`,
  - `data/` (advice),
  - `state/` (duplicate-protection, if enabled),
  - (optionally) other state you decide to persist.

## Secrets You Must Add (GitHub → Actions)

- `METACULUS_TOKEN` (for submission)
- Any LLM / research API keys you plan to use:
  - `OPENROUTER_API_KEY`, `GOOGLE_API_KEY`, `XAI_API_KEY`, `ASKNEWS_CLIENT_ID`, `ASKNEWS_CLIENT_SECRET`, `SERPER_API_KEY`, etc.

## Troubleshooting

- “No calibration changes to commit.”
  - Normal if no new resolutions or no forecasts before the resolution time.
- Numeric submission errors (CDF monotonicity etc.).
  - The numeric path enforces ordered quantiles (P10 ≤ P50 ≤ P90). If Metaculus still complains, reduce extremely steep distributions (too tiny sigma) or clip the extreme tails. (The current code already guards the common pitfalls.)
- Logs not committing locally.
  - Set `COMMIT_LOGS=true` and ensure your git remote/branch are correct.
- Advice file write error.
  - Ensure `data/` folder exists and isn’t `.gitignore`’d.
- Duplicate forecasts.
  - Make sure `SEEN_GUARD_PATH` is set to a path inside the repo (e.g., `forecast_logs/state/seen_forecasts.jsonl`) and that your workflow imports `filter_post_ids`/`assert_not_seen`/`mark_post_seen` successfully (see the compat layer in `seen_guard.py` above).
- Metaculus API call hangs for minutes.

