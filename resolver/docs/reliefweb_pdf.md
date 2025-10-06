# ReliefWeb PDF ingestion

This document explains how the ReliefWeb PDF extension behaves inside the
Resolver ingestion pipeline.  The connector augments the classic
`reliefweb_client.py` API workflow by downloading report attachments, extracting
figures, and writing them to `staging/reliefweb_pdf.csv` in the canonical facts
schema.

## Overview

- The connector uses the ReliefWeb API to discover resources that contain
  `application/pdf` attachments.
- A heuristic chooses the most relevant PDF per report.  Preferences are defined
  in the configuration via `preferred_pdf_titles` (e.g. “Situation Report”,
  “Flash Update”, “Key Figures”).
- PDFs are downloaded once and cached under `staging/.cache/reliefweb/pdf/`.
- Text extraction first attempts native text and falls back to OCR when the
  native layer is sparse.  OCR can be disabled in configuration or during tests.
- Parsed metrics include `people_in_need`, `people_affected`, and `idps`.  When
  only household counts are provided, the connector converts them to people
  using the reference `avg_household_size.csv` file.
- Output rows are emitted per country with `tier=2`, deterministic `event_id`
  hashes, and `series_semantics="new"` monthly deltas.  The level value is
  stored in `value_level` while `value` represents the new monthly value.

## Configuration

The ReliefWeb configuration (`ingestion/config/reliefweb.yml`) exposes the
following keys:

| Key | Description | Default |
| --- | --- | --- |
| `enable_pdfs` | Toggle PDF parsing | `true` |
| `enable_ocr` | Allow OCR fallback | `true` |
| `min_text_chars_before_ocr` | Native text length threshold before OCR | `1500` |
| `preferred_pdf_titles` | Ordered list of substrings used by the selection heuristic | see config |
| `since_months` | Look-back window for reports when PDF ingestion is run standalone | `6` |

To disable OCR entirely, set `enable_ocr: false`.  Tests can also monkeypatch
`resolver.ingestion._pdf_text.PDF_TEXT_TEST_MODE` to avoid heavyweight OCR.

## People-per-household (PPH)

Household → people conversions use `reference/avg_household_size.csv` merged with
overrides from `reference/overrides/avg_household_size_overrides.yml`.  The
lookup returns the best match for an ISO3 code and falls back to a global
default of `4.5` people per household.  The chosen value is surfaced via the
`method_details` column (`PPH=<value>, source=<source>, year=<year>`).

To override PPH values for a project, edit the overrides YAML file, e.g.:

```yaml
overrides:
  USA:
    people_per_household: 2.60
    source: "Custom Study"
    year: 2024
    notes: "Hurricane-specific household composition"
```

## Date logic and deltas

The connector extracts an `as_of_date` from the PDF by prioritising:

1. Coverage or reporting period end dates (`Reporting period: 01–31 Aug 2025`).
2. A `Report date:` field in the document body.
3. ReliefWeb metadata timestamps.

Monthly deltas follow Resolver’s standard rule: compare the current level value
with the previous level for the same `(iso3, hazard_code, metric, source_family`
=`reliefweb_pdf`) lineage.  Level history is cached under
`staging/.cache/reliefweb/levels/levels.json`.

## Output schema

`staging/reliefweb_pdf.csv` uses the following columns:

```
event_id,country_name,iso3,hazard_code,hazard_label,hazard_class,
metric,series_semantics,value,unit,value_level,as_of_date,month_start,
publication_date,publisher,source_type,source_url,resource_url,doc_title,
definition_text,method,method_value,method_details,extraction_layer,
matched_phrase,tier,confidence,revision,ingested_at
```

- `value` is the monthly new figure.
- `value_level` stores the level extracted from the PDF.
- `method_value` captures whether the figure was reported directly or derived
  from households.
- `method_details` records PPH and extraction hints.
- `extraction_layer` identifies the winning parsing layer (`table`,
  `infographic`, `narrative`).

Each PDF run updates the manifest via `reliefweb_pdf.csv.meta.json` and logs the
row count in the CLI output.

## Troubleshooting

- **OCR performance:** when OCR is unexpectedly triggered, increase
  `min_text_chars_before_ocr` or disable OCR.
- **Ambiguous multi-country totals:** the parser skips values that cannot be
  allocated to a single country.  Check logs for `ambiguous_allocation` notes.
- **Missing PPH:** add ISO3 values to `avg_household_size.csv` or the overrides
  file.  Without an entry the global default (4.5) is used.
- **Caching:** remove `staging/.cache/reliefweb/pdf/` to force re-download of
  PDFs and `staging/.cache/reliefweb/levels/` to reset monthly deltas.
