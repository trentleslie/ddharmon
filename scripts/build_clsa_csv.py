"""Build CLSA flat CSV from the multi-sheet baseline Excel file.

CLSA ships as `clsa_baseline.xlsx` with two sheets:
  - Variables  — one row per field (name, label:en, question:en, comment:en, ...)
  - Categories — one row per (variable, value_code) pair with display label

This script joins the two sheets on (table, variable) and produces a flat CSV
with `code=label|code=label|...` style value encodings appended as a new
`value_encoding` column. Output matches the shape ddharmon's `load_dictionary`
expects (compare UKBB's `response_options` column).

Filtering:
  - Missing-data sentinels (`missing == 1`: Don't know / Refused / Missing
    codes) are excluded from value encodings so they don't pollute embeddings.
  - Categories rows with NaN labels are dropped.

Usage:
    python scripts/build_clsa_csv.py [--xlsx PATH] [--out PATH]

Idempotent — overwrites the output CSV.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_XLSX = Path("data/final combined responses/clsa_baseline.xlsx")
DEFAULT_OUT = Path("data/final combined responses/clsa_baseline.csv")


def build_clsa_csv(xlsx_path: Path, out_path: Path) -> None:
    vars_df = pd.read_excel(xlsx_path, sheet_name="Variables")
    cat_df = pd.read_excel(xlsx_path, sheet_name="Categories")

    print(f"Variables: {len(vars_df)} rows, {len(vars_df.columns)} cols")
    print(f"Categories: {len(cat_df)} rows")

    real = cat_df[(cat_df["missing"] == 0) & cat_df["label:en"].notna()].copy()
    print(f"Real categories after dropping sentinels + NaN labels: {len(real)} rows")

    real["pair"] = (
        real["name"].astype(str).str.strip()
        + "="
        + real["label:en"].astype(str).str.strip()
    )
    agg = (
        real.groupby(["table", "variable"], sort=False)["pair"]
        .apply(lambda s: "|".join(s))
        .reset_index()
        .rename(columns={"variable": "name", "pair": "value_encoding"})
    )
    print(f"Aggregated to {len(agg)} (table, variable) groups")

    merged = vars_df.merge(agg, on=["table", "name"], how="left")
    n_with_enc = int(merged["value_encoding"].notna().sum())
    print(
        f"Merged shape: {merged.shape} — "
        f"{n_with_enc} variables with value_encoding "
        f"({n_with_enc / len(merged):.1%}), "
        f"{len(merged) - n_with_enc} without (numeric/open-text)"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    if not args.xlsx.exists():
        raise SystemExit(f"Source not found: {args.xlsx}")
    build_clsa_csv(args.xlsx, args.out)


if __name__ == "__main__":
    main()
