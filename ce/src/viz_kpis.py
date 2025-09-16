#!/usr/bin/env python3
import os
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from typing import Sequence, Optional
from pandas_gbq import read_gbq
import matplotlib.pyplot as plt


def load_table(project: str, fq_table: str) -> pd.DataFrame:
    """Load a fully-qualified BigQuery table into a DataFrame."""
    df = read_gbq(f"SELECT * FROM `{fq_table}`", project_id=project)
    return df


def _add_grid(ax) -> None:
    ax.grid(True, which="major", linestyle="--", linewidth=0.6, alpha=0.5)
    ax.grid(True, which="minor", linestyle=":", linewidth=0.4, alpha=0.35)
    ax.minorticks_on()


def plot_daily_revenue(
    df: pd.DataFrame,
    outpath: Path,
    annotate_style: Optional[dict] = None,
) -> None:
    """
    Line chart: daily baseline vs policy revenue with gridlines and annotations.
    Assumes columns: date, baseline_rev, policy_rev
    - Highlights the max positive and max negative (if any) gaps between policy and baseline.
    """
    df = df.copy()
    # Normalize/cleanup
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Compute gaps (policy - baseline)
    if {"policy_rev", "baseline_rev"}.issubset(df.columns):
        df["gap"] = df["policy_rev"] - df["baseline_rev"]
    else:
        raise ValueError("Expected columns: 'policy_rev' and 'baseline_rev'.")

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    l1, = ax.plot(df["date"], df["baseline_rev"], label="Baseline revenue", linewidth=2.0)
    l2, = ax.plot(df["date"], df["policy_rev"], label="Policy revenue", linewidth=2.0)

    _add_grid(ax)
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue")
    ax.set_title("XGB Daily Revenue: Baseline vs Policy")

    # Prepare annotation style
    ann_style = dict(
        arrowprops=dict(arrowstyle="->", linewidth=1.2, alpha=0.9),
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.8),
        ha="left",
    )
    if annotate_style:
        # Shallow-merge
        ann_style.update({k: v for k, v in annotate_style.items() if k != "arrowprops"})
        if annotate_style.get("arrowprops"):
            ann_style["arrowprops"].update(annotate_style["arrowprops"])

    # Peak positive gap (policy above baseline the most)
    if not df["gap"].isna().all():
        pos_idx = int(df["gap"].idxmax())
        neg_idx = int(df["gap"].idxmin())

        # Annotate max positive gap
        x_pos = df.loc[pos_idx, "date"]
        y_pol_pos = df.loc[pos_idx, "policy_rev"]
        y_base_pos = df.loc[pos_idx, "baseline_rev"]
        gap_pos = df.loc[pos_idx, "gap"]

        ax.annotate(
            f"Peak +Δ: {gap_pos:,.0f}",
            xy=(x_pos, (y_pol_pos + y_base_pos) / 2),
            xytext=(x_pos, max(y_pol_pos, y_base_pos) * 1.02),
            **ann_style,
        )

        # If there is a meaningful negative gap, annotate it too
        if neg_idx != pos_idx and df.loc[neg_idx, "gap"] < 0:
            x_neg = df.loc[neg_idx, "date"]
            y_pol_neg = df.loc[neg_idx, "policy_rev"]
            y_base_neg = df.loc[neg_idx, "baseline_rev"]
            gap_neg = df.loc[neg_idx, "gap"]

            ax.annotate(
                f"Peak −Δ: {gap_neg:,.0f}",
                xy=(x_neg, (y_pol_neg + y_base_neg) / 2),
                xytext=(x_neg, min(y_pol_neg, y_base_neg) * 0.98),
                **ann_style,
            )

        # Optional: draw a translucent band showing the gap at each point
        ax.fill_between(
            df["date"].values,
            df["baseline_rev"].values,
            df["policy_rev"].values,
            where=(df["policy_rev"] >= df["baseline_rev"]),
            alpha=0.08,
            interpolate=True,
            label="Policy ≥ Baseline",
        )
        ax.fill_between(
            df["date"].values,
            df["baseline_rev"].values,
            df["policy_rev"].values,
            where=(df["policy_rev"] < df["baseline_rev"]),
            alpha=0.05,
            interpolate=True,
            label="Policy < Baseline",
        )

    ax.legend()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_uplift_by_expiry(
    df: pd.DataFrame,
    outpath: Path,
    soft_palette: Optional[Sequence[str]] = None,
    show_value_labels: bool = True,
) -> None:
    """
    Bar chart: uplift % by expiry bucket with gridlines, soft tones, and annotation for max.
    Assumes columns: expiry_bucket, uplift_pct
    """
    df = df.copy()

    # Fix bucket order if present
    default_order = ["0-1d", "2-3d", "4-5d", "6d+"]
    if set(default_order).issubset(set(df["expiry_bucket"].unique())):
        df["expiry_bucket"] = pd.Categorical(
            df["expiry_bucket"], categories=default_order, ordered=True
        )
        df = df.sort_values("expiry_bucket")

    # Convert to percentage points
    df["uplift_pct_pp"] = df["uplift_pct"] * 100.0

    # Soft pastel palette (color-blind considerate tones)
    if soft_palette is None:
        soft_palette = [
            "#A3C4F3",  # soft blue
            "#B9E3C6",  # mint
            "#FDE2E4",  # blush
            "#FFF1B6",  # pale yellow
            "#D7E3FC",  # light periwinkle (fallback if >4 buckets)
            "#E2F0CB",  # pale green
        ]

    # Map colors per bucket in order of appearance
    buckets = df["expiry_bucket"].astype(str).tolist()
    colors = {b: soft_palette[i % len(soft_palette)] for i, b in enumerate(buckets)}

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(df["expiry_bucket"].astype(str), df["uplift_pct_pp"], color=[colors[b] for b in buckets])

    _add_grid(ax)
    ax.set_xlabel("Expiry bucket")
    ax.set_ylabel("Revenue uplift (%)")
    ax.set_title("XGB Policy Uplift by Time-to-Expiry")

    # Highlight the max uplift bar with an annotation/arrow
    max_idx = int(df["uplift_pct_pp"].idxmax())
    max_bucket = str(df.loc[max_idx, "expiry_bucket"])
    max_value = float(df.loc[max_idx, "uplift_pct_pp"])
    max_bar = bars[buckets.index(max_bucket)]
    ax.annotate(
        f"Peak uplift: {max_value:.1f}%",
        xy=(max_bar.get_x() + max_bar.get_width() / 2, max_bar.get_height()),
        xytext=(0, 20),
        textcoords="offset points",
        ha="center",
        bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.85),
        arrowprops=dict(arrowstyle="->", linewidth=1.2, alpha=0.9),
    )

    # Optional value labels above bars
    if show_value_labels:
        for b in bars:
            ax.text(
                b.get_x() + b.get_width() / 2,
                b.get_height(),
                f"{b.get_height():.1f}%",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("PROJECT"), help="GCP project ID")
    ap.add_argument("--dataset", default=os.getenv("ML_DATASET", "dynamic_pricing_ml"),
                    help="BigQuery dataset containing KPI tables")
    ap.add_argument("--outdir", default="reports/viz", help="Directory to save charts")
    args = ap.parse_args()

    assert args.project, "Missing --project (or set PROJECT env var)"

    # Fully-qualified table names
    t_daily = f"{args.project}.{args.dataset}.lgb_policy_kpis_by_date"
    t_exp   = f"{args.project}.{args.dataset}.lgb_policy_kpis_by_expiry"

    # Load
    df_daily = load_table(args.project, t_daily)      # expects: date, baseline_rev, policy_rev, uplift_pct
    df_exp   = load_table(args.project, t_exp)        # expects: expiry_bucket, baseline_rev, policy_rev, uplift_pct

    # Plot
    outdir = Path(args.outdir)
    plot_daily_revenue(df_daily, outdir / "lgb_daily_revenue_baseline_vs_policy.png")
    plot_uplift_by_expiry(df_exp, outdir / "lgb_uplift_by_expiry_bucket.png")

    print(f"Saved charts to: {outdir.resolve()}")


if __name__ == "__main__":
    main()
