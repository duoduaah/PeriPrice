#!/usr/bin/env python3
import os, argparse, json, sys
from typing import Dict, List
import numpy as np
import pandas as pd

from google.cloud import bigquery
import xgboost as xgb


FEATURES: List[str] = [
    "effective_price","discount_pct","time_to_expiry","base_price",
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales","promo_in_last_7d",
    "dow","month","year",
    "family","class","store_nbr","cluster"
]
CAT_COLS = ["family","class","store_nbr","cluster"]
FLOAT_COLS = [
    "effective_price","discount_pct","base_price",
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales"
]
INT_COLS = ["time_to_expiry","promo_in_last_7d","dow","month","year"]

def _cast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    X = df.copy()
    for c in FLOAT_COLS:
        if c in X:
            X[c] = X[c].astype(np.float32)
    if "time_to_expiry" in X: X["time_to_expiry"] = X["time_to_expiry"].astype(np.int16)
    if "promo_in_last_7d" in X: X["promo_in_last_7d"] = X["promo_in_last_7d"].astype(np.int8)
    if "dow" in X: X["dow"] = X["dow"].astype(np.int8)
    if "month" in X: X["month"] = X["month"].astype(np.int8)
    if "year" in X: X["year"] = X["year"].astype(np.int16)
    return X

def _apply_categories(df: pd.DataFrame, cat_vocab: Dict[str, List[str]]) -> pd.DataFrame:
    X = df.copy()
    for c in CAT_COLS:
        s = X[c].astype(str)
        vocab = cat_vocab[c]
        mask = ~s.isin(vocab)
        if mask.any():
            s.loc[mask] = "__UNK__"
        X[c] = pd.Categorical(s, categories=(vocab + ["__UNK__"]))
    return X

def load_scoring_frame(project: str, the_date: str) -> pd.DataFrame:
    """Load one day from scoring_frame_test (keeps memory reasonable)."""
    client = bigquery.Client(project=project)
    table = f"{project}.dynamic_pricing_ml.scoring_frame_test"
    sql = f"""
    SELECT
      date, store_nbr, item_nbr,
      base_price, time_to_expiry,
      lag1_log_sales, lag7_log_sales, lag14_log_sales, lag28_log_sales,
      rm7_log_sales, rm28_log_sales, promo_in_last_7d,
      dow, month, year,
      family, class, cluster,
      baseline_discount_pct, baseline_effective_price
    FROM `{table}`
    WHERE date = @d
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("d","DATE", the_date)]
        )
    )
    return job.result().to_dataframe(create_bqstorage_client=True)

def predict_units(booster: xgb.Booster, X: pd.DataFrame) -> np.ndarray:
    d = xgb.DMatrix(X, enable_categorical=True)
    return booster.predict(d)

def day_sweep(booster: xgb.Booster,
              cat_vocab: Dict[str, List[str]],
              base: pd.DataFrame,
              discount_grid: List[float],
              num_shards: int = 1,
              shard_id: int = 0) -> pd.DataFrame:
    """Run sweep for one day; optionally process only one shard."""
    # Optional sharding in-Python to further cap memory
    if num_shards > 1:
        key_hash = (base["store_nbr"].astype(str) + "|" + base["item_nbr"].astype(str)).apply(hash).astype(np.int64)
        base = base[(np.abs(key_hash) % num_shards) == shard_id]
        if base.empty:
            return pd.DataFrame()

    # ----- Baseline -----
    base_infer = base.copy()
    base_infer["effective_price"] = base_infer["baseline_effective_price"]
    base_infer["discount_pct"]    = base_infer["baseline_discount_pct"]

    base_feats = base_infer[FEATURES].copy()
    base_feats = _cast_numeric(base_feats)
    base_feats = _apply_categories(base_feats, cat_vocab)
    base_pred = predict_units(booster, base_feats)

    baseline = base[["date","store_nbr","item_nbr","baseline_discount_pct","baseline_effective_price"]].copy()
    baseline["pred_units_baseline"] = base_pred.astype(np.float32)
    baseline["baseline_revenue"] = baseline["baseline_effective_price"].astype(np.float32) * baseline["pred_units_baseline"]

    # ----- Candidates (price grid) -----
    # Repeat each row for each discount
    blocks = []
    for g in discount_grid:
        blk = base.copy()
        blk["cand_discount_pct"] = np.float32(g)
        blk["cand_effective_price"] = np.round(blk["base_price"].astype(np.float32) * (1.0 - g), 2)
        # features for model
        blk["effective_price"] = blk["cand_effective_price"]
        blk["discount_pct"]    = blk["cand_discount_pct"]
        blocks.append(blk)
    cand = pd.concat(blocks, ignore_index=True)

    cand_feats = cand[FEATURES].copy()
    cand_feats = _cast_numeric(cand_feats)
    cand_feats = _apply_categories(cand_feats, cat_vocab)
    cand["pred_units"] = predict_units(booster, cand_feats).astype(np.float32)
    cand["exp_revenue"] = cand["cand_effective_price"].astype(np.float32) * cand["pred_units"]

    # Argmax per (date, store, item)
    idx = cand.groupby(["date","store_nbr","item_nbr"])["exp_revenue"].idxmax()
    best = cand.loc[idx, [
        "date","store_nbr","item_nbr",
        "cand_discount_pct","cand_effective_price","pred_units","exp_revenue"
    ]].rename(columns={
        "cand_discount_pct": "policy_discount_pct",
        "cand_effective_price": "policy_effective_price",
        "pred_units": "pred_units_policy",
        "exp_revenue": "policy_revenue"
    })

    # Merge with baseline outputs
    out = baseline.merge(best, on=["date","store_nbr","item_nbr"], how="inner")
    return out

def delete_bq_partition(project: str, table_fq: str, the_date: str):
    """Idempotency: remove existing rows for this date before append."""
    client = bigquery.Client(project=project)
    sql = f"DELETE FROM `{table_fq}` WHERE date = @d"
    client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("d","DATE", the_date)]
        )
    ).result()

def write_bq(project: str, dataset: str, table: str, df: pd.DataFrame, mode="append"):
    from pandas_gbq import to_gbq
    fq = f"{project}.{dataset}.{table}"
    to_gbq(df, fq, project_id=project, if_exists=("append" if mode=="append" else "replace"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--model_path", default="models/xgb_cat.json")
    ap.add_argument("--cat_vocab_path", default="models/xgb_cat_vocab.json")
    ap.add_argument("--start_date", default="2017-08-01")
    ap.add_argument("--end_date",   default="2017-08-15")
    ap.add_argument("--discount_grid", default="0.0,0.1,0.2,0.3,0.4,0.5")
    ap.add_argument("--num_shards", type=int, default=1, help="Process each day in N shards (in-Python)")
    ap.add_argument("--out_csv", default="outputs/xgb_cat_policy_eval_test.csv")
    ap.add_argument("--write_bq", action="store_true")
    ap.add_argument("--bq_table", default="dynamic_pricing_ml.xgb_policy_eval_test",
                    help="dataset.table to write results into")
    args = ap.parse_args()

    # Load model + vocab
    booster = xgb.Booster()
    booster.load_model(args.model_path)
    with open(args.cat_vocab_path) as f:
        cat_vocab = json.load(f)

    # Parse discount grid
    grid = [float(x) for x in args.discount_grid.split(",") if x.strip() != ""]
    dates = pd.date_range(args.start_date, args.end_date, freq="D")

    # CSV setup
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    wrote_header = not os.path.exists(args.out_csv)

    # Loop by day (and optional shards)
    for d in dates:
        dstr = d.date().isoformat()
        base = load_scoring_frame(args.project, dstr)
        if base.empty:
            print(f"[{dstr}] no rows")
            continue

        day_frames = []
        for shard_id in range(args.num_shards):
            df_out = day_sweep(booster, cat_vocab, base, grid, num_shards=args.num_shards, shard_id=shard_id)
            if not df_out.empty:
                day_frames.append(df_out)
                print(f"[{dstr} shard {shard_id}/{args.num_shards}] rows={len(df_out):,}")
        if not day_frames:
            continue

        day_result = pd.concat(day_frames, ignore_index=True)

        # Write CSV (append)
        day_result.to_csv(args.out_csv, mode=("w" if wrote_header else "a"),
                          header=wrote_header, index=False)
        wrote_header = False

        # Optional: write to BigQuery (idempotent per day)
        if args.write_bq:
            ds, tbl = args.bq_table.split(".", 1)
            delete_bq_partition(args.project, f"{args.project}.{ds}.{tbl}", dstr)
            write_bq(args.project, ds, tbl, day_result, mode="append")
            print(f"[{dstr}] wrote {len(day_result):,} rows to {args.project}.{args.bq_table}")

    print(f"Done. CSV at {args.out_csv}")

if __name__ == "__main__":
    # safety check for xgboost version
    ver = tuple(int(x) for x in xgb.__version__.split(".")[:2])
    if ver < (1, 6):
        print(f"XGBoost {xgb.__version__} is too old for native categorical. Please upgrade to >= 1.6.", file=sys.stderr)
        sys.exit(1)
    main()