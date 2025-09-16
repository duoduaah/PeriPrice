#!/usr/bin/env python3
import os, argparse, json
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import mlflow



mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")
print(f"tracking URI: '{mlflow.get_tracking_uri()}'")



# --- Spec: keep identical to BQML/previous code ---
FEATURES: List[str] = [
    # price / expiry
    "effective_price","discount_pct","time_to_expiry","base_price",
    # autoregressive
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales","promo_in_last_7d",
    # calendar
    "dow","month","year",
    # categoricals (now native categorical, NOT one-hot)
    "family","class","store_nbr","cluster"
]
LABEL = "unit_sales"
BQ_TABLE = "{project}.dynamic_pricing_ml.features_split"

CAT_COLS = ["family","class","store_nbr","cluster"]  # categorical
FLOAT_COLS = [
    "effective_price","discount_pct","base_price",
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales"
]
INT_COLS = ["time_to_expiry","promo_in_last_7d","dow","month","year"]

def rmse(y, yhat) -> float:
    return float(np.sqrt(mean_squared_error(y, yhat)))

def load_split(project: str, split: str, cols: List[str]) -> pd.DataFrame:
    client = bigquery.Client(project=project)
    table = BQ_TABLE.format(project=project)
    sql = f"""
    SELECT {", ".join(["date"] + cols + [LABEL, "split"])}
    FROM `{table}`
    WHERE split = @split
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("split","STRING",split)]
        )
    )
    # Use BQ Storage API to stream efficiently
    return job.result().to_dataframe(create_bqstorage_client=True)

def cast_and_categorize_train(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """Down-cast numerics and fit category vocabularies on TRAIN."""
    X = df[FEATURES].copy()

    # Down-cast
    for c in FLOAT_COLS:
        X[c] = X[c].astype(np.float32)
    # Small integers
    X["time_to_expiry"] = X["time_to_expiry"].astype(np.int16)
    X["promo_in_last_7d"] = X["promo_in_last_7d"].astype(np.int8)
    X["dow"] = X["dow"].astype(np.int8)
    X["month"] = X["month"].astype(np.int8)
    X["year"] = X["year"].astype(np.int16)

    # Categorical vocabularies from TRAIN
    cat_vocab: Dict[str, List[str]] = {}
    for c in CAT_COLS:
        s = X[c].astype(str)
        # Fit vocab from train only
        vocab = pd.Index(s.unique()).astype(str).tolist()
        # Make category dtype with known vocab (add UNK slot at the end)
        vocab_with_unk = vocab + ["__UNK__"]
        X[c] = pd.Categorical(s, categories=vocab_with_unk)
        cat_vocab[c] = vocab  # store without UNK for saving
    return X, cat_vocab

def apply_categories(df: pd.DataFrame, cat_vocab: Dict[str, List[str]]) -> pd.DataFrame:
    """Apply train vocabs to VAL/TEST; unseen -> '__UNK__'."""
    X = df[FEATURES].copy()

    # Down-cast numerics
    for c in FLOAT_COLS:
        X[c] = X[c].astype(np.float32)
    X["time_to_expiry"] = X["time_to_expiry"].astype(np.int16)
    X["promo_in_last_7d"] = X["promo_in_last_7d"].astype(np.int8)
    X["dow"] = X["dow"].astype(np.int8)
    X["month"] = X["month"].astype(np.int8)
    X["year"] = X["year"].astype(np.int16)

    for c in CAT_COLS:
        s = X[c].astype(str)
        vocab = cat_vocab[c]
        # map unseen values to UNK
        mask = ~s.isin(vocab)
        if mask.any():
            s.loc[mask] = "__UNK__"
        X[c] = pd.Categorical(s, categories=(vocab + ["__UNK__"]))
    return X

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--mlflow_server", required=True)
    ap.add_argument("--model_out", default="models/xgb_cat.json")
    ap.add_argument("--cat_vocab_out", default="models/xgb_cat_vocab.json")
    ap.add_argument("--experiment", default="peri-price-xgb-cat")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.model_out), exist_ok=True)
    os.makedirs(os.path.dirname(args.cat_vocab_out), exist_ok=True)
    TRACKING_SERVER_HOST = args.mlflow_server

    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name="xgb_cat_train"):
        # 1) Load splits
        df_tr = load_split(args.project, "train", FEATURES)
        df_va = load_split(args.project, "valid", FEATURES)
        df_te = load_split(args.project, "test",  FEATURES)

        # 2) Build y and memory-optimal X with native categorical dtype
        ytr = df_tr[LABEL].astype(np.float32).values
        yva = df_va[LABEL].astype(np.float32).values
        yte = df_te[LABEL].astype(np.float32).values

        Xtr, cat_vocab = cast_and_categorize_train(df_tr)
        Xva = apply_categories(df_va, cat_vocab)
        Xte = apply_categories(df_te, cat_vocab)

        # 3) DMatrix with enable_categorical=True
        dtrain = xgb.DMatrix(Xtr, label=ytr, enable_categorical=True)
        dvalid = xgb.DMatrix(Xva, label=yva, enable_categorical=True)
        dtest  = xgb.DMatrix(Xte, label=yte, enable_categorical=True)

        # 4) Train (histogram algorithm + categorical)
        params = dict(
            objective="reg:squarederror",
            eval_metric="rmse",
            tree_method="hist",
            max_bin=256,           # drop to 128 if memory is still tight
            max_depth=8,          # or use max_leaves with grow_policy='lossguide'
            subsample=0.8,
            colsample_bytree=0.8,
            sampling_method="uniform",  # 'gradient_based' can help on very large data
            nthread=-1
        )
        mlflow.log_params(params)

        booster = xgb.train(
            params,
            dtrain,
            num_boost_round=1000,
            evals=[(dtrain, "train"), (dvalid, "valid")],
            early_stopping_rounds=50,
            verbose_eval=50
        )

        # 5) Eval
        val_pred = booster.predict(dvalid)
        test_pred = booster.predict(dtest)

        metrics = {
            "valid_mae": float(mean_absolute_error(yva, val_pred)),
            "valid_rmse": rmse(yva, val_pred),
            "test_mae":  float(mean_absolute_error(yte, test_pred)),
            "test_rmse": rmse(yte, test_pred),
            "best_iteration": int(booster.best_iteration)
        }
        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        # 6) Save model + vocab
        booster.save_model(args.model_out)
        mlflow.log_artifact(args.model_out)

        with open(args.cat_vocab_out, "w") as f:
            json.dump(cat_vocab, f, indent=2)
        mlflow.log_artifact(args.cat_vocab_out)

        print("Eval:", metrics)

if __name__ == "__main__":
    main()