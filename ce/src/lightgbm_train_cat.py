# =============================================================
# file: ce/src/lightgbm_train_cat.py
# Purpose: Train LightGBM regressor on ~30M rows using native categoricals
# Notes:
#  - Keeps EXACT same features/target as BQML & XGBoost
#  - Uses pandas 'category' dtype with fixed vocab + '__UNK__' for unseen values
#  - Downcasts numerics to float32 / small ints for memory efficiency
#  - Logs metrics/artifacts to MLflow
# =============================================================

#!/usr/bin/env python3
import os, argparse, json
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import mlflow

TRACKING_SERVER_HOST = args.mlflow_server
mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")
print(f"tracking URI: '{mlflow.get_tracking_uri()}'")

# ---------- Spec (identical to BQML/XGB) ----------
FEATURES: List[str] = [
    # price / expiry
    "effective_price","discount_pct","time_to_expiry","base_price",
    # autoregressive
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales","promo_in_last_7d",
    # calendar
    "dow","month","year",
    # categoricals (native categorical, NOT one-hot)
    "family","class","store_nbr","cluster"
]
LABEL = "unit_sales"
BQ_TABLE = "{project}.dynamic_pricing_ml.features_split"

CAT_COLS = ["family","class","store_nbr","cluster"]
FLOAT_COLS = [
    "effective_price","discount_pct","base_price",
    "lag1_log_sales","lag7_log_sales","lag14_log_sales","lag28_log_sales",
    "rm7_log_sales","rm28_log_sales"
]
INT_COLS = ["time_to_expiry","promo_in_last_7d","dow","month","year"]


def rmse(y, yhat) -> float:
    from sklearn.metrics import mean_squared_error
    return float(np.sqrt(mean_squared_error(y, yhat)))


def load_split(project: str, split: str, cols: List[str]) -> pd.DataFrame:
    """Load one split from BigQuery with only needed columns."""
    client = bigquery.Client(project=project)
    table = BQ_TABLE.format(project=project)
    sql = f"""
    SELECT {', '.join(['date'] + cols + [LABEL, 'split'])}
    FROM `{table}`
    WHERE split = @split
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("split","STRING",split)]
        )
    )
    return job.result().to_dataframe(create_bqstorage_client=True)


def cast_and_fit_vocab_train(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """Down-cast numerics; fit categorical vocabularies on TRAIN; set dtype=category."""
    X = df[FEATURES].copy()

    # Down-cast numerics
    for c in FLOAT_COLS:
        X[c] = X[c].astype(np.float32)
    X["time_to_expiry"] = X["time_to_expiry"].astype(np.int16)
    X["promo_in_last_7d"] = X["promo_in_last_7d"].astype(np.int8)
    X["dow"] = X["dow"].astype(np.int8)
    X["month"] = X["month"].astype(np.int8)
    X["year"] = X["year"].astype(np.int16)

    # Fit vocabularies from TRAIN
    cat_vocab: Dict[str, List[str]] = {}
    for c in CAT_COLS:
        s = X[c].astype(str)
        vocab = pd.Index(s.unique()).astype(str).tolist()
        vocab_with_unk = vocab + ["__UNK__"]
        X[c] = pd.Categorical(s, categories=vocab_with_unk)
        cat_vocab[c] = vocab  # store without UNK
    return X, cat_vocab


def apply_vocab(df: pd.DataFrame, cat_vocab: Dict[str, List[str]]) -> pd.DataFrame:
    """Apply TRAIN vocabularies to VAL/TEST; unseen -> '__UNK__'."""
    X = df[FEATURES].copy()
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
        mask = ~s.isin(vocab)
        if mask.any():
            s.loc[mask] = "__UNK__"
        X[c] = pd.Categorical(s, categories=(vocab + ["__UNK__"]))
    return X


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--mlflow_server", required=True)
    ap.add_argument("--model_out", default="models/lgbm_cat.txt")
    ap.add_argument("--cat_vocab_out", default="models/lgbm_cat_vocab.json")
    ap.add_argument("--experiment", default="peri-price-lgbm-cat")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.model_out), exist_ok=True)
    os.makedirs(os.path.dirname(args.cat_vocab_out), exist_ok=True)

    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name="lgbm_cat_train"):
        # 1) Load splits
        df_tr = load_split(args.project, "train", FEATURES)
        df_va = load_split(args.project, "valid", FEATURES)
        df_te = load_split(args.project, "test",  FEATURES)

        ytr = df_tr[LABEL].astype(np.float32).values
        yva = df_va[LABEL].astype(np.float32).values
        yte = df_te[LABEL].astype(np.float32).values

        # 2) Cast + categories
        Xtr, cat_vocab = cast_and_fit_vocab_train(df_tr)
        Xva = apply_vocab(df_va, cat_vocab)
        Xte = apply_vocab(df_te, cat_vocab)

        # 3) LightGBM datasets 
        dtrain = lgb.Dataset(Xtr, label=ytr, categorical_feature=CAT_COLS, free_raw_data=False)
        dvalid = lgb.Dataset(Xva, label=yva, categorical_feature=CAT_COLS, free_raw_data=False)

        # 4) Params (histogram boosting is default). Keep memory safe.
        params = dict(
            objective="regression",
            metric=["rmse","mae"],
            learning_rate=0.08,
            num_leaves=255,         
            max_depth=-1,
            max_bin=255,            
            feature_fraction=0.8,
            bagging_fraction=0.8,
            bagging_freq=1,
            min_data_in_leaf=64,
            verbosity=-1,
            force_row_wise=True     
        )
        mlflow.log_params(params)

        booster = lgb.train(
            params,
            dtrain,
            num_boost_round=800,
            valid_sets=[dtrain, dvalid],
            valid_names=["train","valid"],
            callbacks=[
                lgb.early_stopping(stopping_rounds=100),
                lgb.log_evaluation(period=100)
            ]
        )

        # 5) Eval
        val_pred = booster.predict(Xva, num_iteration=booster.best_iteration)
        test_pred = booster.predict(Xte, num_iteration=booster.best_iteration)

        metrics = {
            "valid_mae": float(mean_absolute_error(yva, val_pred)),
            "valid_rmse": rmse(yva, val_pred),
            "test_mae":  float(mean_absolute_error(yte, test_pred)),
            "test_rmse": rmse(yte, test_pred),
            "best_iteration": int(booster.best_iteration or 0)
        }
        for k,v in metrics.items():
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

