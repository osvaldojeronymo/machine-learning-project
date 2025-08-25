# src/eda/check_schema_vol.py
from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

__all__ = ["schema_report", "leakage_report", "volumetria_report"]

def schema_report(df: pd.DataFrame, name: str, reports_dir: Path | None = None) -> dict:
    rep = {
        "n_rows": int(len(df)),
        "n_cols": int(df.shape[1]),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "nulls": {c: int(df[c].isna().sum()) for c in df.columns},
        "nunique": {c: int(df[c].nunique(dropna=True)) for c in df.columns},
        "mem_mb": float(df.memory_usage(deep=True).sum()/1_048_576),
    }
    if reports_dir is not None:
        reports_dir = Path(reports_dir)
        reports_dir.mkdir(parents=True, exist_ok=True)
        with (reports_dir / f"schema_{name}.json").open("w") as f:
            json.dump(rep, f, indent=2, ensure_ascii=False)
    return rep

def leakage_report(client_split: pd.DataFrame) -> pd.DataFrame:
    """
    Clients that appear in more than one fold.
    """
    if not {"client_id","fold"}.issubset(client_split.columns):
        return pd.DataFrame()
    g = client_split.groupby("client_id")["fold"].nunique()
    leak = g[g > 1].reset_index(name="n_folds_distintos")
    return leak

def volumetria_report(targets: pd.DataFrame, client_split: pd.DataFrame) -> dict:
    """
    Consolidated volumetrics: n clients, counts by fold, rows by month, rows by (month, fold).
    """
    vol: dict = {}
    if "client_id" in client_split.columns:
        vol["n_clients_total"] = int(client_split["client_id"].nunique())
        if "fold" in client_split.columns:
            vol["clientes_por_fold"] = {int(k): int(v) for k, v in client_split.groupby("fold")["client_id"].nunique().items()}
    vol["rows_targets_total"] = int(len(targets))
    if "mon" in targets.columns:
        vol["rows_por_mes"] = {str(k): int(v) for k, v in targets.groupby("mon").size().items()}
    if {"mon","fold"}.issubset(targets.columns):
        vol["rows_por_mes_fold"] = {f"{str(m)}|{int(f)}": int(v)
                                    for (m, f), v in targets.groupby(["mon","fold"]).size().items()}
    return vol
