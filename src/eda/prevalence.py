# src/eda/prevalence.py
from __future__ import annotations
import pandas as pd

__all__ = ["prevalence_table"]

def prevalence_table(df_targets: pd.DataFrame) -> pd.DataFrame:
    """
    Compute prevalence for target_1..4 globally (ALL), by month (mon) and by fold.
    """
    cols = [c for c in ["target_1","target_2","target_3","target_4"] if c in df_targets]
    if not cols:
        return pd.DataFrame()
    out = {}
    out["ALL"] = {c: float(df_targets[c].mean()) for c in cols}
    if "mon" in df_targets:
        for m, g in df_targets.groupby("mon"):
            out[f"mon={m}"] = {c: float(g[c].mean()) for c in cols}
    if "fold" in df_targets:
        for f, g in df_targets.groupby("fold"):
            out[f"fold={f}"] = {c: float(g[c].mean()) for c in cols}
    df = pd.DataFrame(out).T.sort_index()
    return df
