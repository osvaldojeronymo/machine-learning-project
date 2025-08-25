# src/eda/baseline.py
from __future__ import annotations
import pandas as pd

__all__ = ["baseline_from_prevalence"]

def baseline_from_prevalence(prev_df: pd.DataFrame) -> pd.DataFrame:
    """
    Baseline AUPRC for random ranking equals the prevalence of the positive class.
    Here we simply copy the prevalence table as the baseline.
    """
    if prev_df is None or prev_df.empty:
        return pd.DataFrame()
    return prev_df.copy()
