# src/eda/artifacts.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
from .io_tar import file_hash

__all__ = ["write_manifest"]

def write_manifest(
    path_targets: Path,
    path_client_split: Path,
    reports_dir: Path,
    targets_df: pd.DataFrame,
    client_split_df: pd.DataFrame,
) -> dict:
    """
    Build and save a manifest.json with input identities (paths, sizes, hashes)
    and quick notes about the dataframes produced.
    """
    reports_dir = Path(reports_dir)
    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "inputs": {
            "targets_tar": {
                "path": str(path_targets),
                "size_bytes": Path(path_targets).stat().st_size if Path(path_targets).exists() else None,
                "md5": file_hash(path_targets, "md5") if Path(path_targets).exists() else None,
                "sha256": file_hash(path_targets, "sha256") if Path(path_targets).exists() else None,
            },
            "client_split_tar": {
                "path": str(path_client_split),
                "size_bytes": Path(path_client_split).stat().st_size if Path(path_client_split).exists() else None,
                "md5": file_hash(path_client_split, "md5") if Path(path_client_split).exists() else None,
                "sha256": file_hash(path_client_split, "sha256") if Path(path_client_split).exists() else None,
            },
        },
        "reports": {
            "schema_targets": "reports/schema_targets.json",
            "schema_client_split": "reports/schema_client_split.json",
            "volumetria": "reports/volumetria.json",
            "prevalencia": "reports/prevalencia.csv",
            "baseline_auprc": "reports/baseline_auprc.csv",
            "fold_leakage": "reports/fold_leakage.csv" if (reports_dir / "fold_leakage.csv").exists() else None,
        },
        "notes": {
            "mon_dtype": str(targets_df["mon"].dtype) if "mon" in targets_df.columns else None,
            "target_cols": [c for c in targets_df.columns if c.startswith("target_")],
            "n_rows_targets": int(len(targets_df)),
            "n_rows_client_split": int(len(client_split_df)),
        }
    }
    with (reports_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    return manifest
