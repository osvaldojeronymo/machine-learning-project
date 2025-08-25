# src/eda/io_tar.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
import tarfile, tempfile, re, hashlib
import pandas as pd

__all__ = [
    "list_tar",
    "read_parquet_partitions_from_tar",
    "normalize_mon_period_m",
    "file_hash",
]

def list_tar(tar_path: Path, limit: int = 60) -> None:
    """
    List regular files inside a TAR.GZ archive (quick debug for internal names).
    """
    tar_path = Path(tar_path)
    assert tar_path.exists(), f"Arquivo não existe: {tar_path}"
    with tarfile.open(tar_path, "r:gz") as tar:
        names = [m.name for m in tar.getmembers() if m.isreg()]
    print(f"[{tar_path.name}] {len(names)} itens")
    for n in names[:limit]:
        print(" -", n)
    if len(names) > limit:
        print(f"... (+{len(names)-limit} itens)")

def _parse_partitions_from_path(path: str, expected_keys: Tuple[str, ...] = ("fold",)) -> Dict[str, object]:
    """
    Extract key=value segments from a path (e.g., '.../fold=3/...').
    Converts pure-digit values to int.
    """
    kv: Dict[str, object] = {}
    for seg in path.split("/"):
        if "=" in seg:
            k, v = seg.split("=", 1)
            if k in expected_keys:
                if re.fullmatch(r"\d+", v):
                    v = int(v)
                kv[k] = v
    return kv

def read_parquet_partitions_from_tar(
    tar_path: Path,
    prefix: str,
    expected_keys: Tuple[str, ...] = ("fold",),
    allowed_partitions: Optional[Dict[str, Iterable[object]]] = None,
    columns: Optional[Iterable[str]] = None,
    max_parts: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read ALL .parquet files under `prefix` inside a .tar.gz, reconstruct partition columns
    (e.g., 'fold') from the path, and concatenate them into a single DataFrame.

    Parameters
    ----------
    tar_path : Path
        Path to the .tar.gz file.
    prefix : str
        Path prefix inside the archive (e.g., "targets/" or "client_split/").
    expected_keys : tuple[str]
        Partition keys expected in the internal path (e.g., ('fold',)).
    allowed_partitions : dict[str, set] | None
        If provided, keep only members whose partition values (e.g., fold) are in the allowed set.
        Example: {"fold": {0,1}}.
    columns : Iterable[str] | None
        Optional subset of columns to read from parquet.
    max_parts : int | None
        If provided, limit the number of parquet files to read (useful for quick EDA).

    Returns
    -------
    pd.DataFrame
        Concatenated dataframe with partition columns added when missing.

    Notes
    -----
    Requires pyarrow or fastparquet.
    """
    tar_path = Path(tar_path)
    assert tar_path.exists(), f"Arquivo não existe: {tar_path}"
    frames = []
    with tarfile.open(tar_path, "r:gz") as tar:
        members = [
            m for m in tar.getmembers()
            if m.isreg()
            and m.name.lower().startswith(prefix.lower())
            and m.name.lower().endswith(".parquet")
        ]

        # Filter by allowed partitions
        if allowed_partitions:
            keep = []
            for m in members:
                part_info = _parse_partitions_from_path(m.name, expected_keys=expected_keys)
                ok = True
                for k, allowed in allowed_partitions.items():
                    if k in part_info and part_info[k] not in allowed:
                        ok = False; break
                if ok: keep.append(m)
            members = keep

        if max_parts is not None and len(members) > max_parts:
            members = members[:max_parts]

        if not members:
            raise FileNotFoundError(f"Nenhum .parquet encontrado sob '{prefix}' em {tar_path.name}")

        for m in members:
            part_info = _parse_partitions_from_path(m.name, expected_keys=expected_keys)
            with tar.extractfile(m) as fh, tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp.write(fh.read()); tmp.flush()
                try:
                    df = pd.read_parquet(tmp.name, columns=columns)  # requires pyarrow/fastparquet
                finally:
                    Path(tmp.name).unlink(missing_ok=True)
            for k, v in part_info.items():
                if k not in df.columns:
                    df[k] = v
            frames.append(df)

    out = pd.concat(frames, ignore_index=True, sort=False)
    parts_rec = [k for k in expected_keys if k in out.columns]
    print(f"[OK] Lidos {len(frames)} arquivos parquet de '{prefix}' em {tar_path.name}. "
          f"Partições recuperadas: {parts_rec}")
    return out

def normalize_mon_period_m(df: pd.DataFrame, col: str = "mon") -> pd.DataFrame:
    """
    Normalize a 'mon' column to pandas Period[M]. Warn about conversions to NaT.
    """
    if col not in df.columns:
        print(f"[INFO] Coluna '{col}' ausente, pulando normalização para Period[M].")
        return df
    before_na = df[col].isna().sum()
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.to_period("M")
    after_na = df[col].isna().sum()
    if after_na > before_na:
        print(f"[ALERTA] {after_na - before_na} valores em '{col}' viraram NaT ao normalizar.")
    return df

def file_hash(path: Path, algo: str = "md5", chunk: int = 2**20) -> str:
    """
    Compute a file hash (md5/sha256). Useful for manifests.
    """
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()
