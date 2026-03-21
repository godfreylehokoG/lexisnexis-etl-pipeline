"""
Quarantine layer.
Writes rejected rows to CSV files with rejection reasons.
"""

from pathlib import Path

import pandas as pd


def write_quarantine(df: pd.DataFrame, output_dir: str, filename: str) -> int:
    """
    Write rejected DataFrame to a CSV in the quarantine directory.
    Returns the number of rejected rows written.
    """

    if df.empty:
        return 0

    output_path = Path(output_dir) / filename
    df.to_csv(output_path, index=False)

    return len(df)