#!/usr/bin/env python3
"""
scripts/fetch_data.py — downloads the raw DataCo supply chain CSV into
data/raw/.

Same reasoning as the sibling ecommerce-data-warehouse project's fetch
script: the raw file (~92MB) isn't committed to this repo. This script
clones a public GitHub mirror instead of requiring Kaggle credentials.
Original dataset:
    https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis

Usage:
    python scripts/fetch_data.py
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

MIRROR_REPO = "https://github.com/ashishpatel26/DataCo-SMART-SUPPLY-CHAIN-FOR-BIG-DATA-ANALYSIS.git"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

EXPECTED_FILES = [
    "DataCoSupplyChainDataset.csv",
    "DescriptionDataCoSupplyChain.csv",
]


def already_present() -> bool:
    return all((RAW_DIR / f).exists() for f in EXPECTED_FILES)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if already_present():
        print(f"Raw files already present in {RAW_DIR} — nothing to do.")
        return

    print(f"Cloning {MIRROR_REPO} ...")
    with tempfile.TemporaryDirectory() as tmp:
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", MIRROR_REPO, tmp],
                check=True, capture_output=True, text=True,
            )
        except FileNotFoundError:
            sys.exit("git is not installed — install git, or manually download "
                      "DataCoSupplyChainDataset.csv from the Kaggle page linked "
                      "in this script's docstring into data/raw/.")
        except subprocess.CalledProcessError as e:
            sys.exit(f"git clone failed:\n{e.stderr}")

        for filename in EXPECTED_FILES:
            src = Path(tmp) / filename
            if not src.exists():
                sys.exit(f"Expected file not found in mirror: {filename}. "
                          f"The mirror repo layout may have changed — see this "
                          f"script's docstring for the official Kaggle source.")
            shutil.copy(src, RAW_DIR / filename)
            print(f"  {filename}")

    print(f"\nDone. Files written to {RAW_DIR}")
    print("Next: python run_pipeline.py")


if __name__ == "__main__":
    main()
