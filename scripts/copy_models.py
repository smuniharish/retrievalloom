"""Copy the local reference models into the git-ignored ``models/`` directory.

The models are used only by the examples and by tests marked ``models``. They are
never a runtime dependency of RetrievalLoom.

Usage::

    uv run python scripts/copy_models.py --source "C:/path/to/HF_MODELS"
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

MODELS = {
    "bge-m3": "bge-m3",
    "bge-reranker-v2-m3": "bge-reranker-v2-m3",
    "UltraBERT-150M-nli": "NLI MODELS/UltraBERT-150M-nli",
}
IGNORE = shutil.ignore_patterns(".cache", "imgs", "assets", "*.jpg", "*.png", "*.webp", ".DS_Store")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Folder holding the models.")
    parser.add_argument("--target", type=Path, default=Path(__file__).parents[1] / "models")
    args = parser.parse_args()
    args.target.mkdir(parents=True, exist_ok=True)
    for name, relative in MODELS.items():
        source = args.source / relative
        if not source.is_dir():
            print(f"missing: {source}")
            continue
        destination = args.target / name
        print(f"copying {source} -> {destination}")
        shutil.copytree(source, destination, ignore=IGNORE, dirs_exist_ok=True)
    print("done")


if __name__ == "__main__":
    main()
