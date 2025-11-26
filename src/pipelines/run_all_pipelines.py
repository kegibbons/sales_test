from __future__ import annotations

from pathlib import Path
import subprocess
import sys

"""
run_all_pipelines.py

Orchestrates the full medallion pipeline:

  1. Bronze load       (step01_bronze_load.py)
  2. Silver load       (step02_silver_load.py)
  3. Gold build        (step03_gold_load.py)
  4. Export medallions (step04_export_medallion_layers.py)

This script is designed to live in:

    .../sales_test/src/pipelines/

and it resolves all paths relative to that.
"""

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

THIS_FILE = Path(__file__).resolve()          # …/src/pipelines/run_all_pipelines.py
PIPELINES_DIR = THIS_FILE.parent              # …/src/pipelines
SRC_DIR = PIPELINES_DIR.parent                # …/src
PROJECT_ROOT = SRC_DIR.parent                 # …/sales_test

# Ordered list of pipeline steps (filenames in /src/pipelines)
PIPELINE_STEPS = [
    "step01_bronze_load.py",
    "step02_silver_load.py",
    "step03_gold_load.py",
    "step04_export_medallion_layers.py",
]


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def run_step(script_name: str) -> None:
    script_path = PIPELINES_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Pipeline script not found: {script_path}")

    print(f"\n=== Running {script_name} ===")
    # Use the same interpreter uv is using
    subprocess.check_call([sys.executable, str(script_path)])


def main() -> None:
    print("Starting full medallion pipeline...\n")

    for script in PIPELINE_STEPS:
        run_step(script)

    print("\nAll pipeline steps completed successfully.")


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()
