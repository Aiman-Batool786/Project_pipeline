# batch_export.py
# Thin wrapper so main.py can do: from batch_export import run_export
import os, sys, sqlite3, shutil, json
from datetime import datetime

# Re-export run_export directly — implementation lives in export_to_template.py
# If you placed that file in data/, use this:
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "data"))
from export_to_template import run_export  # noqa: F401
