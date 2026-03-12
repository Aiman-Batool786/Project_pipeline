"""
template_filler.py
──────────────────
Fills the Octopia .xlsm template with mapped product data.

CRITICAL: Column lookup uses ROW 5 (Octopia field keys like 'title',
'description', 'richMarketingDescription', '3264', etc.)
NOT row 4 (human headers like 'Titre*', 'Description*').

main.py calls:
    template_file = fill_template_for_product(
        TEMPLATE_PATH,
        mapped_data,   ← keys are Octopia field keys from row 5
        product_id,
        FILLED_TEMPLATES_DIR
    )
"""

import json
import os
import shutil
from datetime import datetime

from openpyxl import load_workbook


# ─────────────────────────────────────────
# TEMPLATE CONSTANTS
# ─────────────────────────────────────────
FIELD_KEY_ROW = 5    # Row containing Octopia field keys (title, description, ...)
CATEGORY_ROW  = 1    # Row 1: OCTOPIA | Catégorie | code | leaf
DATA_ROW      = 9    # First available data row


class TemplateFiller:
    """Fills an Octopia .xlsm template using row-5 field keys."""

    def __init__(self, template_path: str):
        self.template_path = template_path
        self.wb = None
        self.ws = None
        # Built at load time: field_key → column index
        self._field_col: dict[str, int] = {}

    # ─────────────────────────────────────
    def load_template(self) -> bool:
        try:
            self.wb = load_workbook(self.template_path, keep_vba=True)
            self.ws = self.wb.active
            self._build_field_map()
            print(f"[template] ✅ Loaded: {self.ws.title}")
            print(f"[template] 📋 {len(self._field_col)} field keys mapped from ROW {FIELD_KEY_ROW}")
            return True
        except Exception as e:
            print(f"[template] ❌ Load error: {e}")
            return False

    # ─────────────────────────────────────
    def _build_field_map(self):
        """
        Read ROW 5 and build {field_key: col_index}.
        Row 5 contains the Octopia API field names, e.g.:
          col 3  → 'title'
          col 4  → 'description'
          col 9  → 'richMarketingDescription'
          col 26 → '3264'   (Couleur principale)
        """
        self._field_col = {}
        for col in range(1, self.ws.max_column + 1):
            val = self.ws.cell(row=FIELD_KEY_ROW, column=col).value
            if val:
                self._field_col[str(val).strip()] = col

    # ─────────────────────────────────────
    def _find_next_data_row(self) -> int:
        """Return first empty row at or after DATA_ROW (checks col 3 = title)."""
        for r in range(DATA_ROW, self.ws.max_row + 2):
            if self.ws.cell(row=r, column=3).value is None:
                return r
        return self.ws.max_row + 1

    # ─────────────────────────────────────
    def fill_product_data(self, mapped_data: dict) -> bool:
        """
        Write mapped_data into the next available data row.
        Keys in mapped_data must match ROW 5 field keys.
        Special internal keys prefixed with '_' are handled separately.
        """
        try:
            row = self._find_next_data_row()
            filled = 0
            skipped = []

            print(f"[template] 📝 Writing {len(mapped_data)} fields → ROW {row}")

            for field_key, value in mapped_data.items():

                # ── Internal keys: category goes in row 1 ────
                if field_key == "_category_code":
                    self.ws.cell(row=CATEGORY_ROW, column=3).value = str(value)
                    continue
                if field_key == "_category_leaf":
                    self.ws.cell(row=CATEGORY_ROW, column=4).value = str(value)
                    continue
                if field_key.startswith("_"):
                    continue

                col = self._field_col.get(field_key)
                if col is None:
                    skipped.append(field_key)
                    continue

                # Normalise value type
                if isinstance(value, list):
                    cell_val = " | ".join(str(v) for v in value if v)
                elif isinstance(value, dict):
                    cell_val = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    continue
                else:
                    cell_val = str(value).strip()

                if not cell_val:
                    continue

                self.ws.cell(row=row, column=col).value = cell_val
                filled += 1

                preview = cell_val[:50].replace("\n", " ")
                print(f"[template]   col {col:2d}  {field_key:35s} = {preview}")

            print(f"[template] ✅ {filled} fields written, {len(skipped)} skipped")
            if skipped:
                print(f"[template] ⚠️  Skipped (not in template): {skipped}")

            # Ensure OCTOPIA header is in row 1 col 1-2
            if not self.ws.cell(row=CATEGORY_ROW, column=1).value:
                self.ws.cell(row=CATEGORY_ROW, column=1).value = "OCTOPIA"
                self.ws.cell(row=CATEGORY_ROW, column=2).value = "Catégorie"

            return True

        except Exception as e:
            print(f"[template] ❌ fill error: {e}")
            import traceback
            traceback.print_exc()
            return False

    # ─────────────────────────────────────
    def save_template(self, output_path: str) -> bool:
        try:
            self.wb.save(output_path)
            print(f"[template] ✅ Saved: {output_path}")
            return True
        except Exception as e:
            print(f"[template] ❌ Save error: {e}")
            return False

    # ─────────────────────────────────────
    def close(self):
        if self.wb:
            self.wb.close()


# ─────────────────────────────────────────
# PUBLIC ENTRY POINT (called by main.py)
# ─────────────────────────────────────────

def fill_template_for_product(
    template_path: str,
    mapped_data: dict,
    product_id: int,
    output_dir: str = "./filled_templates",
) -> str | None:
    """
    Copy template, fill one product row, save, return output path.

    Args:
        template_path – base .xlsm file
        mapped_data   – dict with Octopia field keys from map_scraped_data_to_template()
        product_id    – used in output filename
        output_dir    – directory to write filled file

    Returns:
        Absolute path of saved file, or None on failure.
    """
    if not os.path.exists(template_path):
        print(f"[template] ❌ Template not found: {template_path}")
        return None

    os.makedirs(output_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"product_{product_id}_{ts}.xlsm"
    output_path = os.path.join(output_dir, filename)

    # Work on a copy so the master template is never modified
    shutil.copy2(template_path, output_path)

    filler = TemplateFiller(output_path)

    if not filler.load_template():
        filler.close()
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

    if not filler.fill_product_data(mapped_data):
        filler.close()
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

    if not filler.save_template(output_path):
        filler.close()
        return None

    filler.close()
    return output_path


# ─────────────────────────────────────────
# UTILITY HELPERS
# ─────────────────────────────────────────

def create_csv_row_from_template(mapped_data, product_id, category_id, category_name):
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
        "timestamp": datetime.now().isoformat(),
    }
    row.update(mapped_data)
    return row


def export_products_to_csv(products_list, output_path="products_export.csv"):
    try:
        import csv
        if not products_list:
            print("[template] WARNING: No products to export")
            return False
        fieldnames = list(products_list[0].keys())
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products_list)
        print(f"[template] ✅ Exported {len(products_list)} products to {output_path}")
        return True
    except Exception as e:
        print(f"[template] ❌ CSV export error: {e}")
        return False
