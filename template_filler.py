"""
template_filler.py - COMPLETE VERSION
Writes category to Row 1 AND data to Row 11+
"""

import json
import os
import shutil
from datetime import datetime
from openpyxl import load_workbook

FIELD_KEY_ROW = 5
CATEGORY_ROW  = 1
DATA_ROW      = 11


class TemplateFiller:
    def __init__(self, template_path: str):
        self.template_path = template_path
        self.wb = None
        self.ws = None
        self._field_col: dict[str, int] = {}

    def load_template(self) -> bool:
        try:
            self.wb = load_workbook(self.template_path, keep_vba=True)
            self.ws = self.wb.active
            self._build_field_map()
            print(f"[template] ✅ Loaded: {self.ws.title}")
            print(f"[template] 📋 {len(self._field_col)} fields from ROW {FIELD_KEY_ROW}")
            return True
        except Exception as e:
            print(f"[template] ❌ Load error: {e}")
            return False

    def _build_field_map(self):
        self._field_col = {}
        for col in range(1, self.ws.max_column + 1):
            val = self.ws.cell(row=FIELD_KEY_ROW, column=col).value
            if val:
                self._field_col[str(val).strip()] = col

    def _find_next_data_row(self) -> int:
        for r in range(DATA_ROW, self.ws.max_row + 2):
            if self.ws.cell(row=r, column=3).value is None:
                return r
        return self.ws.max_row + 1

    def fill_category_row(self, category_id: str, category_name: str) -> bool:
        """Write category to ROW 1"""
        try:
            print(f"\n[template] 🏷️  Writing category to ROW {CATEGORY_ROW}...")
            
            cat_id = str(category_id).strip() if category_id else "0"
            cat_name = str(category_name).strip() if category_name else "Uncategorized"
            
            self.ws.cell(row=CATEGORY_ROW, column=1).value = "OCTOPIA"
            self.ws.cell(row=CATEGORY_ROW, column=2).value = "Catégorie"
            self.ws.cell(row=CATEGORY_ROW, column=3).value = cat_id
            self.ws.cell(row=CATEGORY_ROW, column=4).value = cat_name
            
            print(f"[template]    ✅ C1: {cat_id} | D1: {cat_name}")
            return True
        except Exception as e:
            print(f"[template] ❌ Error: {e}")
            return False

    def fill_product_data(self, mapped_data: dict) -> bool:
        """Write product data to Row 11+"""
        try:
            row = self._find_next_data_row()
            filled = 0

            print(f"\n[template] 📝 Writing {len(mapped_data)} fields → ROW {row}...")

            for field_key, value in mapped_data.items():
                col = self._field_col.get(field_key)
                if col is None:
                    continue

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

            print(f"[template] ✅ {filled} fields written to ROW {row}")
            return True

        except Exception as e:
            print(f"[template] ❌ Error: {e}")
            return False

    def save_template(self, output_path: str) -> bool:
        try:
            self.wb.save(output_path)
            print(f"[template] ✅ Saved: {output_path}")
            return True
        except Exception as e:
            print(f"[template] ❌ Save error: {e}")
            return False

    def close(self):
        if self.wb:
            self.wb.close()


def fill_template_for_product(
    template_path: str,
    mapped_data: dict,
    product_id: int,
    output_dir: str = "./filled_templates",
    category_id: str = "0",
    category_name: str = "Uncategorized"
) -> str | None:
    """Fill template with category and product data"""
    
    if not os.path.exists(template_path):
        print(f"[template] ❌ Template not found")
        return None

    os.makedirs(output_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"product_{product_id}_{ts}.xlsm"
    output_path = os.path.join(output_dir, filename)

    shutil.copy2(template_path, output_path)

    filler = TemplateFiller(output_path)

    if not filler.load_template():
        filler.close()
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

    if not filler.fill_category_row(category_id, category_name):
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
