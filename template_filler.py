from openpyxl import load_workbook
from datetime import datetime
import json


class TemplateFiller:
    """Handles filling and saving Excel templates with product data"""
    
    def __init__(self, template_path):
        """Initialize with template file"""
        self.template_path = template_path
        self.wb = None
        self.ws = None
        self.header_row = 4  # CONFIRMED: Headers are in row 4
        self.data_row = 11   # CONFIRMED: Data goes in row 11
    
    def load_template(self):
        """Load the Excel template"""
        try:
            self.wb = load_workbook(self.template_path)
            self.ws = self.wb.active
            print(f"\n[template] ✅ Loaded: {self.ws.title}")
            print(f"[template] 📊 Size: {self.ws.max_row}R × {self.ws.max_column}C")
            print(f"[template] 📋 Headers: ROW {self.header_row}")
            print(f"[template] 📝 Data row: ROW {self.data_row}")
            return True
        except Exception as e:
            print(f"[template] ❌ ERROR: {e}")
            return False
    
    def get_header_row(self):
        """Get all 73 column headers from row 4"""
        headers = {}
        
        for col in range(1, self.ws.max_column + 1):
            cell_value = self.ws.cell(row=self.header_row, column=col).value
            if cell_value:
                header_name = str(cell_value).strip()
                headers[header_name] = col
        
        print(f"\n[template] 📋 Found {len(headers)} headers in ROW {self.header_row}")
        
        return headers
    
    def fill_product_data(self, mapped_data):
        """Fill product data into ROW 11"""
        try:
            headers = self.get_header_row()
            
            if not headers:
                print(f"[template] ❌ No headers found!")
                return False
            
            filled_count = 0
            missing_fields = []
            
            print(f"\n[template] 🔄 Filling {len(mapped_data)} fields into ROW {self.data_row}...\n")
            
            for field_name, value in mapped_data.items():
                if field_name in headers:
                    col = headers[field_name]
                    cell = self.ws.cell(row=self.data_row, column=col)
                    
                    # Handle different data types
                    if isinstance(value, list):
                        cell.value = " | ".join(str(v) for v in value if v)
                    elif isinstance(value, dict):
                        cell.value = json.dumps(value, ensure_ascii=False)
                    else:
                        cell.value = value
                    
                    filled_count += 1
                    val_preview = str(value)[:40] if not isinstance(value, list) else f"{len(value)} items"
                    print(f"[template] ✅ Col {col:2d}: {field_name:45s} = {val_preview}")
                else:
                    missing_fields.append(field_name)
            
            print(f"\n[template] ✅ FILLED {filled_count}/{len(mapped_data)} fields")
            
            if missing_fields:
                print(f"[template] ⚠️  {len(missing_fields)} fields NOT in template")
            
            return True
            
        except Exception as e:
            print(f"[template] ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def save_template(self, output_path):
        """Save the filled template"""
        try:
            self.wb.save(output_path)
            print(f"\n[template] ✅ Saved: {output_path}\n")
            return True
        except Exception as e:
            print(f"[template] ❌ ERROR: {e}")
            return False
    
    def close(self):
        """Close the workbook"""
        if self.wb:
            self.wb.close()


def fill_template_for_product(template_path, mapped_data, product_id, output_dir="./filled_templates"):
    """Main function to fill a template with product data"""
    
    import os
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"\n{'='*80}")
    print(f"[template] 🎯 PRODUCT {product_id}")
    print(f"[template] 📦 {len(mapped_data)} fields ready")
    print(f"{'='*80}")
    
    filler = TemplateFiller(template_path)
    
    if not filler.load_template():
        return None
    
    if not filler.fill_product_data(mapped_data):
        filler.close()
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"product_{product_id}_{timestamp}.xlsm"
    output_path = os.path.join(output_dir, output_filename)
    
    if not filler.save_template(output_path):
        filler.close()
        return None
    
    filler.close()
    return output_path


def create_csv_row_from_template(mapped_data, product_id, category_id, category_name):
    """Create a CSV row from mapped data"""
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
        "timestamp": datetime.now().isoformat(),
    }
    row.update(mapped_data)
    return row


def export_products_to_csv(products_list, output_path="products_export.csv"):
    """Export multiple products to CSV"""
    try:
        import csv
        
        if not products_list:
            print("[template] WARNING: No products to export")
            return False
        
        fieldnames = list(products_list[0].keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products_list)
        
        print(f"[template] ✅ Exported {len(products_list)} products")
        return True
        
    except Exception as e:
        print(f"[template] ❌ ERROR: {e}")
        return False
