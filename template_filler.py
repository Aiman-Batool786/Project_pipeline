"""
Template Filler: Fills Excel XLSM templates with mapped product data
"""

from openpyxl import load_workbook
from datetime import datetime
import json


class TemplateFiller:
    """Handles filling and saving Excel templates with product data"""
    
    def __init__(self, template_path):
        """
        Initialize with template file
        
        Args:
            template_path: Path to the XLSM template file
        """
        self.template_path = template_path
        self.wb = None
        self.ws = None
    
    def load_template(self):
        """Load the Excel template"""
        try:
            self.wb = load_workbook(self.template_path)
            # Get first sheet (assumes category-specific sheet)
            self.ws = self.wb.active
            print(f"[template] ✅ Loaded template: {self.ws.title}")
            return True
        except Exception as e:
            print(f"[template] ERROR loading template: {e}")
            return False
    
    def get_header_row(self, header_row_num=4):
        """Get the header row with column names"""
        headers = {}
        
        for col in range(1, self.ws.max_column + 1):
            cell_value = self.ws.cell(row=header_row_num, column=col).value
            if cell_value:
                headers[cell_value] = col
        
        return headers
    
    def fill_product_data(self, mapped_data, data_row_num=11):
        """
        Fill product data into the template
        
        Args:
            mapped_data: Dict with template fields as keys
            data_row_num: Row number to insert data (default: 11)
        """
        
        try:
            headers = self.get_header_row()
            
            for field_name, value in mapped_data.items():
                if field_name in headers:
                    col = headers[field_name]
                    cell = self.ws.cell(row=data_row_num, column=col)
                    
                    # Handle different data types
                    if isinstance(value, list):
                        cell.value = " | ".join(str(v) for v in value)
                    elif isinstance(value, dict):
                        cell.value = json.dumps(value, ensure_ascii=False)
                    else:
                        cell.value = value
            
            print(f"[template] ✅ Filled product data in row {data_row_num}")
            return True
            
        except Exception as e:
            print(f"[template] ERROR filling data: {e}")
            return False
    
    def save_template(self, output_path):
        """
        Save the filled template to a new file
        
        Args:
            output_path: Path where to save the filled template
        """
        
        try:
            self.wb.save(output_path)
            print(f"[template] ✅ Saved filled template: {output_path}")
            return True
        except Exception as e:
            print(f"[template] ERROR saving template: {e}")
            return False
    
    def close(self):
        """Close the workbook"""
        if self.wb:
            self.wb.close()


def fill_template_for_product(template_path, mapped_data, product_id, output_dir="./filled_templates"):
    """
    Main function to fill a template with product data
    
    Args:
        template_path: Path to the XLSM template
        mapped_data: Dict with mapped product attributes
        product_id: Product ID for naming
        output_dir: Directory to save filled templates
        
    Returns:
        Path to saved template or None on error
    """
    
    import os
    
    # Create output directory if needed
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Initialize filler
    filler = TemplateFiller(template_path)
    
    if not filler.load_template():
        return None
    
    # Fill data (uses row 11 by default, or row 12 for second product, etc.)
    if not filler.fill_product_data(mapped_data, data_row_num=11):
        filler.close()
        return None
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"product_{product_id}_{timestamp}.xlsm"
    output_path = os.path.join(output_dir, output_filename)
    
    # Save template
    if not filler.save_template(output_path):
        filler.close()
        return None
    
    filler.close()
    return output_path


def create_csv_row_from_template(mapped_data, product_id, category_id, category_name):
    """
    Create a CSV row from mapped data for bulk export
    
    Args:
        mapped_data: Dict with mapped product attributes
        product_id: Product ID
        category_id: Octopia category ID
        category_name: Octopia category name
        
    Returns:
        Dict representing a CSV row
    """
    
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
        "timestamp": datetime.now().isoformat(),
    }
    
    row.update(mapped_data)
    
    return row


def export_products_to_csv(products_list, output_path="products_export.csv"):
    """
    Export multiple products to CSV
    
    Args:
        products_list: List of product dicts
        output_path: Path to save CSV
        
    Returns:
        True if successful
    """
    
    try:
        import csv
        
        if not products_list:
            print("[template] WARNING: No products to export")
            return False
        
        # Get all possible field names from first product
        fieldnames = list(products_list[0].keys())
        
        # Write CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products_list)
        
        print(f"[template] ✅ Exported {len(products_list)} products to {output_path}")
        return True
        
    except Exception as e:
        print(f"[template] ERROR exporting to CSV: {e}")
        return False
