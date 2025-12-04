# example_datedownload_pipeline.py

"""
EXAMPLE: DateDownload-based Pipeline Template
==============================================
Use this template for data sources that don't have an edition system.
The file will be archived monthly based on download date.

To use:
1. Copy this file to pipelines/ folder
2. Rename it (e.g., my_new_pipeline.py)
3. Modify OUTPUT_FILENAME and the data download logic
4. The pipeline will be auto-discovered by main.py

Versioning: DateDownload-based (archives monthly)
"""

import io
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment


# =============================================================================
# CONFIGURATION
# =============================================================================

OUTPUT_FILENAME = "Example_data_LATEST.xlsx"


# =============================================================================
# MAIN PIPELINE FUNCTION
# =============================================================================

def run_pipeline() -> dict:
    """
    Execute the pipeline.
    
    For DateDownload-based versioning:
    - Return 'edition': None (or don't include it)
    - Include 'edition_type': 'DateDownload' in Excel metadata
    - Include 'download_date' in Excel metadata
    
    main.py will automatically use DateDownload logic when edition is None.
    """
    print(f"[Example_pipeline] Started at {datetime.now().isoformat()}")
    
    try:
        # =================================================================
        # 1. DOWNLOAD YOUR DATA HERE
        # =================================================================
        # Replace this with your actual data download logic
        # Example: requests.get(), API calls, file reading, etc.
        
        df = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=12, freq='M'),
            'value_a': np.random.randn(12) * 100,
            'value_b': np.random.randn(12) * 50,
        })
        
        print(f"[Example_pipeline] Downloaded {len(df)} rows")
        
        # =================================================================
        # 2. PROCESS DATA
        # =================================================================
        # Add your data processing logic here
        
        # =================================================================
        # 3. CREATE EXCEL FILE
        # =================================================================
        download_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            
            # Metadati sheet - IMPORTANT for DateDownload versioning!
            global_meta = pd.DataFrame([
                ("edition", ""),  # Empty or omit for DateDownload
                ("edition_type", "DateDownload"),  # <-- Tells main.py to use monthly archiving
                ("download_date", download_date),  # <-- Used for version comparison
                ("source", "Example data source"),
                ("n_rows", len(df)),
            ], columns=["chiave", "valore"])
            global_meta.to_excel(writer, sheet_name="Metadati", index=False)
            
            # Dati sheet
            df.to_excel(writer, sheet_name="Dati", index=False)
            
            # Optional: formatting
            ws = writer.sheets["Dati"]
            for i in range(1, len(df.columns) + 1):
                ws.column_dimensions[get_column_letter(i)].width = 15
        
        print(f"[Example_pipeline] Completed")
        
        return {
            'status': 'success',
            'buffer': buffer,
            'filename': OUTPUT_FILENAME,
            'edition': None,  # <-- None triggers DateDownload versioning in main.py
            'n_variables': len(df.columns),
            'n_observations': len(df),
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    result = run_pipeline()
    print(f"Result: {result['status']}")