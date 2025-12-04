# nic_ecoicop.py

"""
NIC ECOICOP Pipeline
====================
Downloads monthly consumer price indices (NIC) by ECOICOP classification (5 digits)
from ISTAT SDMX API.

Source: ISTAT - Prezzi al consumo per l'intera collettività (NIC)
Dataflow: 167_744_DF_DCSP_NIC1B2015_4
Territory: IT (Italy national)
Classification: ECOICOP 5 digits

Output: NIC_ECOICOP_LATEST.xlsx

Versioning: DateDownload-based (monthly archiving)
"""

import io
import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment


# =============================================================================
# CONFIGURATION
# =============================================================================

OUTPUT_FILENAME = "NIC_ECOICOP_LATEST.xlsx"

# API URLs
DATAFLOW_ID = "167_744_DF_DCSP_NIC1B2015_4"
BASE_URL = "https://esploradati.istat.it/SDMXWS/rest/data/"
STRUCTURE_URL = f"https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/{DATAFLOW_ID}/1.0/?detail=Full&references=Descendants"

# Source path for metadata
SOURCE_PATH = "PRICES / CONSUMER PRICES FOR THE WHOLE NATION / PREVIOUS BASES (NIC) / NIC MONTHLY FROM 2016 (BASE 2015) / ECOICOP CLASSIFICATION (5 DIGITS)"
SOURCE_PATH_IT = "PREZZI / PREZZI AL CONSUMO PER L'INTERA COLLETTIVITA / BASI PRECEDENTI (NIC) / NIC MENSILI DAL 2016 (BASE 2015) / CLASSIFICAZIONE ECOICOP (5 CIFRE)"

# Query parameters
START_PERIOD = "2016-01-01"
END_PERIOD = "2030-12-31"

VERBOSE = True


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def log(msg):
    if VERBOSE:
        print(msg)


def fetch_codelist_names(codelist_id: str) -> dict:
    """
    Fetch code names from ISTAT structure API.
    Returns dict: {code: name_en}
    """
    log(f"[NIC_ECOICOP] Fetching codelist {codelist_id}...")
    
    try:
        response = requests.get(STRUCTURE_URL, timeout=120)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        ns = {
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
        
        names = {}
        codelists = root.findall('.//structure:Codelist', ns)
        
        for cl in codelists:
            if cl.get('id') == codelist_id:
                codes = cl.findall('.//structure:Code', ns)
                for code in codes:
                    code_id = code.get('id')
                    name_elements = code.findall('.//common:Name', ns)
                    name_en = code_id  # fallback
                    for n in name_elements:
                        lang = n.get('{http://www.w3.org/XML/1998/namespace}lang')
                        if lang == 'en':
                            name_en = n.text
                            break
                        elif lang == 'it' and name_en == code_id:
                            name_en = n.text
                    names[code_id] = name_en
                break
        
        log(f"[NIC_ECOICOP] Found {len(names)} codes in {codelist_id}")
        return names
        
    except Exception as e:
        log(f"[NIC_ECOICOP] Error fetching codelist: {e}")
        return {}


def download_nic_data() -> tuple[pd.DataFrame, list]:
    """
    Download NIC ECOICOP data from ISTAT API.
    Returns tuple of (DataFrame with data, list of periods)
    """
    log("[NIC_ECOICOP] Downloading data from ISTAT...")
    
    # Use empty string to get ALL available codes
    # Format: M.IT.DATA_TYPE.MEASURE.ECOICOP
    url = f"{BASE_URL}IT1,{DATAFLOW_ID},1.0/M.IT.39.4./ALL/"
    
    params = {
        "detail": "full",
        "startPeriod": START_PERIOD,
        "endPeriod": END_PERIOD,
        "dimensionAtObservation": "TIME_PERIOD"
    }
    
    headers = {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
    
    try:
        log(f"[NIC_ECOICOP] URL: {url}")
        response = requests.get(url, params=params, headers=headers, timeout=300)
        response.raise_for_status()
    except Exception as e:
        log(f"[NIC_ECOICOP] Download error: {e}")
        return None, []
    
    log(f"[NIC_ECOICOP] Downloaded {len(response.content) / 1024 / 1024:.1f} MB")
    
    # Parse XML
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        log(f"[NIC_ECOICOP] XML parse error: {e}")
        return None, []
    
    ns = {
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
    }
    
    dataset = root.find('.//message:DataSet', ns) or root.find('.//generic:DataSet', ns) or root.find('.//DataSet')
    if dataset is None:
        log("[NIC_ECOICOP] No DataSet found")
        return None, []
    
    series_list = dataset.findall('.//generic:Series', ns) or dataset.findall('.//Series')
    log(f"[NIC_ECOICOP] Found {len(series_list)} series")
    
    if len(series_list) == 0:
        log("[NIC_ECOICOP] No series found in DataSet")
        return None, []
    
    # Extract data: {ecoicop_code: {period: value}}
    data = defaultdict(dict)
    all_periods = set()
    
    for series in series_list:
        series_key = series.find('.//generic:SeriesKey', ns) or series.find('.//SeriesKey')
        ecoicop_code = None
        
        if series_key is not None:
            values = series_key.findall('.//generic:Value', ns) or series_key.findall('.//Value')
            for v in values:
                vid = v.get('id')
                if vid in ('E_COICOP', 'ECOICOP_2015', 'ECOICOP'):
                    ecoicop_code = v.get('value')
                    break
        
        if ecoicop_code is None:
            continue
        
        obs_list = series.findall('.//generic:Obs', ns) or series.findall('.//Obs')
        for obs in obs_list:
            obs_dim = obs.find('.//generic:ObsDimension', ns) or obs.find('.//ObsDimension')
            obs_value = obs.find('.//generic:ObsValue', ns) or obs.find('.//ObsValue')
            
            if obs_dim is not None and obs_value is not None:
                period = obs_dim.get('value', '').replace('-', 'M')  # 2024-01 → 2024M01
                value = obs_value.get('value')
                try:
                    data[ecoicop_code][period] = float(value)
                    all_periods.add(period)
                except (ValueError, TypeError):
                    pass
    
    # Sort periods
    sorted_periods = sorted(all_periods)
    
    # Create DataFrame
    rows = []
    for code in data.keys():
        row = {'CODE': code}
        for period in sorted_periods:
            row[period] = data[code].get(period, None)
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    if df.empty:
        log("[NIC_ECOICOP] No data extracted")
        return None, []
    
    # Sort by CODE to maintain hierarchy
    df = df.sort_values('CODE').reset_index(drop=True)
    
    log(f"[NIC_ECOICOP] Extracted {len(df)} products, {len(sorted_periods)} periods")
    
    return df, sorted_periods


def get_hierarchy_level(code: str) -> int:
    """
    Determine hierarchy level based on code length.
    00 = level 0, 01 = level 1, 011 = level 2, etc.
    """
    if code in ['00', '00ST', 'OR0']:
        return 0
    return len(code.lstrip('0')) if code.lstrip('0') else 1


# =============================================================================
# MAIN PIPELINE FUNCTION
# =============================================================================

def run_pipeline() -> dict:
    """
    Execute the NIC ECOICOP pipeline.
    """
    log(f"[NIC_ECOICOP] Pipeline started at {datetime.now().isoformat()}")
    
    try:
        # 1. Download data
        df, periods = download_nic_data()
        
        if df is None or df.empty:
            return {'status': 'error', 'message': 'Download failed, no data received'}
        
        # 2. Fetch product names
        product_names = fetch_codelist_names('CL_COICOP_2015')
        
        # 3. Add NAME column
        df.insert(1, 'NAME', df['CODE'].map(lambda x: product_names.get(x, x)))
        
        # 4. Add LEVEL column (hierarchy)
        df.insert(2, 'LEVEL', df['CODE'].map(get_hierarchy_level))
        
        # 5. Create Excel
        log("[NIC_ECOICOP] Creating Excel file...")
        download_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            # Metadata sheet
            meta_rows = [
                ("edition", ""),
                ("edition_type", "DateDownload"),
                ("download_date", download_date),
                ("source_path", SOURCE_PATH),
                ("source_path_it", SOURCE_PATH_IT),
                ("dataflow_url", STRUCTURE_URL),
                ("measure", "Index numbers"),
                ("measure_code", "4"),
                ("frequency", "Monthly"),
                ("frequency_code", "M"),
                ("base_year", "2015"),
                ("territory", "IT (Italy)"),
                ("start_period", periods[0] if periods else ""),
                ("end_period", periods[-1] if periods else ""),
                ("n_products", len(df)),
                ("n_periods", len(periods)),
            ]
            df_meta = pd.DataFrame(meta_rows, columns=["key", "value"])
            df_meta.to_excel(writer, sheet_name="Metadata", index=False)
            
            # Data sheet
            df.to_excel(writer, sheet_name="Data", index=False)
            
            # Formatting
            ws_data = writer.sheets["Data"]
            ws_meta = writer.sheets["Metadata"]
            
            ws_data.column_dimensions['A'].width = 12
            ws_data.column_dimensions['B'].width = 50
            ws_data.column_dimensions['C'].width = 8
            for i in range(4, len(df.columns) + 1):
                ws_data.column_dimensions[get_column_letter(i)].width = 10
            
            ws_meta.column_dimensions['A'].width = 20
            ws_meta.column_dimensions['B'].width = 80
        
        log(f"[NIC_ECOICOP] Pipeline completed successfully")
        
        return {
            'status': 'success',
            'buffer': buffer,
            'filename': OUTPUT_FILENAME,
            'edition': None,
            'n_variables': len(df),
            'n_observations': len(df) * len(periods),
            'period_range': f"{periods[0]} → {periods[-1]}" if periods else None,
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    result = run_pipeline()
    print(f"\nResult: {result['status']}")
    if result['status'] == 'success':
        print(f"Products: {result['n_variables']}")
        print(f"Observations: {result['n_observations']}")
        print(f"Period: {result['period_range']}")