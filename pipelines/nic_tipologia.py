# nic_tipologia.py

"""
NIC Tipologia di Prodotto Pipeline
==================================
Downloads monthly consumer price indices (NIC) by product type and territory
from ISTAT SDMX API.

Source: ISTAT - Prezzi al consumo per l'intera collettività (NIC)
Dataflow: 167_744_DF_DCSP_NIC1B2015_2
Territories: Italy + macro-areas + regions
Classification: Product types (aggregations)

Output: NIC_Tipologia_prodotto_LATEST.xlsx

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

OUTPUT_FILENAME = "NIC_Tipologia_prodotto_LATEST.xlsx"

# API URLs
DATAFLOW_ID = "167_744_DF_DCSP_NIC1B2015_2"
BASE_URL = "https://esploradati.istat.it/SDMXWS/rest/data/"
STRUCTURE_URL = f"https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/{DATAFLOW_ID}/1.0/?detail=Full&references=Descendants"

# Source path for metadata
SOURCE_PATH = "PRICES / CONSUMER PRICES FOR THE WHOLE NATION / PREVIOUS BASES (NIC) / NIC MONTHLY FROM 2016 (BASE 2015) / PRODUCT TYPES"
SOURCE_PATH_IT = "PREZZI / PREZZI AL CONSUMO PER L'INTERA COLLETTIVITA / BASI PRECEDENTI (NIC) / NIC MENSILI DAL 2016 (BASE 2015) / TIPOLOGIE DI PRODOTTO"

# Query parameters
START_PERIOD = "1995-01-01"
END_PERIOD = "2030-12-31"

# Territories (from API)
# TERRITORIES = "IT+ITC+ITC1+ITC11+ITC12+ITC13+ITC14+ITC15+ITC16+ITC17+ITC18+ITC2+ITC20+ITC3+ITC31+ITC32+ITC33+ITC34+ITC4+ITC41+ITC42+ITC43+ITC44+ITC45+ITC46+ITC47+ITC48+ITC49+ITC4A+ITC4B+ITD+ITD1+ITD10+ITD2+ITD20+ITD3+ITD31+ITD32+ITD33+ITD34+ITD35+ITD36+ITD37+ITD4+ITD41+ITD42+ITD43+ITD44+ITD5+ITD51+ITD52+ITD53+ITD54+ITD55+ITD56+ITD57+ITD58+ITD59+ITDA+ITE+ITE1+ITE11+ITE12+ITE13+ITE14+ITE15+ITE16+ITE17+ITE18+ITE19+ITE1A+ITE2+ITE21+ITE22+ITE3+ITE31+ITE32+ITE33+ITE34+ITE4+ITE41+ITE42+ITE43+ITE44+ITE45+ITF+ITF1+ITF11+ITF12+ITF13+ITF14+ITF2+ITF21+ITF22+ITF3+ITF31+ITF32+ITF33+ITF34+ITF35+ITF4+ITF41+ITF42+ITF43+ITF44+ITF45+ITF5+ITF51+ITF52+ITF6+ITF61+ITF62+ITF63+ITF64+ITF65+ITG+ITG1+ITG11+ITG12+ITG13+ITG14+ITG15+ITG16+ITG17+ITG18+ITG19+ITG2+ITG25+ITG26+ITG27+ITG28+ITG29"
TERRITORIES = ""

# Product types (from API)
# PRODUCT_TYPES = "00+00XAP+00XE+00XEFOOD+00XEFOODUNP+AP+APENRGY+APGOODS+APGOODSXAPE+APSERV+ENRGY+ENRGYXAPE+FOODHPC+FOODPROCXT+FOODUNP+FOODXT+FROOPP+GOODS+GOODSXAP+IGOODSXE+IGOODSXEDU+IGOODSXEND+IGOODSXESD+LOCAPSERV+LOWFRP+MEDFRP+NATAPSERV+OR1+OR2+OR3+SERV+SERVCOMM+SERVHOUSE+SERVMISC+SERVRP+SERVTRANS+SERVXAPS+TOBAC"
PRODUCT_TYPES = ""

VERBOSE = True


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def log(msg):
    if VERBOSE:
        print(msg)


def fetch_codelists() -> tuple[dict, dict]:
    """
    Fetch territory and product type names from ISTAT structure API.
    Returns tuple of (territory_names, product_names)
    """
    log(f"[NIC_Tipologia] Fetching codelists...")
    
    try:
        response = requests.get(STRUCTURE_URL, timeout=120)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        ns = {
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
        
        territory_names = {}
        product_names = {}
        
        codelists = root.findall('.//structure:Codelist', ns)
        
        for cl in codelists:
            cl_id = cl.get('id')
            
            if cl_id == 'CL_ITTER107':
                # Territory codelist
                codes = cl.findall('.//structure:Code', ns)
                for code in codes:
                    code_id = code.get('id')
                    name_elements = code.findall('.//common:Name', ns)
                    name_en = code_id
                    for n in name_elements:
                        lang = n.get('{http://www.w3.org/XML/1998/namespace}lang')
                        if lang == 'en':
                            name_en = n.text
                            break
                        elif lang == 'it' and name_en == code_id:
                            name_en = n.text
                    territory_names[code_id] = name_en
                    
            elif cl_id == 'CL_COICOP_2015':
                # Product type codelist
                codes = cl.findall('.//structure:Code', ns)
                for code in codes:
                    code_id = code.get('id')
                    name_elements = code.findall('.//common:Name', ns)
                    name_en = code_id
                    for n in name_elements:
                        lang = n.get('{http://www.w3.org/XML/1998/namespace}lang')
                        if lang == 'en':
                            name_en = n.text
                            break
                        elif lang == 'it' and name_en == code_id:
                            name_en = n.text
                    product_names[code_id] = name_en
        
        log(f"[NIC_Tipologia] Found {len(territory_names)} territories, {len(product_names)} product types")
        return territory_names, product_names
        
    except Exception as e:
        log(f"[NIC_Tipologia] Error fetching codelists: {e}")
        return {}, {}


def download_nic_data() -> tuple[pd.DataFrame, list]:
    """
    Download NIC Tipologia data from ISTAT API.
    Returns tuple of (DataFrame with data, list of periods)
    """
    log("[NIC_Tipologia] Downloading data from ISTAT...")
    
    url = f"{BASE_URL}IT1,{DATAFLOW_ID},1.0/M.{TERRITORIES}.39.4.{PRODUCT_TYPES}/ALL/"
    
    params = {
        "detail": "full",
        "startPeriod": START_PERIOD,
        "endPeriod": END_PERIOD,
        "dimensionAtObservation": "TIME_PERIOD"
    }
    
    headers = {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=600)
        response.raise_for_status()
    except Exception as e:
        log(f"[NIC_Tipologia] Download error: {e}")
        return None, []
    
    log(f"[NIC_Tipologia] Downloaded {len(response.content) / 1024 / 1024:.1f} MB")
    
    # Parse XML
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        log(f"[NIC_Tipologia] XML parse error: {e}")
        return None, []
    
    ns = {
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
    }
    
    dataset = root.find('.//message:DataSet', ns) or root.find('.//generic:DataSet', ns) or root.find('.//DataSet')
    if dataset is None:
        log("[NIC_Tipologia] No DataSet found")
        return None, []
    
    series_list = dataset.findall('.//generic:Series', ns) or dataset.findall('.//Series')
    log(f"[NIC_Tipologia] Found {len(series_list)} series")
    
    # Extract data: {(territory, product): {period: value}}
    data = defaultdict(dict)
    all_periods = set()
    
    for series in series_list:
        series_key = series.find('.//generic:SeriesKey', ns) or series.find('.//SeriesKey')
        territory = None
        product = None
        
        if series_key is not None:
            values = series_key.findall('.//generic:Value', ns) or series_key.findall('.//Value')
            for v in values:
                vid = v.get('id')
                if vid == 'REF_AREA':
                    territory = v.get('value')
                elif vid == 'E_COICOP_REV_ISTAT':
                    product = v.get('value')
        
        if territory is None or product is None:
            continue
        
        obs_list = series.findall('.//generic:Obs', ns) or series.findall('.//Obs')
        for obs in obs_list:
            obs_dim = obs.find('.//generic:ObsDimension', ns) or obs.find('.//ObsDimension')
            obs_value = obs.find('.//generic:ObsValue', ns) or obs.find('.//ObsValue')
            
            if obs_dim is not None and obs_value is not None:
                period = obs_dim.get('value', '').replace('-', 'M')  # 2024-01 → 2024M01
                value = obs_value.get('value')
                try:
                    data[(territory, product)][period] = float(value)
                    all_periods.add(period)
                except (ValueError, TypeError):
                    pass
    
    # Sort periods
    sorted_periods = sorted(all_periods)
    
    # Create DataFrame
    rows = []
    for (territory, product) in sorted(data.keys()):
        row = {
            'TERRITORY': territory,
            'PRODUCT_TYPE': product
        }
        for period in sorted_periods:
            row[period] = data[(territory, product)].get(period, None)
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    log(f"[NIC_Tipologia] Extracted {len(df)} rows, {len(sorted_periods)} periods")
    
    return df, sorted_periods


# =============================================================================
# MAIN PIPELINE FUNCTION
# =============================================================================

def run_pipeline() -> dict:
    """
    Execute the NIC Tipologia pipeline.
    """
    log(f"[NIC_Tipologia] Pipeline started at {datetime.now().isoformat()}")
    
    try:
        # 1. Download data
        df, periods = download_nic_data()
        
        if df is None or df.empty:
            return {'status': 'error', 'message': 'Download failed, no data received'}
        
        # 2. Fetch names
        territory_names, product_names = fetch_codelists()
        
        # 3. Add name columns
        df.insert(1, 'TERRITORY_NAME', df['TERRITORY'].map(lambda x: territory_names.get(x, x)))
        df.insert(3, 'PRODUCT_NAME', df['PRODUCT_TYPE'].map(lambda x: product_names.get(x, x)))
        
        # 4. Create Excel
        log("[NIC_Tipologia] Creating Excel file...")
        download_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Count unique territories and products
        n_territories = df['TERRITORY'].nunique()
        n_products = df['PRODUCT_TYPE'].nunique()
        
        # Build data API URL for metadata
        data_api_url = f"{BASE_URL}IT1,{DATAFLOW_ID},1.0/M.{TERRITORIES[:50]}...{PRODUCT_TYPES[:50]}.../ALL/"
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            # Metadati sheet
            meta_rows = [
                ("edition", ""),
                ("edition_type", "DateDownload"),
                ("download_date", download_date),
                ("source_path", SOURCE_PATH),
                ("source_path_it", SOURCE_PATH_IT),
                ("dataflow_url", STRUCTURE_URL),
                ("data_api_url", data_api_url + " (truncated)"),
                ("measure", "Index numbers"),
                ("measure_code", "4"),
                ("frequency", "Monthly"),
                ("frequency_code", "M"),
                ("base_year", "2015"),
                ("start_period", periods[0] if periods else ""),
                ("end_period", periods[-1] if periods else ""),
                ("n_territories", n_territories),
                ("n_product_types", n_products),
                ("n_combinations", len(df)),
                ("n_periods", len(periods)),
            ]
            df_meta = pd.DataFrame(meta_rows, columns=["key", "value"])
            df_meta.to_excel(writer, sheet_name="Metadata", index=False)
            
            # Dati sheet
            df.to_excel(writer, sheet_name="Data", index=False)
            
            # Formatting
            ws_data = writer.sheets["Data"]
            ws_meta = writer.sheets["Metadata"]
            
            # Format Data sheet
            ws_data.column_dimensions['A'].width = 12  # TERRITORY
            ws_data.column_dimensions['B'].width = 40  # TERRITORY_NAME
            ws_data.column_dimensions['C'].width = 15  # PRODUCT_TYPE
            ws_data.column_dimensions['D'].width = 40  # PRODUCT_NAME
            for i in range(5, len(df.columns) + 1):
                ws_data.column_dimensions[get_column_letter(i)].width = 10
            
            # Format Metadata sheet
            ws_meta.column_dimensions['A'].width = 20
            ws_meta.column_dimensions['B'].width = 80
        
        log(f"[NIC_Tipologia] Pipeline completed successfully")
        
        return {
            'status': 'success',
            'buffer': buffer,
            'filename': OUTPUT_FILENAME,
            'edition': None,  # DateDownload versioning
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
        print(f"Combinations: {result['n_variables']}")
        print(f"Observations: {result['n_observations']}")
        print(f"Period: {result['period_range']}")