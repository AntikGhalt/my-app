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
START_PERIOD = "1995-01-01"
END_PERIOD = "2030-12-31"

# All ECOICOP codes from the API query
# ECOICOP_CODES = "00+00ST+01+011+0111+01111+011110+01112+011120+01113+011131+011132+01114+011141+011142+011143+01115+011150+01116+011161+011162+01117+011170+01118+011180+0112+01121+011211+011212+01122+011220+01123+011230+01124+011240+01125+011250+01126+011260+01127+011271+011272+01128+011281+011282+0113+01131+011310+01132+011320+01133+011331+011332+01134+011340+01135+011350+01136+011360+0114+01141+011410+01142+011420+01143+011430+01144+011440+01145+011451+011452+011453+01146+011460+01147+011470+0115+01151+011510+01152+011520+01153+011530+01154+011540+0116+01161+011611+011612+011613+011614+011615+011616+011617+011618+011619+01163+011630+01164+011640+0117+01171+011711+011712+011713+011714+011715+011716+01172+011720+01173+011731+011732+01174+011740+01175+011750+0118+01181+011810+01182+011820+01183+011830+01184+011840+01185+011850+01186+011860+0119+01191+011910+01192+011920+01193+011930+01194+011940+01199+011990+012+0121+01211+012110+01212+012120+01213+012130+0122+01221+012210+01222+012221+012222+01223+012230+02+021+0211+02111+021111+021112+02112+021120+0212+02121+021211+021212+021213+02123+021231+0213+02131+021310+02133+021330+022+0220+02201+022010+02202+022020+02203+022030+03+031+0312+03121+031211+031212+031213+031214+031215+031216+03122+031221+031222+031223+031224+031225+031226+03123+031231+031232+031233+031234+031235+0313+03131+031310+0314+03141+031410+03142+031420+032+0321+03211+032110+03212+032120+03213+032130+0322+03220+032200+04+041+0411+04110+041100+043+0431+04310+043100+0432+04321+043210+04322+043220+04323+043230+04324+043240+04325+043250+044+0441+04410+044100+0442+04420+044200+0443+04430+044300+0444+04441+044410+045+0451+04510+045100+045101+045102+045103+0452+04521+045210+045211+045212+04522+045220+0453+04530+045300+0454+04549+045490+05+051+0511+05111+051111+051112+051113+051114+05112+051120+05113+051130+05119+051190+0512+05121+051210+0513+05130+051300+052+0520+05201+052010+05202+052022+052023+05203+052031+052032+053+0531+05311+053110+05312+053120+05313+053130+05314+053140+05315+053150+0532+05321+053210+05322+053220+05323+053230+05329+053290+0533+05330+053300+054+0540+05401+054011+054012+05402+054020+05403+054030+055+0551+05511+055110+0552+05521+055210+05522+055222+056+0561+05611+056111+05612+056120+0562+05621+056211+05622+056220+06+061+0611+06110+061100+0612+06121+061210+06129+061290+0613+06131+061310+06132+061320+06139+061390+062+0621+06212+062120+0622+06220+062200+0623+06231+062311+062312+06232+062320+06239+062390+063+0630+06300+063000+07+071+0711+07111+071111+071112+071113+07112+071120+071121+071122+071123+0712+07120+071201+071202+0713+07130+071300+072+0721+07211+072111+072112+07212+072121+0722+07221+072210+07222+072220+07223+072230+07224+072240+0723+07230+072300+0724+07241+072411+072412+07242+072421+072422+07243+072430+073+0731+07311+073110+0732+07321+073211+073212+07322+073220+0733+07331+073310+07332+073321+073322+0734+07341+073411+07342+073421+0735+07350+073500+0736+07362+073620+08+081+0810+08101+081010+08109+081090+082+0820+08201+082010+08202+082020+08204+082040+083+0830+08301+083010+08302+083020+08303+083030+08304+083040+09+091+0911+09111+091110+09112+091120+09119+091191+0912+09121+091210+0913+09131+091311+091312+09132+091320+0914+09141+091410+09142+091420+09149+091490+092+0921+09211+092110+09213+092130+0922+09221+092210+093+0931+09311+093111+093112+09312+093120+0932+09321+093210+0933+09331+093310+09332+093321+093322+0934+09342+093421+093422+0935+09350+093501+093502+094+0941+09411+094111+094112+09412+094120+0942+09421+094211+094212+09422+094221+094222+09423+094230+09425+094250+09429+094290+0943+09430+094300+095+0951+09511+095110+09512+095120+09513+095130+09514+095141+0952+09521+095210+09522+095220+0954+09541+095410+09549+095490+096+0960+09601+096010+09602+096020+10+101+1010+10101+101010+10102+101020+102+1020+10200+102000+104+1040+10400+104000+105+1050+10500+105000+11+111+1111+11111+111111+111112+111113+111114+111115+11112+111121+111122+111123+111124+1112+11120+111201+111202+112+1120+11201+112011+112012+11202+112020+11203+112030+12+121+1211+12111+121110+12112+121120+12113+121130+1212+12121+121210+1213+12131+121310+12132+121321+121322+123+1231+12311+123110+12312+123120+1232+12321+123210+12322+123220+12329+123290+124+1240+12401+124010+12402+124020+12403+124030+125+1252+12520+125200+1253+12532+125320+1254+12541+125410+126+1262+12621+126210+127+1270+12701+127011+12702+127020+12703+127030+12704+127040+OR0"
ECOICOP_CODES = ""

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
    
    url = f"{BASE_URL}IT1,{DATAFLOW_ID},1.0/M.IT.{ECOICOP_CODES}.39.4/ALL/"
    
    params = {
        "detail": "full",
        "startPeriod": START_PERIOD,
        "endPeriod": END_PERIOD,
        "dimensionAtObservation": "TIME_PERIOD"
    }
    
    headers = {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
    
    try:
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
    
    # Extract data: {ecoicop_code: {period: value}}
    data = defaultdict(dict)
    all_periods = set()
    
    for series in series_list:
        series_key = series.find('.//generic:SeriesKey', ns) or series.find('.//SeriesKey')
        ecoicop_code = None
        
        if series_key is not None:
            values = series_key.findall('.//generic:Value', ns) or series_key.findall('.//Value')
            for v in values:
                if v.get('id') == 'E_COICOP':
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
    # Remove leading zeros for counting
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
        
        # Build data API URL for metadata
        data_api_url = f"{BASE_URL}IT1,{DATAFLOW_ID},1.0/M.IT.{ECOICOP_CODES}.39.4/ALL/?detail=full&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}&dimensionAtObservation=TIME_PERIOD"
        
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
                ("data_api_url", data_api_url[:500] + "..."),  # Truncate long URL
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
            
            # Dati sheet
            df.to_excel(writer, sheet_name="Data", index=False)
            
            # Formatting
            ws_data = writer.sheets["Data"]
            ws_meta = writer.sheets["Metadata"]
            
            # Format Data sheet
            ws_data.column_dimensions['A'].width = 12  # CODE
            ws_data.column_dimensions['B'].width = 50  # NAME
            ws_data.column_dimensions['C'].width = 8   # LEVEL
            for i in range(4, len(df.columns) + 1):
                ws_data.column_dimensions[get_column_letter(i)].width = 10
            
            # Format Metadata sheet
            ws_meta.column_dimensions['A'].width = 20
            ws_meta.column_dimensions['B'].width = 80
        
        log(f"[NIC_ECOICOP] Pipeline completed successfully")
        
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
        print(f"Products: {result['n_variables']}")
        print(f"Observations: {result['n_observations']}")
        print(f"Period: {result['period_range']}")