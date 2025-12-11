# main.py
"""
ISTAT/EUROSTAT Data Pipeline - Main Application
================================================
Flask application with AUTO-DISCOVERY of pipelines.
Just add a new file in pipelines/ folder and it will be available automatically.

Versioning Logic:
- If pipeline returns 'edition' → uses Edition-based versioning
- If pipeline returns no edition → uses DateDownload-based versioning (monthly)
- Archive naming: FILE_2025M10_Edition.xlsx or FILE_2025M11_DateDownload.xlsx

Output Folders:
- Pipelines can specify custom output folders via 'folder_id' in return dict
- If not specified, files go to main DRIVE_FOLDER_ID
- Log file always stays in main folder
- Archive always stays in ARCHIVE_FOLDER_ID

Author: Paolo Refuto
Last Updated: December 2025
"""
import os
import io
import importlib
import pkgutil
from datetime import datetime
from flask import Flask, jsonify
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import pandas as pd

# =============================================================================
# FLASK APP INITIALIZATION
# =============================================================================
app = Flask(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "0ACZ58HkBSJpjUk9PVA")
ARCHIVE_FOLDER_ID = os.environ.get("ARCHIVE_FOLDER_ID", "1wT0j1Hz26TW9v891LQ2ZFSpGHwQkkAmu")
LOG_FILENAME = "pipeline_log.txt"

# Subfolder IDs (can be overridden by environment variables)
# These are the IDs of subfolders within DATABASE3
SUBFOLDER_IDS = {
    'Dati_trimestrali': os.environ.get("FOLDER_DATI_TRIMESTRALI", ""),
    'Dati_mensili': os.environ.get("FOLDER_DATI_MENSILI", ""),
    'Dati_annuali': os.environ.get("FOLDER_DATI_ANNUALI", ""),
}

# =============================================================================
# GOOGLE DRIVE UTILITIES
# =============================================================================

def get_drive_service():
    credentials, project = default(scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=credentials)


def find_file_by_name(filename: str, folder_id: str) -> dict | None:
    """Find a file by name in a specific folder."""
    service = get_drive_service()
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, spaces='drive', fields='files(id, name)',
        supportsAllDrives=True, includeItemsFromAllDrives=True,
        corpora='drive', driveId=DRIVE_FOLDER_ID
    ).execute()
    files = results.get('files', [])
    return files[0] if files else None


def get_metadata_from_excel(file_id: str) -> dict:
    """
    Download an Excel file and extract metadata from Metadati sheet.
    Returns dict with 'edition', 'edition_type', and 'download_date'.
    """
    service = get_drive_service()
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)
        
        df_meta = pd.read_excel(buffer, sheet_name='Metadati', nrows=15)
        
        metadata = {
            'edition': None,
            'edition_type': None,
            'download_date': None
        }
        
        for idx, row in df_meta.iterrows():
            key = str(row.iloc[0]).lower().strip()
            value = row.iloc[1]
            if key == 'edition':
                metadata['edition'] = str(value) if pd.notna(value) else None
            elif key == 'edition_type':
                metadata['edition_type'] = str(value) if pd.notna(value) else None
            elif key == 'download_date':
                metadata['download_date'] = str(value) if pd.notna(value) else None
        
        return metadata
        
    except Exception as e:
        print(f"Error reading metadata from file: {e}")
        return {'edition': None, 'edition_type': None, 'download_date': None}


def move_file_to_archive(file_id: str, filename: str, version_suffix: str, archive_folder_id: str) -> bool:
    """
    Move a file to the archive folder with version suffix in the name.
    Archive always stays in the main ARCHIVE_FOLDER_ID regardless of source folder.
    """
    service = get_drive_service()
    try:
        # Create archived filename: "FILE_LATEST.xlsx" → "FILE_2025M10_Edition.xlsx"
        archived_name = filename.replace('_LATEST', f'_{version_suffix}')
        
        file = service.files().get(fileId=file_id, fields='parents', supportsAllDrives=True).execute()
        previous_parents = ",".join(file.get('parents', []))
        
        service.files().update(
            fileId=file_id, addParents=archive_folder_id, removeParents=previous_parents,
            body={'name': archived_name}, fields='id, parents', supportsAllDrives=True
        ).execute()
        
        print(f"Archived: {filename} → {archived_name}")
        return True
        
    except Exception as e:
        print(f"Error archiving: {e}")
        return False


def upload_excel_to_drive(buffer: io.BytesIO, filename: str, folder_id: str) -> tuple[str, str]:
    """Upload Excel file to specified folder."""
    service = get_drive_service()
    buffer.seek(0)
    file_metadata = {
        'name': filename, 
        'parents': [folder_id], 
        'driveId': DRIVE_FOLDER_ID,  # Always use main drive ID for shared drive
        'supportsAllDrives': True
    }
    media = MediaIoBaseUpload(buffer, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    file = service.files().create(
        body=file_metadata, 
        media_body=media, 
        fields='id, webViewLink', 
        supportsAllDrives=True
    ).execute()
    return file.get('id'), file.get('webViewLink')


def smart_upload(buffer: io.BytesIO, filename: str, edition: str | None, 
                 folder_id: str, archive_folder_id: str) -> dict:
    """
    Smart upload with Edition or DateDownload versioning.
    
    Args:
        buffer: Excel file content
        filename: Output filename
        edition: Edition string or None for DateDownload-based
        folder_id: Target folder for output (can be subfolder)
        archive_folder_id: Archive folder (always main archive)
    
    Logic:
    1. Determine version type:
       - If edition is provided and not empty → Edition-based
       - If edition is None/empty → DateDownload-based (monthly)
    
    2. Check existing file:
       - Edition: compare edition strings, archive if different
       - DateDownload: compare year-month, archive if different month
    
    3. Fallback: if metadata extraction fails → archive with timestamp, log error
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_month = datetime.now().strftime("%YM%m")  # e.g., "2025M12"
    
    # Determine version type for new file
    has_edition = edition is not None and str(edition).strip() != ''
    new_version_type = 'Edition' if has_edition else 'DateDownload'
    new_version_value = edition if has_edition else current_month
    new_version_suffix = f"{new_version_value}_{new_version_type}"
    
    print(f"[smart_upload] New file version: {new_version_suffix}")
    print(f"[smart_upload] Target folder: {folder_id}")
    
    # Check if file already exists in target folder
    existing_file = find_file_by_name(filename, folder_id)
    
    if existing_file:
        print(f"[smart_upload] Found existing file: {existing_file['name']}")
        
        # Get metadata from existing file
        existing_meta = get_metadata_from_excel(existing_file['id'])
        existing_edition = existing_meta.get('edition')
        existing_edition_type = existing_meta.get('edition_type')
        existing_download_date = existing_meta.get('download_date')
        
        print(f"[smart_upload] Existing metadata: edition={existing_edition}, type={existing_edition_type}, download_date={existing_download_date}")
        
        # Determine existing version info
        if existing_edition_type == 'Edition' and existing_edition:
            existing_version_suffix = f"{existing_edition}_Edition"
            should_archive = (existing_edition != new_version_value) if has_edition else True
            
        elif existing_edition_type == 'DateDownload':
            # Extract month from download_date for comparison
            if existing_download_date:
                try:
                    # Parse download_date and extract YYYYMM
                    dt = pd.to_datetime(existing_download_date)
                    existing_month = dt.strftime("%YM%m")
                    existing_version_suffix = f"{existing_month}_DateDownload"
                    # Archive only if month changed
                    should_archive = (existing_month != current_month)
                except:
                    existing_version_suffix = f"{existing_download_date[:7].replace('-', 'M')}_DateDownload"
                    should_archive = True
            else:
                existing_version_suffix = "unknown_DateDownload"
                should_archive = True
                
        elif existing_edition:
            # Legacy: has edition but no edition_type → assume Edition
            existing_version_suffix = f"{existing_edition}_Edition"
            should_archive = (existing_edition != new_version_value) if has_edition else True
            
        else:
            # No metadata found → fallback to timestamp, log error
            fallback_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            existing_version_suffix = f"{fallback_timestamp}_ErrorNoMetadata"
            should_archive = True
            print(f"[smart_upload] WARNING: No edition or download_date found in existing file!")
        
        print(f"[smart_upload] Existing version: {existing_version_suffix}, should_archive: {should_archive}")
        
        if not should_archive:
            return {
                'status': 'not_updated',
                'reason': 'Version unchanged',
                'version_type': new_version_type,
                'version_value': new_version_value,
                'filename': filename,
                'timestamp': timestamp
            }
        
        # Archive the old file (always to main archive folder)
        archived = move_file_to_archive(
            file_id=existing_file['id'],
            filename=filename,
            version_suffix=existing_version_suffix,
            archive_folder_id=archive_folder_id
        )
        
        if not archived:
            return {
                'status': 'error',
                'reason': 'Failed to archive old file',
                'timestamp': timestamp
            }
    
    # Upload new file to target folder
    file_id, web_link = upload_excel_to_drive(buffer, filename, folder_id)
    
    return {
        'status': 'updated',
        'version_type': new_version_type,
        'version_value': new_version_value,
        'filename': filename,
        'file_id': file_id,
        'web_link': web_link,
        'folder_id': folder_id,
        'timestamp': timestamp
    }


def update_log(pipeline_name: str, status: str, version_info: str, details: str = "") -> bool:
    """
    Update the log file in Google Drive with pipeline result.
    Log always stays in main DRIVE_FOLDER_ID.
    """
    service = get_drive_service()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if status == "updated":
        log_line = f"[{timestamp}] {pipeline_name}: updated; version: {version_info}"
    elif status == "not_updated":
        log_line = f"[{timestamp}] {pipeline_name}: not_updated; version: {version_info} (unchanged)"
    else:
        log_line = f"[{timestamp}] {pipeline_name}: error; {details}"
    
    try:
        existing_log = find_file_by_name(LOG_FILENAME, DRIVE_FOLDER_ID)
        
        if existing_log:
            request = service.files().get_media(fileId=existing_log['id'], supportsAllDrives=True)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buffer.seek(0)
            existing_content = buffer.read().decode('utf-8')
            new_content = existing_content + log_line + "\n"
            media = MediaIoBaseUpload(io.BytesIO(new_content.encode('utf-8')), mimetype='text/plain')
            service.files().update(fileId=existing_log['id'], media_body=media, supportsAllDrives=True).execute()
        else:
            file_metadata = {'name': LOG_FILENAME, 'parents': [DRIVE_FOLDER_ID], 'mimeType': 'text/plain'}
            media = MediaIoBaseUpload(io.BytesIO((log_line + "\n").encode('utf-8')), mimetype='text/plain')
            service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
        
        return True
        
    except Exception as e:
        print(f"Error updating log: {e}")
        return False


# =============================================================================
# AUTO-DISCOVERY OF PIPELINES
# =============================================================================

def discover_pipelines() -> dict:
    """
    Automatically discover all pipeline modules in the pipelines/ folder.
    Each module must have a run_pipeline() function.
    """
    pipelines = {}
    try:
        import pipelines as pipelines_pkg
        for importer, modname, ispkg in pkgutil.iter_modules(pipelines_pkg.__path__):
            if not ispkg and not modname.startswith('_'):
                try:
                    module = importlib.import_module(f'pipelines.{modname}')
                    if hasattr(module, 'run_pipeline'):
                        pipelines[modname] = module
                        print(f"[Discovery] Found pipeline: {modname}")
                except Exception as e:
                    print(f"[Discovery] Error loading {modname}: {e}")
    except Exception as e:
        print(f"[Discovery] Error scanning pipelines folder: {e}")
    return pipelines


def run_single_pipeline(pipeline_name: str, module) -> dict:
    """Execute a single pipeline and handle upload."""
    try:
        result = module.run_pipeline()
        
        if result['status'] == 'error':
            update_log(pipeline_name, "error", "", result.get('message', ''))
            return result
        
        # Get edition (may be None for DateDownload pipelines)
        edition = result.get('edition')
        
        # Get custom folder_id if specified, otherwise use main folder
        output_folder_id = result.get('folder_id', DRIVE_FOLDER_ID)
        
        # Validate folder_id - if empty string or placeholder, use main folder
        if not output_folder_id or output_folder_id.startswith('YOUR_'):
            print(f"[{pipeline_name}] No valid folder_id specified, using main folder")
            output_folder_id = DRIVE_FOLDER_ID
        
        upload_result = smart_upload(
            buffer=result['buffer'],
            filename=result['filename'],
            edition=edition,
            folder_id=output_folder_id,
            archive_folder_id=ARCHIVE_FOLDER_ID
        )
        
        # Build version info for logging
        version_info = f"{upload_result.get('version_value', 'unknown')}_{upload_result.get('version_type', 'unknown')}"
        update_log(pipeline_name, upload_result['status'], version_info)
        
        # Add pipeline metadata to response
        upload_result['n_variables'] = result.get('n_variables')
        upload_result['n_observations'] = result.get('n_observations')
        upload_result['period_range'] = result.get('period_range')
        upload_result['sector'] = result.get('sector')
        
        return upload_result
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        update_log(pipeline_name, "error", "", str(e))
        return {'status': 'error', 'message': str(e)}


# =============================================================================
# FLASK ENDPOINTS
# =============================================================================

@app.route('/')
def health_check():
    return jsonify({
        'status': 'healthy', 
        'service': 'istat-pipeline', 
        'timestamp': datetime.now().isoformat()
    })


@app.route('/test')
def test_drive():
    try:
        service = get_drive_service()
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents", pageSize=10, fields="files(id, name)",
            supportsAllDrives=True, includeItemsFromAllDrives=True, corpora='drive', driveId=DRIVE_FOLDER_ID
        ).execute()
        files = results.get('files', [])
        return jsonify({
            'status': 'success', 
            'message': 'Drive connection working',
            'folder_id': DRIVE_FOLDER_ID, 
            'archive_folder_id': ARCHIVE_FOLDER_ID,
            'files_in_folder': len(files), 
            'sample_files': [f['name'] for f in files],
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e), 
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/pipelines')
def list_pipelines():
    """List all available pipelines."""
    pipelines = discover_pipelines()
    return jsonify({
        'status': 'success',
        'available_pipelines': list(pipelines.keys()),
        'endpoints': [f'/run/{name}' for name in pipelines.keys()],
        'timestamp': datetime.now().isoformat()
    })


@app.route('/run/<pipeline_name>', methods=['GET', 'POST'])
def run_pipeline_by_name(pipeline_name: str):
    """Run any pipeline by name. Example: /run/istat_reddito_famiglie"""
    pipelines = discover_pipelines()
    
    if pipeline_name not in pipelines:
        return jsonify({
            'status': 'error',
            'message': f'Pipeline "{pipeline_name}" not found',
            'available_pipelines': list(pipelines.keys())
        }), 404
    
    result = run_single_pipeline(pipeline_name, pipelines[pipeline_name])
    status_code = 200 if result.get('status') != 'error' else 500
    return jsonify(result), status_code


@app.route('/run', methods=['GET', 'POST'])
def run_default():
    """Run the default pipeline (istat_reddito_famiglie) for backward compatibility."""
    return run_pipeline_by_name('istat_reddito_famiglie')


@app.route('/run/all', methods=['GET', 'POST'])
def run_all():
    """Run all discovered pipelines sequentially."""
    pipelines = discover_pipelines()
    results = {}
    
    for name, module in pipelines.items():
        print(f"[run/all] Running pipeline: {name}")
        results[name] = run_single_pipeline(name, module)
    
    return jsonify({
        'status': 'completed',
        'pipelines_run': len(results),
        'results': results,
        'timestamp': datetime.now().isoformat()
    })


@app.errorhandler(404)
def handle_not_found(error):
    pipelines = discover_pipelines()
    return jsonify({
        'status': 'error', 
        'message': 'Route not found',
        'available_routes': ['/', '/test', '/pipelines', '/run', '/run/all'] + [f'/run/{p}' for p in pipelines.keys()],
        'timestamp': datetime.now().isoformat()
    }), 404


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)