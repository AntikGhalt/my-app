# main.py
"""
ISTAT/EUROSTAT Data Pipeline - Main Application
================================================
Flask application with shared utilities for data pipelines.
Each pipeline is a separate module in the pipelines/ folder.

Author: Paolo Refuto
Last Updated: December 2025
"""
import sys
print("=" * 50, file=sys.stderr)
print("MAIN.PY: Starting imports...", file=sys.stderr)
print("=" * 50, file=sys.stderr)

import os
import io
from datetime import datetime
from flask import Flask, jsonify
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import pandas as pd

# =============================================================================
# FLASK APP INITIALIZATION
# =============================================================================
print("MAIN.PY: Creating Flask app...", file=sys.stderr)
app = Flask(__name__)
print(f"MAIN.PY: Flask app created: {app}", file=sys.stderr)
print(f"MAIN.PY: App name: {app.name}", file=sys.stderr)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Google Drive Shared Drive ID
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "0ACZ58HkBSJpjUk9PVA")

# Archive folder ID (subfolder inside DRIVE_FOLDER_ID)
ARCHIVE_FOLDER_ID = os.environ.get("ARCHIVE_FOLDER_ID", "1wT0j1Hz26TW9v891LQ2ZFSpGHwQkkAmu")

# Log file name
LOG_FILENAME = "pipeline_log.txt"

# Verbose logging
VERBOSE = os.environ.get("VERBOSE", "false").lower() == "true"


# =============================================================================
# GOOGLE DRIVE UTILITIES
# =============================================================================

def get_drive_service():
    """
    Authenticate with Google Drive using Application Default Credentials.
    Works automatically on Cloud Run with the service account.
    
    Returns:
        Google Drive API service object
    """
    credentials, project = default(
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=credentials)


def find_file_by_name(filename: str, folder_id: str) -> dict | None:
    """
    Search for a file by name in a specific folder.
    
    Args:
        filename: Name of the file to find
        folder_id: Google Drive folder ID to search in
    
    Returns:
        File metadata dict with 'id' and 'name', or None if not found
    """
    service = get_drive_service()
    
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)',
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora='drive',
        driveId=DRIVE_FOLDER_ID
    ).execute()
    
    files = results.get('files', [])
    
    if files:
        return files[0]
    return None


def get_edition_from_excel(file_id: str) -> str | None:
    """
    Download an Excel file and extract the 'edition' value from Metadati sheet.
    
    Args:
        file_id: Google Drive file ID
    
    Returns:
        Edition string (e.g., "2025M10") or None if not found
    """
    service = get_drive_service()
    
    try:
        # Download file content
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        buffer.seek(0)
        
        # Read Metadati sheet
        df_meta = pd.read_excel(buffer, sheet_name='Metadati', nrows=10)
        
        # Find edition row
        for idx, row in df_meta.iterrows():
            if row.iloc[0] == 'edition':
                return str(row.iloc[1])
        
        return None
        
    except Exception as e:
        print(f"Error reading edition from file: {e}")
        return None


def move_file_to_archive(file_id: str, filename: str, edition: str, archive_folder_id: str) -> bool:
    """
    Move a file to the archive folder with edition in the name.
    
    Args:
        file_id: Google Drive file ID
        filename: Current filename (e.g., "Reddito_disponibile_famiglie_LATEST.xlsx")
        edition: Edition string to include in archived name
        archive_folder_id: Destination folder ID
    
    Returns:
        True if successful, False otherwise
    """
    service = get_drive_service()
    
    try:
        # Create archived filename: remove _LATEST and add edition
        # "Reddito_disponibile_famiglie_LATEST.xlsx" → "Reddito_disponibile_famiglie_2025M10.xlsx"
        archived_name = filename.replace('_LATEST', f'_{edition}')
        
        # Get current parent
        file = service.files().get(
            fileId=file_id,
            fields='parents',
            supportsAllDrives=True
        ).execute()
        
        previous_parents = ",".join(file.get('parents', []))
        
        # Move file to archive and rename
        service.files().update(
            fileId=file_id,
            addParents=archive_folder_id,
            removeParents=previous_parents,
            body={'name': archived_name},
            fields='id, parents',
            supportsAllDrives=True
        ).execute()
        
        return True
        
    except Exception as e:
        print(f"Error moving file to archive: {e}")
        return False


def upload_excel_to_drive(buffer: io.BytesIO, filename: str, folder_id: str) -> tuple[str, str]:
    """
    Upload an Excel file to Google Drive.
    
    Args:
        buffer: BytesIO containing Excel file data
        filename: Name for the file in Drive
        folder_id: Google Drive folder ID
    
    Returns:
        Tuple of (file_id, web_link)
    """
    service = get_drive_service()
    
    buffer.seek(0)
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id],
        'driveId': folder_id,
        'supportsAllDrives': True
    }
    media = MediaIoBaseUpload(
        buffer, 
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, webViewLink',
        supportsAllDrives=True
    ).execute()
    
    return file.get('id'), file.get('webViewLink')


def delete_file(file_id: str) -> bool:
    """
    Delete a file from Google Drive.
    
    Args:
        file_id: Google Drive file ID
    
    Returns:
        True if successful, False otherwise
    """
    service = get_drive_service()
    
    try:
        service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        return True
    except Exception as e:
        print(f"Error deleting file: {e}")
        return False


# =============================================================================
# SMART UPLOAD FUNCTION
# =============================================================================

def smart_upload(buffer: io.BytesIO, filename: str, edition: str, 
                 folder_id: str, archive_folder_id: str) -> dict:
    """
    Smart upload that checks edition and archives old files if needed.
    
    Logic:
    1. Check if LATEST file exists
    2. If exists, compare editions:
       - Same edition → Skip upload, return "not_updated"
       - Different edition → Archive old file, upload new
    3. If not exists → Upload new file
    
    Args:
        buffer: BytesIO containing Excel file data
        filename: Name for the file (should end with _LATEST.xlsx)
        edition: Edition string for comparison
        folder_id: Main folder ID
        archive_folder_id: Archive folder ID
    
    Returns:
        Dict with status and details
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Check if file already exists
    existing_file = find_file_by_name(filename, folder_id)
    
    if existing_file:
        # File exists - check edition
        existing_edition = get_edition_from_excel(existing_file['id'])
        
        if existing_edition == edition:
            # Same edition - no update needed
            return {
                'status': 'not_updated',
                'reason': 'Edition unchanged',
                'edition': edition,
                'existing_edition': existing_edition,
                'filename': filename,
                'timestamp': timestamp
            }
        else:
            # Different edition - archive old and upload new
            print(f"Edition changed: {existing_edition} → {edition}")
            
            # Archive the old file
            archived = move_file_to_archive(
                file_id=existing_file['id'],
                filename=filename,
                edition=existing_edition or 'unknown',
                archive_folder_id=archive_folder_id
            )
            
            if not archived:
                return {
                    'status': 'error',
                    'reason': 'Failed to archive old file',
                    'timestamp': timestamp
                }
    
    # Upload new file
    file_id, web_link = upload_excel_to_drive(buffer, filename, folder_id)
    
    return {
        'status': 'updated',
        'edition': edition,
        'filename': filename,
        'file_id': file_id,
        'web_link': web_link,
        'timestamp': timestamp
    }


# =============================================================================
# LOGGING UTILITIES
# =============================================================================

def update_log(pipeline_name: str, status: str, edition: str, details: str = "") -> bool:
    """
    Update the log file in Google Drive with pipeline result.
    
    Log format (appended):
    [2025-12-03 11:34:14] Reddito_disponibile_famiglie: updated; edition: 2025M10
    [2025-12-03 11:34:14] Reddito_disponibile_famiglie: not_updated; edition: 2025M10 (unchanged)
    
    Args:
        pipeline_name: Name of the pipeline
        status: "updated", "not_updated", or "error"
        edition: Edition string
        details: Additional details
    
    Returns:
        True if successful, False otherwise
    """
    service = get_drive_service()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Build log line
    if status == "updated":
        log_line = f"[{timestamp}] {pipeline_name}: updated; edition: {edition}"
    elif status == "not_updated":
        log_line = f"[{timestamp}] {pipeline_name}: not_updated; edition: {edition} (unchanged)"
    else:
        log_line = f"[{timestamp}] {pipeline_name}: error; {details}"
    
    try:
        # Check if log file exists
        existing_log = find_file_by_name(LOG_FILENAME, DRIVE_FOLDER_ID)
        
        if existing_log:
            # Download existing content
            request = service.files().get_media(fileId=existing_log['id'], supportsAllDrives=True)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            
            done = False
            while not done:
                status_dl, done = downloader.next_chunk()
            
            buffer.seek(0)
            existing_content = buffer.read().decode('utf-8')
            
            # Append new line
            new_content = existing_content + log_line + "\n"
            
            # Update file
            media = MediaIoBaseUpload(
                io.BytesIO(new_content.encode('utf-8')),
                mimetype='text/plain'
            )
            service.files().update(
                fileId=existing_log['id'],
                media_body=media,
                supportsAllDrives=True
            ).execute()
        else:
            # Create new log file
            new_content = log_line + "\n"
            
            file_metadata = {
                'name': LOG_FILENAME,
                'parents': [DRIVE_FOLDER_ID],
                'mimeType': 'text/plain'
            }
            media = MediaIoBaseUpload(
                io.BytesIO(new_content.encode('utf-8')),
                mimetype='text/plain'
            )
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
        
        return True
        
    except Exception as e:
        print(f"Error updating log: {e}")
        return False


# =============================================================================
# FLASK ENDPOINTS
# =============================================================================

@app.route('/')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'istat-pipeline',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/test')
def test_drive():
    """Test Google Drive connection."""
    try:
        service = get_drive_service()
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents",
            pageSize=10,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora='drive',
            driveId=DRIVE_FOLDER_ID
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


# -----------------------------------------------------------------------------
# Pipeline Endpoints
# -----------------------------------------------------------------------------

@app.route('/run/reddito', methods=['GET', 'POST'])
def run_reddito():
    """
    Run the Reddito Disponibile Famiglie pipeline.
    Downloads ISTAT data and uploads to Google Drive.
    """
    try:
        # Import pipeline module
        from pipelines import istat_reddito_famiglie
        
        # Run the pipeline
        result = istat_reddito_famiglie.run_pipeline()
        
        if result['status'] == 'error':
            update_log(
                pipeline_name="Reddito_disponibile_famiglie",
                status="error",
                edition="",
                details=result.get('message', 'Unknown error')
            )
            return jsonify(result), 500
        
        # Smart upload with edition check
        upload_result = smart_upload(
            buffer=result['buffer'],
            filename=result['filename'],
            edition=result['edition'],
            folder_id=DRIVE_FOLDER_ID,
            archive_folder_id=ARCHIVE_FOLDER_ID
        )
        
        # Update log
        update_log(
            pipeline_name="Reddito_disponibile_famiglie",
            status=upload_result['status'],
            edition=result['edition']
        )
        
        # Add pipeline metadata to response
        upload_result['n_variables'] = result.get('n_variables')
        upload_result['n_observations'] = result.get('n_observations')
        upload_result['period_range'] = result.get('period_range')
        upload_result['sector'] = result.get('sector')
        
        return jsonify(upload_result), 200
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        
        update_log(
            pipeline_name="Reddito_disponibile_famiglie",
            status="error",
            edition="",
            details=str(e)
        )
        
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/run/all', methods=['GET', 'POST'])
def run_all():
    """
    Run all pipelines sequentially.
    Returns summary of all results.
    """
    results = {}
    
    # List of all pipeline endpoints to call
    pipelines = [
        ('Reddito_disponibile_famiglie', run_reddito),
        # Add more pipelines here as they are created
        # ('Another_pipeline', run_another),
    ]
    
    for name, func in pipelines:
        try:
            response = func()
            # Flask response object - get JSON data
            if hasattr(response, '__iter__') and len(response) == 2:
                data, status_code = response
                results[name] = data.get_json()
            else:
                results[name] = {'status': 'unknown'}
        except Exception as e:
            results[name] = {'status': 'error', 'message': str(e)}
    
    return jsonify({
        'status': 'completed',
        'results': results,
        'timestamp': datetime.now().isoformat()
    })


# =============================================================================
# BACKWARD COMPATIBILITY - Keep /run endpoint working
# =============================================================================

@app.route('/run', methods=['GET', 'POST'])
def run_legacy():
    """
    Legacy endpoint - redirects to /run/reddito for backward compatibility.
    """
    return run_reddito()


# =============================================================================
# LOCAL EXECUTION
# =============================================================================

# Debug: Print registered routes
print("MAIN.PY: Routes registered:", file=sys.stderr)
for rule in app.url_map.iter_rules():
    print(f"  {rule.rule} -> {rule.endpoint}", file=sys.stderr)
print("MAIN.PY: App ready!", file=sys.stderr)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)
