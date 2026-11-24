"""Google Drive sink (shared drives included).

Credentials come from Application Default Credentials: the service account
of the Cloud Run job in production, `gcloud auth application-default login`
on a laptop. The account needs Content manager on the target folder.
"""

from __future__ import annotations

import io
import mimetypes
from posixpath import basename, dirname

FOLDER_MIME = "application/vnd.google-apps.folder"


def _quote(name: str) -> str:
    return name.replace("\\", "\\\\").replace("'", "\\'")


class DriveSink:
    def __init__(self, root_folder_id: str, service=None):
        if service is None:
            import google.auth
            from googleapiclient.discovery import build

            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
        self.files = service.files()
        self.root = root_folder_id
        self._folders: dict[str, str] = {"": root_folder_id}

    def describe(self) -> str:
        return f"gdrive:{self.root}"

    # -- lookups ----------------------------------------------------------

    def _find(self, parent: str, name: str, *, folder: bool) -> str | None:
        q = f"name = '{_quote(name)}' and '{parent}' in parents and trashed = false"
        q += f" and mimeType {'=' if folder else '!='} '{FOLDER_MIME}'"
        res = self.files.list(
            q=q,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=2,
        ).execute()
        found = res.get("files", [])
        return found[0]["id"] if found else None

    def _folder(self, path: str, create: bool) -> str | None:
        if path in self._folders:
            return self._folders[path]
        parent = self._folder(dirname(path), create)
        if parent is None:
            return None
        fid = self._find(parent, basename(path), folder=True)
        if fid is None and create:
            fid = self.files.create(
                body={"name": basename(path), "mimeType": FOLDER_MIME, "parents": [parent]},
                fields="id",
                supportsAllDrives=True,
            ).execute()["id"]
        if fid:
            self._folders[path] = fid
        return fid

    def _file(self, path: str) -> str | None:
        parent = self._folder(dirname(path), create=False)
        return self._find(parent, basename(path), folder=False) if parent else None

    # -- Sink API ---------------------------------------------------------

    def read(self, path: str) -> bytes | None:
        fid = self._file(path)
        if fid is None:
            return None
        return self.files.get_media(fileId=fid, supportsAllDrives=True).execute()

    def write(self, path: str, data: bytes) -> None:
        from googleapiclient.http import MediaIoBaseUpload

        mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=len(data) > 5_000_000)
        fid = self._file(path)
        if fid:
            # Updating in place keeps the file id, so links and Tableau
            # connections to the file keep working.
            self.files.update(fileId=fid, media_body=media, supportsAllDrives=True).execute()
        else:
            parent = self._folder(dirname(path), create=True)
            self.files.create(
                body={"name": basename(path), "parents": [parent]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()

    def copy(self, src: str, dst: str) -> None:
        # Archiving copies rather than moves: the current file is overwritten
        # right after, keeping its id stable for links and Tableau.
        fid = self._file(src)
        if fid is None:
            return
        parent = self._folder(dirname(dst), create=True)
        self.files.copy(
            fileId=fid,
            body={"name": basename(dst), "parents": [parent]},
            supportsAllDrives=True,
        ).execute()
