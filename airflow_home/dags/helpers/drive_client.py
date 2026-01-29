"""Google Drive client for checking file modification times."""

import os
import logging
from typing import Optional
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


class DriveClient:
    """Client for checking Google Drive file metadata."""

    def __init__(self, credentials_path: str = None):
        """
        Initialize the Drive client.

        Args:
            credentials_path: Path to service account JSON. If None, uses
                             GOOGLE_APPLICATION_CREDENTIALS env var.
        """
        if credentials_path is None:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if not credentials_path:
            raise ValueError(
                "credentials_path must be provided or "
                "GOOGLE_APPLICATION_CREDENTIALS env var must be set"
            )

        self.credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=self.credentials)

    def get_file_modified_time(self, file_id: str) -> Optional[str]:
        """
        Get the last modified time of a Google Drive file (including Sheets).

        Args:
            file_id: The Google Drive file ID (same as spreadsheet ID for Sheets).

        Returns:
            ISO 8601 formatted modified time string, or None on error.
        """
        try:
            file_metadata = self.service.files().get(
                fileId=file_id,
                fields="modifiedTime"
            ).execute()

            modified_time = file_metadata.get("modifiedTime")
            logger.info(f"File {file_id} last modified: {modified_time}")
            return modified_time

        except Exception as e:
            logger.error(f"Failed to get file metadata for {file_id}: {e}")
            return None

    def has_file_changed(self, file_id: str, last_known_modified: Optional[str]) -> bool:
        """
        Check if a file has been modified since the last known time.

        Args:
            file_id: The Google Drive file ID.
            last_known_modified: The last known modified time (ISO 8601 string).

        Returns:
            True if the file has changed (or if we can't determine), False otherwise.
        """
        if not last_known_modified:
            logger.info("No last known modified time - assuming file has changed")
            return True

        current_modified = self.get_file_modified_time(file_id)
        if not current_modified:
            logger.warning("Could not get current modified time - assuming file has changed")
            return True

        # Compare timestamps
        try:
            current_dt = datetime.fromisoformat(current_modified.replace("Z", "+00:00"))
            last_dt = datetime.fromisoformat(last_known_modified.replace("Z", "+00:00"))

            if current_dt > last_dt:
                logger.info(f"File has changed: {last_known_modified} -> {current_modified}")
                return True
            else:
                logger.info(f"File has not changed since {last_known_modified}")
                return False

        except Exception as e:
            logger.error(f"Error comparing timestamps: {e}")
            return True  # Assume changed on error



