"""Google Sheets client for reading clinic data."""

import os
import logging
from typing import List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


class SheetsClient:
    """Client for reading data from Google Sheets."""

    def __init__(self, credentials_path: str = None):
        """
        Initialize the Sheets client.

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
        self.client = gspread.authorize(self.credentials)

    def read_clinics(
        self,
        spreadsheet_id: str,
        worksheet_name: str = "KLINIK PERUBATAN SWASTA",
        skip_rows: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Read clinic data from the Google Sheet.

        Args:
            spreadsheet_id: The Google Sheets document ID.
            worksheet_name: Name of the worksheet to read.
            skip_rows: Number of header rows to skip (default 2 for this sheet).

        Returns:
            List of dicts with clinic data, mapped to expected column names.
        """
        logger.info(f"Opening spreadsheet: {spreadsheet_id}")
        spreadsheet = self.client.open_by_key(spreadsheet_id)

        logger.info(f"Reading worksheet: {worksheet_name}")
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Get all values
        all_values = worksheet.get_all_values()

        if len(all_values) <= skip_rows:
            logger.warning("No data rows found in sheet")
            return []

        # The header row is at index skip_rows - 1 (row 2 in the sheet, 0-indexed as 1)
        # Based on the sheet structure:
        # Row 1: TARIKH KEMASKINI: 10/12/2025
        # Row 2: BIL, JENIS_FASILITI, NAMA_PENUH_FASILITI, ALAMAT, POSKOD, BANDAR, NEGERI
        # Row 3+: Data

        header_row = all_values[skip_rows - 1] if skip_rows > 0 else all_values[0]
        data_rows = all_values[skip_rows:]

        # Expected columns mapping (sheet column name -> our key)
        column_mapping = {
            "BIL": "bil",
            "JENIS_FASILITI": "jenis_fasiliti",
            "NAMA_PENUH_FASILITI": "nama_penuh_fasiliti",
            "ALAMAT": "alamat",
            "POSKOD": "poskod",
            "BANDAR": "bandar",
            "NEGERI": "negeri",
        }

        # Find column indices
        col_indices = {}
        for i, col_name in enumerate(header_row):
            clean_name = col_name.strip().upper()
            if clean_name in column_mapping:
                col_indices[column_mapping[clean_name]] = i

        logger.info(f"Found columns: {list(col_indices.keys())}")

        # Parse data rows
        clinics = []
        for row_idx, row in enumerate(data_rows):
            if not row or not any(cell.strip() for cell in row):
                # Skip empty rows
                continue

            clinic = {}
            for key, col_idx in col_indices.items():
                if col_idx < len(row):
                    clinic[key] = row[col_idx].strip()
                else:
                    clinic[key] = ""

            # Skip rows without a name
            if not clinic.get("nama_penuh_fasiliti"):
                continue

            clinics.append(clinic)

        logger.info(f"Read {len(clinics)} clinic records from sheet")
        return clinics



