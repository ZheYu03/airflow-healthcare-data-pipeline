# Helper modules for clinic sync DAG
"""
Helpers package.

Important: keep this module import-safe. Airflow imports `helpers` when DAGs do
`from helpers.x import Y`, which executes this file.

Do NOT import optional/unused modules here (e.g. Google Places) because a missing
dependency would prevent *all* DAGs from loading.
"""

__all__ = []

