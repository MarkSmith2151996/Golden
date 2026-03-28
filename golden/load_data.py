"""Load raw_data.csv into the SQLite database.

Usage: python -m golden.load_data
"""

import csv
import sys
import time
from pathlib import Path

from golden.database import EstablishmentRow, ViolationRow, get_session, init_db

CSV_PATH = Path("data/raw_data.csv")
BATCH_SIZE = 5000


def load():
    if not CSV_PATH.exists():
        print(f"ERROR: {CSV_PATH} not found")
        sys.exit(1)

    init_db()
    print("Database initialized.")

    # --- Pass 1: Insert unique establishments ---
    print("\n--- Pass 1: Loading establishments ---")
    establishments = {}  # (city, est_id) -> row dict
    row_count = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row_count += 1
            key = (row["city"], row["establishment_id"])
            if key not in establishments:
                establishments[key] = {
                    "city": row["city"],
                    "establishment_id": row["establishment_id"],
                    "name": row.get("name", ""),
                    "address": row.get("address", ""),
                    "zip": row.get("zip", ""),
                    "owner": row.get("owner", ""),
                    "establishment_type": row.get("establishment_type", ""),
                }

    print(f"  Scanned {row_count:,} CSV rows → {len(establishments):,} unique establishments")

    # Bulk insert establishments
    with get_session() as session:
        batch = []
        for i, est_dict in enumerate(establishments.values(), 1):
            batch.append(EstablishmentRow(**est_dict))
            if len(batch) >= BATCH_SIZE:
                session.bulk_save_objects(batch)
                session.flush()
                batch.clear()
                if i % 50_000 == 0:
                    print(f"  Inserted {i:,} establishments...")
        if batch:
            session.bulk_save_objects(batch)
            session.flush()

    print(f"  Inserted {len(establishments):,} establishments total.")

    # Build FK lookup: (city, est_id) -> db id
    fk_lookup = {}
    with get_session() as session:
        for db_id, city, est_id in session.query(
            EstablishmentRow.id, EstablishmentRow.city, EstablishmentRow.establishment_id
        ).all():
            fk_lookup[(city, est_id)] = db_id

    # --- Pass 2: Insert violations ---
    print("\n--- Pass 2: Loading violations ---")
    inserted = 0
    skipped = 0

    with get_session() as session:
        batch = []
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code = row.get("violation_code", "").strip()
                if not code:
                    skipped += 1
                    continue

                key = (row["city"], row["establishment_id"])
                est_pk = fk_lookup.get(key)
                if est_pk is None:
                    skipped += 1
                    continue

                in_comp = row.get("in_compliance", "").strip().lower() == "true"
                is_corr = row.get("is_corrected", "").strip().lower() == "true"

                batch.append(ViolationRow(
                    establishment_id=est_pk,
                    city=row["city"],
                    inspection_date=row.get("inspection_date", ""),
                    inspection_type=row.get("inspection_type", ""),
                    in_compliance=in_comp,
                    violation_code=code,
                    violation_type=row.get("violation_type", ""),
                    violation_description=row.get("violation_description", ""),
                    problem_description=row.get("problem_description", ""),
                    is_corrected=is_corr,
                ))
                inserted += 1

                if len(batch) >= BATCH_SIZE:
                    session.bulk_save_objects(batch)
                    session.flush()
                    batch.clear()
                    if inserted % 50_000 == 0:
                        print(f"  Inserted {inserted:,} violations...")

        if batch:
            session.bulk_save_objects(batch)
            session.flush()

    print(f"  Inserted {inserted:,} violations ({skipped:,} rows skipped — no violation_code).")

    # --- Summary ---
    with get_session() as session:
        est_count = session.query(EstablishmentRow).count()
        viol_count = session.query(ViolationRow).count()

    print(f"\n=== Done ===")
    print(f"  Establishments: {est_count:,}")
    print(f"  Violations:     {viol_count:,}")
    print(f"  Database:       data/golden.db")


if __name__ == "__main__":
    t0 = time.time()
    load()
    print(f"  Time:           {time.time() - t0:.1f}s")
