"""
Seeds legacy_db with realistic, messy data.

Run AFTER the MySQL container is up and 01_schema.sql has been applied
(docker-compose init scripts handle that automatically on first boot).

Usage:
    python db/02_seed.py

Deliberately injects the kind of mess a real legacy DB accumulates:
  - inconsistent status code casing / stray whitespace
  - a handful of unrecognized / "junk" codes with no clear mapping
  - nullable FKs left null on a subset of rows
  - sparse dx_desc fill (never backfilled, as called out in the problem statement)
"""

import os
import random
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker()
random.seed(42)
Faker.seed(42)

SOURCE_DB_URL = os.getenv(
    "SOURCE_DB_URL",
    "mysql+pymysql://legacy_user:legacy_pass@localhost:3307/legacy_db",
)

N_DEPTS = 8
N_PROVIDERS = 40
N_PATIENTS = 12_000
N_ENCOUNTERS = 25_000

# Undocumented codes as they'd really exist -- including a few "junk" ones
# that have no clean 1:1 mapping and MUST be flagged for human review.
PAT_STATUS_CODES = ["A", "D", "I", "S", "a", " D", "X"]  # X, lowercase, stray space = junk
PAT_STATUS_WEIGHTS = [40, 30, 15, 10, 2, 2, 1]

SEX_CODES = ["M", "F", "U", None]
SEX_WEIGHTS = [47, 47, 4, 2]

PRV_TYPE_CODES = ["MD", "DO", "NP", "PA"]

ENC_TYPE_CODES = ["IP", "OP", "ER", "TC"]

PMT_STATUS_CODES = ["P", "U", "W", "R", "?"]  # '?' = junk code, no clear mapping
PMT_STATUS_WEIGHTS = [50, 30, 12, 6, 2]

DX_SAMPLE = ["I10", "E11.9", "J45.909", "M54.5", "R51", "K21.9", "N39.0", "Z00.00", None, "TBD"]


def weighted_choice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]


def main():
    engine = create_engine(SOURCE_DB_URL)

    with engine.begin() as conn:
        print("Seeding dept_master...")
        dept_ids = []
        for _ in range(N_DEPTS):
            res = conn.execute(
                text("INSERT INTO dept_master (dept_nm, dept_loc, active_flg) VALUES (:n, :l, 'Y')"),
                {"n": fake.company() + " Dept", "l": fake.city()},
            )
            dept_ids.append(res.lastrowid)

        print("Seeding prv_tbl...")
        prv_ids = []
        for _ in range(N_PROVIDERS):
            res = conn.execute(
                text(
                    "INSERT INTO prv_tbl (prv_nm, prv_typ_cd, dept_id, npi_num) "
                    "VALUES (:n, :t, :d, :npi)"
                ),
                {
                    "n": "Dr. " + fake.name(),
                    "t": random.choice(PRV_TYPE_CODES),
                    "d": random.choice(dept_ids + [None]),  # some providers unassigned
                    "npi": str(fake.random_number(digits=10, fix_len=True)),
                },
            )
            prv_ids.append(res.lastrowid)

        print(f"Seeding patient_records ({N_PATIENTS} rows)...")
        pat_ids = []
        batch = []
        for i in range(N_PATIENTS):
            admit = fake.date_time_between(start_date="-5y", end_date="now")
            discharged = random.random() < 0.85
            discharge = admit + timedelta(days=random.randint(0, 14)) if discharged else None
            batch.append(
                {
                    "st": weighted_choice(PAT_STATUS_CODES, PAT_STATUS_WEIGHTS),
                    "fn": fake.first_name(),
                    "ln": fake.last_name(),
                    "dob": fake.date_of_birth(minimum_age=0, maximum_age=95),
                    "sex": weighted_choice(SEX_CODES, SEX_WEIGHTS),
                    "adm": admit,
                    "dis": discharge,
                }
            )
            if len(batch) >= 1000 or i == N_PATIENTS - 1:
                for row in batch:
                    res = conn.execute(
                        text(
                            "INSERT INTO patient_records "
                            "(pat_st_cd, fst_nm, lst_nm, dob, sex_cd, admit_dt, discharge_dt) "
                            "VALUES (:st, :fn, :ln, :dob, :sex, :adm, :dis)"
                        ),
                        row,
                    )
                    pat_ids.append(res.lastrowid)
                batch = []
                print(f"  ...{len(pat_ids)} patients inserted")

        print(f"Seeding enc_log ({N_ENCOUNTERS} rows)...")
        enc_ids = []
        batch = []
        for i in range(N_ENCOUNTERS):
            pat_id = random.choice(pat_ids)
            has_provider = random.random() < 0.9
            batch.append(
                {
                    "pat": pat_id,
                    "prv": random.choice(prv_ids) if has_provider else None,
                    "typ": random.choice(ENC_TYPE_CODES),
                    "dt": fake.date_time_between(start_date="-5y", end_date="now"),
                    "amt": round(random.uniform(50, 15000), 2),
                    "pmt": weighted_choice(PMT_STATUS_CODES, PMT_STATUS_WEIGHTS),
                    "notes": fake.sentence() if random.random() < 0.3 else None,
                }
            )
            if len(batch) >= 1000 or i == N_ENCOUNTERS - 1:
                for row in batch:
                    res = conn.execute(
                        text(
                            "INSERT INTO enc_log "
                            "(pat_id, prv_id, enc_typ_cd, enc_dt, bill_amt, pmt_st_cd, notes_txt) "
                            "VALUES (:pat, :prv, :typ, :dt, :amt, :pmt, :notes)"
                        ),
                        row,
                    )
                    enc_ids.append(res.lastrowid)
                batch = []
                print(f"  ...{len(enc_ids)} encounters inserted")

        print("Seeding dx_codes (sparse, ~60% of encounters)...")
        batch = []
        count = 0
        for enc_id in enc_ids:
            if random.random() < 0.6:
                batch.append(
                    {
                        "enc": enc_id,
                        "cd": random.choice(DX_SAMPLE),
                        "desc": fake.sentence(nb_words=4) if random.random() < 0.4 else None,
                        "pri": "Y" if random.random() < 0.5 else "N",
                    }
                )
                count += 1
            if len(batch) >= 1000:
                for row in batch:
                    conn.execute(
                        text(
                            "INSERT INTO dx_codes (enc_id, dx_cd, dx_desc, primary_flg) "
                            "VALUES (:enc, :cd, :desc, :pri)"
                        ),
                        row,
                    )
                batch = []
        for row in batch:
            conn.execute(
                text(
                    "INSERT INTO dx_codes (enc_id, dx_cd, dx_desc, primary_flg) "
                    "VALUES (:enc, :cd, :desc, :pri)"
                ),
                row,
            )
        print(f"  ...{count} dx_codes inserted")

    print("\nSeed complete.")
    print(f"  dept_master:      {len(dept_ids)}")
    print(f"  prv_tbl:          {len(prv_ids)}")
    print(f"  patient_records:  {len(pat_ids)}")
    print(f"  enc_log:          {len(enc_ids)}")


if __name__ == "__main__":
    main()