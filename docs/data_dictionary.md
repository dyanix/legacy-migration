# Target Data Dictionary

Generated 2026-08-12T19:30:10.904653+00:00 by `doc_generator`. 5 tables, 32 columns. Column descriptions are approved mappings, llm table overviews.

Do not edit by hand -- this file is regenerated on every pipeline run from the approved mappings and transformation rules.

## Migration summary

| Table | Source rows | Target rows | Load mode | Status |
|---|---|---|---|---|
| `dept_master` | 8 | 8 | full | OK |
| `patient_records` | 12000 | 12000 | full | OK |
| `prv_tbl` | 40 | 40 | full | OK |
| `enc_log` | 25000 | 25000 | full | OK |
| `dx_codes` | 15080 | 15080 | full | OK |

## Sensitive column inventory

12 columns were flagged as PII/PHI by the schema profiler. Access to these should be restricted in the target warehouse.

| Table | Column | Categories |
|---|---|---|
| `dept_master` | `department_name` | person_name |
| `dx_codes` | `dx_id` | diagnosis_phi |
| `dx_codes` | `diagnosis_code` | diagnosis_phi |
| `dx_codes` | `dx_description` | diagnosis_phi |
| `enc_log` | `encounter_datetime` | service_date_phi |
| `patient_records` | `first_name` | person_name |
| `patient_records` | `last_name` | person_name |
| `patient_records` | `date_of_birth` | date_of_birth |
| `patient_records` | `admission_datetime` | service_date_phi |
| `patient_records` | `discharge_datetime` | service_date_phi |
| `prv_tbl` | `provider_name` | person_name |
| `prv_tbl` | `npi_number` | phone, provider_identifier |

## Human review overrides

13 mapping(s) were changed or explicitly approved by a human reviewer rather than auto-approved by confidence score.

### `dept_master.dept_id`

- Confidence: 0.75
- Decision: approve by - at 2026-08-09T18:18:16.885241+00:00
- Rationale: Pass-through PK confirmed. Empty-string sample is a profiler display artifact, not source data — dept_id is populated for all 8 rows.
- Prompt ID: `prompt-ec0e1df3-cb91-4fab-b089-a49cdc293552`

### `dept_master.is_active`

- Confidence: 0.75
- Decision: approve by - at 2026-08-09T18:18:32.137660+00:00
- Rationale: Y/N/else-NULL is correct. Only 'Y' present today; rule handles 'N' if it appears and refuses to guess on anything else.
- Prompt ID: `prompt-f37ec6a3-9d32-488e-998d-633224001388`

### `dx_codes.diagnosis_code`

- Confidence: 0.75
- Decision: override by - at 2026-08-09T18:23:09.866780+00:00
- Rationale: Preserving 'TBD' rather than nulling it. TBD is ~11% of rows and source already has 10.16% true NULLs; mapping TBD to NULL merges "diagnosis pending" with "diagnosis absent" and the distinction is unrecoverable after migration.
- Prompt ID: `prompt-fe900418-52e8-4635-adfb-d58743a48691`

### `dx_codes.dx_description`

- Confidence: 0.4
- Decision: override by - at 2026-08-09T18:23:39.515599+00:00
- Rationale: Removed CAST to VARCHAR(255) — dx_desc is TEXT and the cast silently truncates anything longer.
- Prompt ID: `prompt-e536f024-9265-4a9f-b09a-7db6c568b5ed`

### `enc_log.encounter_type_code`

- Confidence: 0.75
- Decision: override by - at 2026-08-09T18:24:08.588024+00:00
- Rationale: Codes preserved rather than expanded to labels. 'TC' is genuinely ambiguous (Telehealth / Transitional Care / Trauma Center) with no lookup table to confirm, and ELSE 'UNKNOWN' would mask any new code silently. Column is named _code; expansion belongs in a lookup table.
- Prompt ID: `prompt-8f345925-4ae4-4e19-a764-d91d374cf749`

### `enc_log.payment_status_code`

- Confidence: 0.55
- Decision: override by - at 2026-08-09T18:24:31.856721+00:00
- Rationale: Codes preserved. 'P' could be Paid or Pending — a material difference on billing data — and '?' (486 rows) would have been merged with the ELSE branch, losing the distinction between an explicit '?' and an unrecognised code.
- Prompt ID: `prompt-517f25ef-6c7d-4790-84af-cd27b991d435`

### `enc_log.notes`

- Confidence: 0.55
- Decision: override by - at 2026-08-09T18:25:02.740234+00:00
- Rationale: Original contained two expressions; comment-stripping kept the CAST and dropped the TRIM, so no trimming would have occurred.
- Prompt ID: `prompt-736f6837-3695-442f-997d-ea979a078726`

### `patient_records.patient_status_code`

- Confidence: 0.55
- Decision: approve by - at 2026-08-09T18:18:59.497533+00:00
- Rationale: Codes preserved raw, not expanded to labels. 'X' (130 rows) has no confirmed meaning and A/D/I/S are unverified, so expanding would bake in a guess. TRIM/UPPER normalises the ' D' whitespace variant only.
- Prompt ID: `prompt-8068a6d5-188b-47b8-9ee4-b6a63e3750fe`

### `patient_records.last_name`

- Confidence: 0.0
- Decision: approve by - at 2026-08-09T18:19:42.205918+00:00
- Rationale: Confidence displayed as 0.00 is a structured-output parse bug — model's actual confidence was 0.97, visible leaked into edge_cases. Approving on merit. INITCAP in the comment is not applied and must not be: proper-casing destroys O'BRIEN and van der Berg.
- Prompt ID: `prompt-a5b91c29-ec04-495e-ac4e-c4cef7f91cd1`

### `patient_records.created_ts`

- Confidence: 0.6
- Decision: override by - at 2026-08-09T18:52:45.334996+00:00
- Rationale: created_ts is a genuine MySQL TIMESTAMP column, not a malformed string. The 'datetime.datetime(...)' pattern in the schema profile is a Python repr artifact from schema_profiler serialising datetime objects, not the stored value — confirmed by querying the source directly. No parsing needed.
- Prompt ID: `prompt-1b9d745a-8479-4187-a754-ff0c7b8e9e26`

### `patient_records.updated_at`

- Confidence: 0.55
- Decision: approve by - at 2026-08-09T18:19:21.765675+00:00
- Rationale: Column is 100% NULL in source. Migrating as-is rather than defaulting to created_ts. Note: this rules it out as a watermark, so patient_records will always full-load.
- Prompt ID: `prompt-989e7c64-b2d1-4b3a-8df1-a35c29260f41`

### `prv_tbl.provider_name`

- Confidence: 0.0
- Decision: approve by - at 2026-08-09T18:20:09.223861+00:00
- Rationale: Same 0.00 parse bug; actual confidence 0.85. Name kept whole rather than split into title/first/last — the regex in the comment breaks on multi-word surnames.
- Prompt ID: `prompt-6ce43495-ec06-4f6f-88fe-9bb71541c1f0`

### `prv_tbl.dept_id`

- Confidence: 0.55
- Decision: override by - at 2026-08-09T18:22:32.136799+00:00
- Rationale: Renamed to match dept_master's PK, which maps to dept_id. FK detection in generate_dbt_models.py matches on exact column name, so department_id would silently drop the prv_tbl → dept_master relationship test.
- Prompt ID: `prompt-986a9671-95f0-4c17-825a-dfdec5c0a811`

## Tables

### `dept_master`

dept_master holds 8 department records, each with a unique identifier, name, location, and active status, and serves as a reference table joined to prv_tbl via dept_id as a foreign key. Consumers should note that department_name is flagged as containing person_name-type sensitive data and appears synthetically generated with a literal " Dept" suffix that may need stripping, while dept_location values also look synthetic and lack standardized formatting. The is_active flag has only been observed with the value "Y" in this dataset, so handling of "N" or other values is untested, and dept_id's low cardinality relative to a full load warrants verification that this represents the complete table.

| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |
|---|---|---|---|---|---|---|
| `dept_id` **(PK)** | `dept_id` | - | `CAST(dept_id AS INTEGER) -- direct pass-through, no transformation needed; preserve as primary key` | 0.75 | - | Unique numeric identifier for a department record, serving as the primary key of dept_master and referenced by prv_tbl.dept_id as a foreign key. |
| `department_name` | `dept_nm` | - | `TRIM(dept_nm) -- direct passthrough, no case/format change since values already appear as intended display names; optionally CAST to VARCHAR(100) to match target schema` | 0.82 | person_name | Name/label of the department, appearing to be auto-generated from company-style naming patterns (e.g., "Rodriguez, Figueroa and Sanchez Dept") combined with a literal "Dept" suffix, used as the human-readable identifier for a department record in dept_master |
| `dept_location` | `dept_loc` | - | `TRIM(dept_loc) -- direct pass-through rename, no value transformation needed` | 0.8 | - | Location/city name associated with a department, likely representing the department's physical office or branch location |
| `is_active` | `active_flg` | - | `CASE WHEN active_flg = 'Y' THEN TRUE WHEN active_flg = 'N' THEN FALSE ELSE NULL END` | 0.75 | - | Flag indicating whether the department record is currently active (Y=active, presumably N=inactive though not observed) |

### `dx_codes`

The dx_codes table holds diagnosis records (15,080 rows, full load), with each row linking via encounter_id to a single visit in enc_log and identifying whether it was the primary diagnosis for that encounter. Diagnosis codes are intended to follow ICD-10-CM format but include a "TBD" placeholder in roughly 11% of rows and nulls in about 10%, with unclear distinction between the two; dx_description has been human-reviewed but appears to contain synthetic/placeholder text with a 60% null rate rather than genuine clinical descriptions, and lacks a validated link to a standard code description table. Consumers should treat diagnosis_code and dx_description as sensitive (diagnosis_phi), confirm encounter_id foreign keys against enc_log before use, and not assume a validated one-diagnosis-per-encounter constraint on is_primary_diagnosis.

| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |
|---|---|---|---|---|---|---|
| `dx_id` **(PK)** | `dx_id` | - | `CAST(dx_id AS INTEGER) -- direct passthrough, no transformation needed` | 0.98 | diagnosis_phi | Surrogate primary key uniquely identifying each diagnosis record in the dx_codes table |
| `encounter_id` | `enc_id` | - | `SELECT enc_id AS encounter_id FROM dx_codes -- direct passthrough, cast to INTEGER if not already; validate referential integrity against enc_log.enc_id` | 0.95 | - | Foreign key referencing the encounter (visit) record associated with this diagnosis code entry, linking dx_codes to the enc_log table |
| `diagnosis_code` | `dx_cd` | - | `UPPER(TRIM(dx_cd))` | 0.75 | diagnosis_phi | ICD-10-CM diagnosis code assigned to an encounter, identifying the medical diagnosis (e.g., N39.0 = Urinary tract infection, E11.9 = Type 2 diabetes without complications) |
| `dx_description` | `dx_desc` | - | `TRIM(dx_desc)` | 0.4 | diagnosis_phi | Intended to be a free-text description of a diagnosis code (dx_cd) associated with an encounter, but the sample values appear to be randomly generated placeholder/lorem-ipsum-style text rather than real clinical diagnosis descriptions |
| `is_primary_diagnosis` | `primary_flg` | - | `CASE WHEN primary_flg = 'Y' THEN TRUE WHEN primary_flg = 'N' THEN FALSE ELSE NULL END` | 0.95 | - | Flag indicating whether this diagnosis code is the primary diagnosis for the associated encounter (Y) or a secondary/non-primary diagnosis (N) |

### `enc_log`

The enc_log table captures individual patient encounter records (25,000 rows, full load), including encounter timing, type, billing amount, payment status, and free-text notes, with encounter_id as its primary key. It links to patient_records via patient_id, to prv_tbl via provider_id, and to dx_codes via encounter_id, while dept_master is referenced elsewhere in the model though not directly joined from columns listed here. Consumers should note that encounter_datetime is treated as sensitive (service_date_phi), encounter_type_code and payment_status_code use human-reviewed but unconfirmed single/double-letter code mappings (including an unresolved '?' status value), and the notes field appears to contain synthetic/placeholder text rather than genuine clinical documentation.

| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |
|---|---|---|---|---|---|---|
| `encounter_id` **(PK)** | `enc_id` | - | `CAST(enc_id AS INTEGER) AS encounter_id` | 0.97 | - | Unique numeric identifier for an encounter record (primary key of enc_log table, representing a single patient encounter/visit) |
| `patient_id` | `pat_id` | - | `SELECT CAST(pat_id AS INTEGER) AS patient_id FROM enc_log -- direct passthrough, maps 1:1 to patient_records.pat_id` | 0.97 | - | Foreign key referencing the patient record associated with this encounter log entry (i.e., the patient who received the encounter/service) |
| `provider_id` | `prv_id` | - | `CAST(prv_id AS INTEGER) AS provider_id -- direct passthrough, no value transformation needed; ensure referential integrity against prv_tbl.prv_id` | 0.9 | - | Foreign key referencing the provider (e.g., physician/clinician) associated with the encounter record, linking to prv_tbl.prv_id |
| `encounter_type_code` | `enc_typ_cd` | - | `TRIM(UPPER(enc_typ_cd))` | 0.75 | - | Code representing the type/category of a patient encounter (visit) in a healthcare encounter log - e.g., Emergency Room, Outpatient, Telehealth/Telemedicine Consult, Inpatient |
| `encounter_datetime` | `enc_dt` | - | `CAST(enc_dt AS DATETIME) -- direct passthrough, no conversion needed since already in DATETIME format; standardize to ISO 8601 (YYYY-MM-DD HH:MM:SS) if target schema requires string representation` | 0.9 | service_date_phi | Timestamp indicating when the encounter (patient visit/interaction) recorded in enc_log occurred or was created/logged |
| `bill_amount` | `bill_amt` | - | `CAST(bill_amt AS DECIMAL(10,2)) -- direct pass-through, renaming column to bill_amount; no unit conversion needed` | 0.85 | - | The billed amount (monetary charge) associated with a specific encounter/visit record in the encounter log table |
| `payment_status_code` | `pmt_st_cd` | - | `TRIM(UPPER(pmt_st_cd))` | 0.55 | - | Payment status code for an encounter (billing) record - likely indicates whether payment is Pending, Unpaid, Waived, Rejected/Refunded, etc. |
| `notes` | `notes_txt` | - | `TRIM(notes_txt)` | 0.55 | - | Free-text notes field associated with an encounter log record; appears to contain arbitrary/placeholder-like sentences rather than structured clinical or business content, likely used for annotation or comments about the encounter. |

### `patient_records`

patient_records holds one row per patient (12,000 total, full load) with demographic, administrative, and status details, keyed by patient_id, which enc_log references via pat_id to link patients to their encounters; dept_master, dx_codes, and prv_tbl are not directly joined by columns present here. Consumers should treat first_name, last_name, and date_of_birth as sensitive person-level PHI, and admission_datetime/discharge_datetime as service-date PHI, with discharge_datetime null for ~14.88% of records (likely still-admitted patients). Human-reviewed fields include patient_status_code (note the ' D' whitespace variant and ambiguous 'X' code), last_name, created_ts, and updated_at; created_ts values are malformed stringified datetime objects with a truncated "datetime" prefix and low cardinality suggesting batch-load timestamps, while updated_at is entirely null in the observed data and may be unused.

| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |
|---|---|---|---|---|---|---|
| `patient_id` **(PK)** | `pat_id` | - | `CAST(pat_id AS INTEGER) AS patient_id -- direct 1:1 passthrough, no value transformation needed` | 0.97 | - | Unique identifier for a patient record; primary key of the patient_records table used to link patient demographic/administrative data to related tables (e.g., enc_log encounter logs). |
| `patient_status_code` | `pat_st_cd` | - | `TRIM(UPPER(pat_st_cd)) -- normalize whitespace/case; e.g. ' D' -> 'D'; then map: A='Active', D='Discharged', I='Inactive', S='Suspended', X='Excluded/Unknown' (pending confirmation of exact code meanings)` | 0.55 | - | Patient status code indicating current state (e.g., Active, Discharged, Inactive, Suspended, Excluded/Unknown) |
| `first_name` | `fst_nm` | - | `TRIM(fst_nm) -- copy value directly, trimming leading/trailing whitespace; optionally apply proper-case normalization if source casing is inconsistent` | 0.97 | person_name | Patient's first (given) name |
| `last_name` | `lst_nm` | - | `TRIM(lst_nm) -- direct copy, standardize whitespace and optionally proper-case: INITCAP(TRIM(lst_nm))` | 0.0 | person_name | Patient's last name (surname) |
| `date_of_birth` | `dob` | - | `CAST(dob AS DATE) -- values already in ISO 8601 (YYYY-MM-DD) format, direct mapping to target DATE column` | 0.97 | date_of_birth | Patient's date of birth |
| `sex_code` | `sex_cd` | - | `CASE sex_cd WHEN 'M' THEN 'MALE' WHEN 'F' THEN 'FEMALE' WHEN 'U' THEN 'UNKNOWN' ELSE NULL END` | 0.9 | - | Patient's sex/gender code, using single-character codes for Male, Female, or Unknown/Unspecified |
| `admission_datetime` | `admit_dt` | - | `CAST(admit_dt AS DATETIME) -- direct copy, ensure ISO 8601 format 'YYYY-MM-DD HH:MI:SS'` | 0.9 | service_date_phi | Timestamp marking when a patient was admitted to a facility/care episode |
| `discharge_datetime` | `discharge_dt` | - | `CAST(discharge_dt AS DATETIME) -- direct pass-through, ensure ISO 8601 format (YYYY-MM-DD HH:MM:SS); no timezone conversion applied since source has no explicit TZ info` | 0.9 | service_date_phi | Timestamp indicating when a patient was discharged from care/facility, paired with admit_dt to define the encounter duration |
| `created_ts` | `created_ts` | - | `CAST(created_ts AS DATETIME)` | 0.6 | - | Timestamp indicating when the patient record was created, stored as a stringified Python datetime.datetime object (missing leading 'd' - 'atetime' instead of 'datetime'), with format datetime.datetime(year, month, day, hour, minute, second) |
| `updated_at` | `updated_ts` | - | `CAST(updated_ts AS TIMESTAMP) -- direct passthrough, rename column; no value transformation needed` | 0.55 | - | Timestamp indicating when the patient record was last updated/modified |

### `prv_tbl`

This table holds provider records (40 rows, full load), with provider_id serving as the primary key referenced by enc_log to associate encounters with the provider involved, and dept_id serving as a foreign key linking each provider to a department in dept_master; no direct relationship to dx_codes or patient_records is indicated by the columns present. Consumers should note that provider_name is a sensitive person_name field that has been human-reviewed, npi_number is sensitive (flagged as phone/provider_identifier) and should be validated for correct 10-digit formatting and non-placeholder values, and dept_id contains empty-string values inconsistent with its declared integer type, warranting investigation before use. The low row count and human-reviewed dept_id mapping suggest this may be a curated or sample extract rather than a full production population, so referential integrity against enc_log and dept_master should be verified before downstream use.

| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |
|---|---|---|---|---|---|---|
| `provider_id` **(PK)** | `prv_id` | - | `CAST(prv_id AS INTEGER) AS provider_id -- direct 1:1 passthrough, no value transformation needed` | 0.95 | - | Primary key identifier for a provider record in the provider table (prv_tbl), uniquely identifying each healthcare provider entity. Referenced as a foreign key from enc_log (encounter log) to link encounters to the provider who performed/is associated with them. |
| `provider_name` | `prv_nm` | - | `TRIM(prv_nm) -- copy as-is to provider_name; optionally split into title/first_name/last_name via regex: '^(Dr\.\|Mr\.\|Mrs\.\|Ms\.)?\s*([A-Za-z]+)\s+([A-Za-z]+)$' capturing title, first_name, last_name` | 0.0 | person_name | Full name (with title prefix) of a healthcare provider/physician |
| `provider_type_code` | `prv_typ_cd` | - | `CASE prv_typ_cd WHEN 'MD' THEN 'MD' WHEN 'DO' THEN 'DO' WHEN 'NP' THEN 'NP' WHEN 'PA' THEN 'PA' ELSE prv_typ_cd END -- pass-through of existing 2-letter codes into standardized target column name; optionally join to a reference/lookup table for full description (e.g., 'MD' -> 'Medical Doctor') if target schema requires descriptive text` | 0.92 | - | Provider type/credential code indicating the professional classification of a healthcare provider (Medical Doctor, Doctor of Osteopathy, Nurse Practitioner, Physician Assistant) |
| `dept_id` | `dept_id` | - | `CAST(dept_id AS INTEGER) -- direct passthrough mapping to department_id, preserving referential integrity with dept_master.dept_id` | 0.55 | - | Foreign key reference linking a provider record (prv_tbl) to its associated department in dept_master |
| `npi_number` | `npi_num` | - | `CAST(TRIM(npi_num) AS VARCHAR(10)) -- validate that value is exactly 10 digits (regex '^\d{10}$'); no case or format conversion needed, just rename column` | 0.9 | phone, provider_identifier | National Provider Identifier (NPI) - a unique 10-digit identification number issued to healthcare providers in the US, used here to identify records in the provider table (prv_tbl). |

## AI provenance

32 of 32 column mappings trace to a recorded prompt ID. Full prompt text and model reasoning are in `audit/reviewed_mappings.json`.

| Table | Column | Prompt ID | Confidence | Review |
|---|---|---|---|---|
| `dept_master` | `dept_id` | `prompt-ec0e1df3-cb91-4fab-b089-a49cdc293552` | 0.75 | approve |
| `dept_master` | `department_name` | `prompt-80fa6dc9-f515-4701-9a43-0b4d93741de8` | 0.82 | auto-approved |
| `dept_master` | `dept_location` | `prompt-d5b34be4-8f02-4992-a2c3-a9127d6d6ae1` | 0.8 | auto-approved |
| `dept_master` | `is_active` | `prompt-f37ec6a3-9d32-488e-998d-633224001388` | 0.75 | approve |
| `dx_codes` | `dx_id` | `prompt-a8e9d98e-f8a1-4d3e-bbbb-086599a421cc` | 0.98 | auto-approved |
| `dx_codes` | `encounter_id` | `prompt-f7b81421-a523-4565-9db8-e1e2e87727d6` | 0.95 | auto-approved |
| `dx_codes` | `diagnosis_code` | `prompt-fe900418-52e8-4635-adfb-d58743a48691` | 0.75 | override |
| `dx_codes` | `dx_description` | `prompt-e536f024-9265-4a9f-b09a-7db6c568b5ed` | 0.4 | override |
| `dx_codes` | `is_primary_diagnosis` | `prompt-d0c7070d-444b-4f3b-ae17-25a8b95d53a4` | 0.95 | auto-approved |
| `enc_log` | `encounter_id` | `prompt-9518d07c-6c12-40ed-9414-4a0adfd8a7de` | 0.97 | auto-approved |
| `enc_log` | `patient_id` | `prompt-934ae664-3345-405b-bf72-e24f58369ddd` | 0.97 | auto-approved |
| `enc_log` | `provider_id` | `prompt-e0391554-06aa-4a6a-ac0d-01113414d83c` | 0.9 | auto-approved |
| `enc_log` | `encounter_type_code` | `prompt-8f345925-4ae4-4e19-a764-d91d374cf749` | 0.75 | override |
| `enc_log` | `encounter_datetime` | `prompt-9172369a-9996-4756-9078-b33da4beaf40` | 0.9 | auto-approved |
| `enc_log` | `bill_amount` | `prompt-518d3c04-2d2a-483b-b567-057841a139a1` | 0.85 | auto-approved |
| `enc_log` | `payment_status_code` | `prompt-517f25ef-6c7d-4790-84af-cd27b991d435` | 0.55 | override |
| `enc_log` | `notes` | `prompt-736f6837-3695-442f-997d-ea979a078726` | 0.55 | override |
| `patient_records` | `patient_id` | `prompt-4324004f-1b26-47d3-88ea-9bff5931746e` | 0.97 | auto-approved |
| `patient_records` | `patient_status_code` | `prompt-8068a6d5-188b-47b8-9ee4-b6a63e3750fe` | 0.55 | approve |
| `patient_records` | `first_name` | `prompt-a24d1546-e1dd-4d28-b1b7-84f16feea9a1` | 0.97 | auto-approved |
| `patient_records` | `last_name` | `prompt-a5b91c29-ec04-495e-ac4e-c4cef7f91cd1` | 0.0 | approve |
| `patient_records` | `date_of_birth` | `prompt-b02600c0-c9fb-45c0-81a4-2c142468c23f` | 0.97 | auto-approved |
| `patient_records` | `sex_code` | `prompt-4dd93300-a512-4c8c-9276-72f4bb60c38e` | 0.9 | auto-approved |
| `patient_records` | `admission_datetime` | `prompt-3a89d7f5-cc0e-44d0-ae4a-7f9535684b78` | 0.9 | auto-approved |
| `patient_records` | `discharge_datetime` | `prompt-525afece-25d5-4bd2-8b43-b03c9f12eb85` | 0.9 | auto-approved |
| `patient_records` | `created_ts` | `prompt-1b9d745a-8479-4187-a754-ff0c7b8e9e26` | 0.6 | override |
| `patient_records` | `updated_at` | `prompt-989e7c64-b2d1-4b3a-8df1-a35c29260f41` | 0.55 | approve |
| `prv_tbl` | `provider_id` | `prompt-05bc4c6f-5b3e-45e6-b202-f62a6220bd7b` | 0.95 | auto-approved |
| `prv_tbl` | `provider_name` | `prompt-6ce43495-ec06-4f6f-88fe-9bb71541c1f0` | 0.0 | approve |
| `prv_tbl` | `provider_type_code` | `prompt-41ab4057-1cb3-486a-b688-2bab1d2b396c` | 0.92 | auto-approved |
| `prv_tbl` | `dept_id` | `prompt-986a9671-95f0-4c17-825a-dfdec5c0a811` | 0.55 | override |
| `prv_tbl` | `npi_number` | `prompt-d3d430c5-08ac-4ce5-8198-095a434133a7` | 0.9 | auto-approved |

## Lineage

Every target column traces to exactly one source column via the transformation shown above. Full machine-readable lineage is in `audit/data_dictionary.json`; the authoritative rules are in `audit/transformation_rules.json`.
