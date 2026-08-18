-- ============================================================================
-- LEGACY SCHEMA — legacy_db
-- Simulates years of organic growth on a hospital ops system.
-- Deliberately messy: inconsistent naming, undocumented status codes,
-- nullable FKs, zero column comments (as a real legacy DB would have).
-- ============================================================================

SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------------------------------------------------------
-- dept_master — reference table, added early, never renamed
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dept_master (
    dept_id     INT AUTO_INCREMENT PRIMARY KEY,
    dept_nm     VARCHAR(100) NOT NULL,
    dept_loc    VARCHAR(50),
    active_flg  CHAR(1) DEFAULT 'Y'
);

-- ----------------------------------------------------------------------------
-- prv_tbl — providers. "prv" prefix never explained anywhere.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prv_tbl (
    prv_id      INT AUTO_INCREMENT PRIMARY KEY,
    prv_nm      VARCHAR(150) NOT NULL,
    prv_typ_cd  VARCHAR(2),                 -- undocumented: 'MD','DO','NP','PA'
    dept_id     INT NULL,                   -- nullable FK, no ON DELETE rule
    npi_num     VARCHAR(10),
    CONSTRAINT fk_prv_dept FOREIGN KEY (dept_id) REFERENCES dept_master(dept_id)
);

-- ----------------------------------------------------------------------------
-- patient_records — core table. pat_st_cd is the classic undocumented
-- status-code column referenced in the problem statement.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patient_records (
    pat_id       INT AUTO_INCREMENT PRIMARY KEY,
    pat_st_cd    VARCHAR(2),                -- undocumented: A/D/I/S
    fst_nm       VARCHAR(80),
    lst_nm       VARCHAR(80),
    dob          DATE,
    sex_cd       CHAR(1),                   -- undocumented: M/F/U
    admit_dt     DATETIME,
    discharge_dt DATETIME NULL,
    created_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts   TIMESTAMP NULL
);

-- ----------------------------------------------------------------------------
-- enc_log — encounters/visits. Mixes snake_case and abbreviations freely.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enc_log (
    enc_id      INT AUTO_INCREMENT PRIMARY KEY,
    pat_id      INT NOT NULL,
    prv_id      INT NULL,                   -- nullable FK: not every encounter has an assigned provider on record
    enc_typ_cd  VARCHAR(2),                  -- undocumented: IP/OP/ER/TC
    enc_dt      DATETIME NOT NULL,
    bill_amt    DECIMAL(10,2),
    pmt_st_cd   VARCHAR(1),                  -- undocumented: P/U/W/R
    notes_txt   TEXT,
    CONSTRAINT fk_enc_pat FOREIGN KEY (pat_id) REFERENCES patient_records(pat_id),
    CONSTRAINT fk_enc_prv FOREIGN KEY (prv_id) REFERENCES prv_tbl(prv_id)
);

-- ----------------------------------------------------------------------------
-- dx_codes — diagnosis codes tied to encounters. Sparse / inconsistent fill.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dx_codes (
    dx_id       INT AUTO_INCREMENT PRIMARY KEY,
    enc_id      INT NOT NULL,
    dx_cd       VARCHAR(10),                -- ICD-10-ish, sometimes malformed
    dx_desc     VARCHAR(255) NULL,          -- frequently NULL — never backfilled
    primary_flg CHAR(1) DEFAULT 'N',
    CONSTRAINT fk_dx_enc FOREIGN KEY (enc_id) REFERENCES enc_log(enc_id)
);

SET FOREIGN_KEY_CHECKS = 1;