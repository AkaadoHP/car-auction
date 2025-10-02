# risk_scoring.py
# Adds and maintains a comprehensive risk score for:
#   - tbl_next_24h
#   - tbl_next_2h
#   - tbl_live
#
# Scoring components (higher = riskier):
#   • Title (CLEAN, SAL, REB, COD, PARTS)
#   • Damage (Flood, Hail, Biohazard, Burn, Rollover, etc.) + secondary (reduced weight)
#   • Keys (missing keys penalty)
#   • Non-runner status penalty
#   • Mileage anomaly (odometer vs expected by age)
#   • Repair ratio (est_repair_cost / retail_value)
#   • Low images penalty (data confidence)
#
# Output columns created if missing:
#   risk_score (0..100), risk_flags (JSONB), risk_last_updated (timestamptz),
#   risk_mileage_ratio (numeric), risk_expected_miles (numeric),
#   risk_title_score, risk_damage_score, risk_keys_score, risk_nonrun_score,
#   risk_mileage_score, risk_repair_score, risk_images_score  (diagnostics)
#
# Refresh cadence:
#   - tbl_live: every 15s
#   - tbl_next_24h / tbl_next_2h: every 60s

import os
import time
from sqlalchemy import create_engine, text

POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=False, future=True)

SNAP_TABLES = ("tbl_next_24h", "tbl_next_2h", "tbl_live")

# ---- Tunable constants (adjust as you learn the market) ----
MILES_PER_YEAR = 12000.0  # expected average miles/year
# Image threshold for data confidence (few photos => higher risk)
MIN_IMAGE_COUNT = 5

# ---- Ensure columns exist on all snapshots ----
ALTERS = [
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_flags JSONB",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_last_updated TIMESTAMPTZ",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_mileage_ratio DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_expected_miles DOUBLE PRECISION",
    # Per-component diagnostics (optional but very helpful)
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_title_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_damage_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_keys_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_nonrun_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_mileage_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_repair_score DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS risk_images_score DOUBLE PRECISION",
]

def ensure_columns():
    with engine.begin() as conn:
        for t in SNAP_TABLES:
            for stmt in ALTERS:
                conn.execute(text(stmt.format(t=t)))

# ---- Core SQL scorer (set-based; fast & transparent) ----
SCORE_TMPL = """
WITH base AS (
  SELECT
    lot_id, broker_id, year, image_count, status, keys,
    COALESCE(title_norm, '') AS title_norm,
    COALESCE(damage_primary_norm, '') AS dmg1,
    COALESCE(damage_secondary_norm, '') AS dmg2,
    COALESCE(odometer_miles, NULL) AS odo_mi,
    COALESCE(retail_value, NULL) AS retail_val,
    COALESCE(est_repair_cost, NULL) AS repair_cost
  FROM {table}
),
feat AS (
  SELECT
    b.*,
    -- Age in years (>= 0.5 to avoid zero expected miles). Use mid-year bias.
    GREATEST(0.5, (EXTRACT(YEAR FROM now()) - b.year + 0.5))::double precision AS age_years,
    CASE
      WHEN b.year IS NULL THEN NULL
      ELSE (EXTRACT(YEAR FROM now()) - b.year + 0.5)
    END AS age_years_raw
  FROM base b
),
calc AS (
  SELECT
    f.*,
    -- Expected miles ~ age_years * MILES_PER_YEAR
    CASE
      WHEN f.age_years IS NULL THEN NULL
      ELSE f.age_years * {mpy}
    END AS expected_miles,
    -- Mileage ratio (observed/expected)
    CASE
      WHEN f.odo_mi IS NOT NULL AND f.age_years IS NOT NULL AND f.age_years > 0
        THEN f.odo_mi / (f.age_years * {mpy})
      ELSE NULL
    END AS mileage_ratio,
    -- Derived booleans
    (f.status ILIKE '%NON%RUN%' OR f.status ILIKE '%DOES NOT RUN%' OR f.status ILIKE '%NO START%') AS is_non_run,
    -- Repair ratio
    CASE
      WHEN f.repair_cost IS NOT NULL AND f.retail_val IS NOT NULL AND f.retail_val > 0
        THEN (f.repair_cost / f.retail_val)
      ELSE NULL
    END AS repair_ratio
  FROM feat f
),
title_score AS (
  SELECT
    c.lot_id, c.broker_id,
    CASE UPPER(c.title_norm)
      WHEN 'CLEAN'  THEN 0
      WHEN 'REB'    THEN 15    -- rebuilt
      WHEN 'SAL'    THEN 20    -- salvage
      WHEN 'COD'    THEN 80    -- certificate of destruction
      WHEN 'PARTS'  THEN 90    -- parts only
      ELSE 5                   -- unknown/other small penalty
    END AS sc_title
  FROM calc c
),
damage_score AS (
  SELECT
    c.lot_id, c.broker_id,
    -- Primary damage
    CASE UPPER(c.dmg1)
      WHEN 'FLOOD'          THEN 60
      WHEN 'BIOHAZARD'      THEN 60
      WHEN 'BURN'           THEN 80
      WHEN 'ROLLOVER'       THEN 50
      WHEN 'ALL OVER'       THEN 35
      WHEN 'HAIL'           THEN 25
      WHEN 'MECHANICAL'     THEN 20
      WHEN 'FRONT END'      THEN 20
      WHEN 'REAR END'       THEN 18
      WHEN 'SIDE'           THEN 15
      WHEN 'UNDERCARRIAGE'  THEN 25
      WHEN 'VANDALISM'      THEN 15
      WHEN 'MINOR DAMAGE'   THEN 5
      WHEN 'NORMAL WEAR'    THEN 0
      WHEN ''               THEN 5
      ELSE 10
    END
    +
    -- Secondary damage (reduced weight 50%)
    0.5 * CASE UPPER(c.dmg2)
      WHEN 'FLOOD'          THEN 60
      WHEN 'BIOHAZARD'      THEN 60
      WHEN 'BURN'           THEN 80
      WHEN 'ROLLOVER'       THEN 50
      WHEN 'ALL OVER'       THEN 35
      WHEN 'HAIL'           THEN 25
      WHEN 'MECHANICAL'     THEN 20
      WHEN 'FRONT END'      THEN 20
      WHEN 'REAR END'       THEN 18
      WHEN 'SIDE'           THEN 15
      WHEN 'UNDERCARRIAGE'  THEN 25
      WHEN 'VANDALISM'      THEN 15
      WHEN 'MINOR DAMAGE'   THEN 5
      WHEN 'NORMAL WEAR'    THEN 0
      WHEN ''               THEN 0
      ELSE 10
    END AS sc_damage
  FROM calc c
),
keys_score AS (
  SELECT
    c.lot_id, c.broker_id,
    CASE WHEN c.keys IS FALSE THEN 20 ELSE 0 END AS sc_keys
  FROM calc c
),
nonrun_score AS (
  SELECT
    c.lot_id, c.broker_id,
    CASE WHEN c.is_non_run THEN 25 ELSE 0 END AS sc_nonrun
  FROM calc c
),
mileage_score AS (
  SELECT
    c.lot_id, c.broker_id, c.mileage_ratio, c.expected_miles,
    CASE
      WHEN c.mileage_ratio IS NULL               THEN 5   -- unknown mileage: small risk
      WHEN c.mileage_ratio < 0.7                 THEN 5
      WHEN c.mileage_ratio >= 0.7 AND c.mileage_ratio < 1.2 THEN 10
      WHEN c.mileage_ratio >= 1.2 AND c.mileage_ratio < 1.6 THEN 20
      WHEN c.mileage_ratio >= 1.6 AND c.mileage_ratio < 2.0 THEN 30
      WHEN c.mileage_ratio >= 2.0 AND c.mileage_ratio < 3.0 THEN 40
      ELSE 50  -- extremely high mileage relative to age
    END AS sc_mileage
  FROM calc c
),
repair_score AS (
  SELECT
    c.lot_id, c.broker_id,
    CASE
      WHEN c.repair_ratio IS NULL               THEN 0
      WHEN c.repair_ratio < 0.3                 THEN 0
      WHEN c.repair_ratio >= 0.3 AND c.repair_ratio < 0.5 THEN 10
      WHEN c.repair_ratio >= 0.5 AND c.repair_ratio < 0.8 THEN 20
      WHEN c.repair_ratio >= 0.8 AND c.repair_ratio < 1.2 THEN 30
      ELSE 40
    END AS sc_repair
  FROM calc c
),
images_score AS (
  SELECT
    c.lot_id, c.broker_id,
    CASE
      WHEN c.image_count IS NULL THEN 5  -- uncertain
      WHEN c.image_count < {min_img} THEN 10
      ELSE 0
    END AS sc_images
  FROM calc c
),
sumup AS (
  SELECT
    c.lot_id, c.broker_id,
    t.sc_title,
    d.sc_damage,
    k.sc_keys,
    n.sc_nonrun,
    m.sc_mileage,
    r.sc_repair,
    i.sc_images,
    m.mileage_ratio,
    m.expected_miles,
    LEAST(
      100,
      -- weights: adjust as needed; they sum to a typical max ~100+
      t.sc_title
      + d.sc_damage
      + k.sc_keys
      + n.sc_nonrun
      + m.sc_mileage
      + r.sc_repair
      + i.sc_images
    ) AS total_score,
    jsonb_build_object(
      'title', t.sc_title,
      'damage', d.sc_damage,
      'keys', k.sc_keys,
      'nonrun', n.sc_nonrun,
      'mileage', m.sc_mileage,
      'repair_ratio_score', r.sc_repair,
      'images', i.sc_images
    ) AS flags
  FROM calc c
  JOIN title_score  t USING (lot_id, broker_id)
  JOIN damage_score d USING (lot_id, broker_id)
  JOIN keys_score   k USING (lot_id, broker_id)
  JOIN nonrun_score n USING (lot_id, broker_id)
  JOIN mileage_score m USING (lot_id, broker_id)
  JOIN repair_score  r USING (lot_id, broker_id)
  JOIN images_score  i USING (lot_id, broker_id)
)
UPDATE {table} dst
SET risk_score         = s.total_score,
    risk_flags         = s.flags,
    risk_last_updated  = now(),
    risk_mileage_ratio = s.mileage_ratio,
    risk_expected_miles= s.expected_miles,
    risk_title_score   = s.sc_title,
    risk_damage_score  = s.sc_damage,
    risk_keys_score    = s.sc_keys,
    risk_nonrun_score  = s.sc_nonrun,
    risk_mileage_score = s.sc_mileage,
    risk_repair_score  = s.sc_repair,
    risk_images_score  = s.sc_images
FROM sumup s
WHERE dst.lot_id = s.lot_id AND dst.broker_id = s.broker_id;
"""

def update_risk_for_table(table_name: str):
    sql = SCORE_TMPL.format(
        table=table_name,
        mpy=MILES_PER_YEAR,
        min_img=MIN_IMAGE_COUNT
    )
    with engine.begin() as conn:
        conn.execute(text(sql))

# ---- Main loop: 15s for LIVE, 60s for 24h/2h ----
def run_loop():
    ensure_columns()
    tick = 0
    while True:
        tick += 1

        # Always update LIVE every 15s
        update_risk_for_table("tbl_live")

        # Update 24h/2h every 4 ticks (~60s)
        if tick % 4 == 0:
            update_risk_for_table("tbl_next_24h")
            update_risk_for_table("tbl_next_2h")

        # Stats (optional)
        with engine.begin() as conn:
            c_live = conn.execute(text("SELECT COUNT(*) FROM tbl_live")).scalar()
            c_2h   = conn.execute(text("SELECT COUNT(*) FROM tbl_next_2h")).scalar()
            c_24h  = conn.execute(text("SELECT COUNT(*) FROM tbl_next_24h")).scalar()
        print(f"🔎 Risk refresh | live:{c_live}  2h:{c_2h}  24h:{c_24h}")

        time.sleep(15)  # 15-second cadence

if __name__ == "__main__":
    run_loop()
