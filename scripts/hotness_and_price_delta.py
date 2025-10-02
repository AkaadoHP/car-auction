# analytics_phase5.py
# Computes and updates:
#   - hotness_pct (0..100) per (make_norm, model_norm, year)
#   - fair_value and price_delta_pct per lot
# Sources: sales_history (history), tbl_next_24h / tbl_next_2h / tbl_live (current), prices_timeseries (optional velocity)
#
# Run continuously: python analytics_phase5.py

#######################
# CAN BE DIVIDED INTO 2 LAYERS IF TOO MUCH DATA TO HANDLE  HISTORY vs CURRENT
#######################

import os
import time
from sqlalchemy import create_engine, text

POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=False, future=True)

SNAP_TABLES = ("tbl_next_24h", "tbl_next_2h", "tbl_live")

# ---------- 1) Ensure required columns exist on all snapshot tables ----------
ALTERS = [
    # Per-lot price ladder outputs
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS fair_value NUMERIC",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS comp_count INTEGER",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS price_delta_pct DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS price_last_updated TIMESTAMPTZ",

    # Per-segment hotness
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS hotness_pct DOUBLE PRECISION",
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS hotness_last_updated TIMESTAMPTZ",

    # (Optional) diagnostic columns
    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS hotness_components JSONB"
]

def ensure_columns():
    with engine.begin() as conn:
        for t in SNAP_TABLES:
            for stmt in ALTERS:
                conn.execute(text(stmt.format(t=t)))

# ---------- 2) Compute & update segment Hotness % ----------
HOTNESS_SQL = """
WITH
-- segments we care about (from history + current)
seg AS (
  SELECT DISTINCT lower(unaccent(make)) AS make_norm, lower(unaccent(model)) AS model_norm, year
  FROM sales_history
  WHERE sale_date_utc >= now() - interval '365 days'
  UNION
  SELECT make_norm, model_norm, year FROM tbl_next_24h
  UNION
  SELECT make_norm, model_norm, year FROM tbl_live
),

hist12 AS (
  SELECT lower(unaccent(make)) AS make_norm, lower(unaccent(model)) AS model_norm, year,
         COUNT(*)::numeric AS c12
  FROM sales_history
  WHERE sale_date_utc >= now() - interval '365 days'
  GROUP BY 1,2,3
),
hist30 AS (
  SELECT lower(unaccent(make)) AS make_norm, lower(unaccent(model)) AS model_norm, year,
         COUNT(*)::numeric AS c30
  FROM sales_history
  WHERE sale_date_utc >= now() - interval '30 days'
  GROUP BY 1,2,3
),

curr AS (
  SELECT make_norm, model_norm, year, COUNT(*)::numeric AS cnow
  FROM (
    SELECT make_norm, model_norm, year FROM tbl_next_24h
    UNION ALL
    SELECT make_norm, model_norm, year FROM tbl_live
  ) x
  GROUP BY 1,2,3
),

-- optional: bid velocity ($/h) from last 24h time-series, only for lots currently in 24h or live
lot_set AS (
  SELECT lot_id, broker_id, make_norm, model_norm, year
  FROM tbl_next_24h
  UNION
  SELECT lot_id, broker_id, make_norm, model_norm, year
  FROM tbl_live
),
vel_lot AS (
  SELECT
    p.lot_id, p.broker_id,
    (MAX(p.prebid) - MIN(p.prebid)) / NULLIF(EXTRACT(EPOCH FROM (MAX(p.ts_utc) - MIN(p.ts_utc))) / 3600.0, 0) AS vel_per_h
  FROM prices_timeseries p
  JOIN lot_set s ON (s.lot_id = p.lot_id AND s.broker_id = p.broker_id)
  WHERE p.ts_utc >= now() - interval '24 hours'
  GROUP BY p.lot_id, p.broker_id
),
vel_seg AS (
  SELECT s.make_norm, s.model_norm, s.year,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v.vel_per_h) AS vel_median
  FROM vel_lot v
  JOIN lot_set s ON (s.lot_id = v.lot_id AND s.broker_id = v.broker_id)
  GROUP BY 1,2,3
),

features AS (
  SELECT
    g.make_norm, g.model_norm, g.year,
    COALESCE(h30.c30, 0) AS c30,
    COALESCE(h12.c12, 0) AS c12,
    COALESCE(cur.cnow, 0) AS cnow,
    /* month-vs-year rate ratio */
    CASE
      WHEN COALESCE(h12.c12,0) > 0
        THEN ((COALESCE(h30.c30,0) / 30.0) / (h12.c12 / 365.0))
      ELSE NULL
    END AS rate_ratio,
    COALESCE(vs.vel_median, NULL) AS vel_median
  FROM seg g
  LEFT JOIN hist30 h30 USING (make_norm, model_norm, year)
  LEFT JOIN hist12 h12 USING (make_norm, model_norm, year)
  LEFT JOIN curr  cur USING (make_norm, model_norm, year)
  LEFT JOIN vel_seg vs USING (make_norm, model_norm, year)
),

scored AS (
  SELECT
    f.*,
    -- Percentile ranks per feature (neutral=0.5 if NULL)
    COALESCE(CUME_DIST() OVER (ORDER BY rate_ratio), 0.5)  AS pct_ratio,
    COALESCE(CUME_DIST() OVER (ORDER BY cnow),       0.5)  AS pct_activity,
    COALESCE(CUME_DIST() OVER (ORDER BY vel_median), 0.5)  AS pct_velocity
  FROM features f
),

hot AS (
  SELECT
    make_norm, model_norm, year,
    /* weights: ratio 0.5, activity 0.4, velocity 0.1 */
    ROUND(100.0 * (0.5 * pct_ratio + 0.4 * pct_activity + 0.1 * pct_velocity))::int AS hotness_pct,
    jsonb_build_object(
      'c30', c30, 'c12', c12, 'cnow', cnow,
      'rate_ratio', rate_ratio,
      'vel_median', vel_median,
      'pct_ratio', pct_ratio,
      'pct_activity', pct_activity,
      'pct_velocity', pct_velocity
    ) AS components
  FROM scored
)
SELECT * FROM hot;
"""

def update_hotness():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS tmp_hotness"))
        conn.execute(text("CREATE TEMP TABLE tmp_hotness AS " + HOTNESS_SQL))

        for t in SNAP_TABLES:
            conn.execute(text(f"""
                UPDATE {t} dst
                SET hotness_pct = h.hotness_pct,
                    hotness_last_updated = now(),
                    hotness_components = h.components
                FROM tmp_hotness h
                WHERE dst.make_norm = h.make_norm
                  AND dst.model_norm = h.model_norm
                  AND dst.year = h.year
            """))

# ---------- 3) Compute & update per-lot Price Delta % ----------
PRICE_UPDATE_TMPL = """
WITH target AS (
  SELECT lot_id, broker_id, make_norm, model_norm, year, title_norm, odometer_miles, latest_prebid
  FROM {table}
),
comps_a AS (
  SELECT
    t.lot_id, t.broker_id,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s.sale_price) AS fair_value,
    COUNT(*) AS comp_count
  FROM target t
  JOIN sales_history s
    ON lower(unaccent(s.make))  = t.make_norm
   AND lower(unaccent(s.model)) = t.model_norm
   AND ABS(s.year - t.year) <= 1
   AND (t.title_norm IS NULL OR s.title_norm IS NULL OR s.title_norm = t.title_norm)
   AND s.sale_price IS NOT NULL
   AND (
        t.odometer_miles IS NULL OR s.odometer_miles IS NULL OR
        (s.odometer_miles BETWEEN t.odometer_miles*0.8 AND t.odometer_miles*1.2)
   )
  GROUP BY t.lot_id, t.broker_id
),
comps_b AS (
  SELECT
    t.lot_id, t.broker_id,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s.sale_price) AS fair_value,
    COUNT(*) AS comp_count
  FROM target t
  JOIN sales_history s
    ON lower(unaccent(s.make))  = t.make_norm
   AND lower(unaccent(s.model)) = t.model_norm
   AND ABS(s.year - t.year) <= 2
   AND s.sale_price IS NOT NULL
   AND (
        t.odometer_miles IS NULL OR s.odometer_miles IS NULL OR
        (s.odometer_miles BETWEEN t.odometer_miles*0.6 AND t.odometer_miles*1.4)
   )
  GROUP BY t.lot_id, t.broker_id
),
pick AS (
  SELECT
    t.lot_id, t.broker_id,
    COALESCE(a.fair_value, b.fair_value) AS fair_value,
    COALESCE(a.comp_count, b.comp_count, 0) AS comp_count
  FROM target t
  LEFT JOIN comps_a a USING (lot_id, broker_id)
  LEFT JOIN comps_b b USING (lot_id, broker_id)
),
upd AS (
  SELECT
    p.lot_id, p.broker_id,
    p.fair_value, p.comp_count,
    CASE WHEN p.fair_value IS NOT NULL AND p.fair_value > 0
      THEN 100.0 * (t.latest_prebid - p.fair_value) / p.fair_value
      ELSE NULL
    END AS price_delta_pct
  FROM pick p
  JOIN {table} t USING (lot_id, broker_id)
)
UPDATE {table} dst
SET fair_value = u.fair_value,
    comp_count = u.comp_count,
    price_delta_pct = u.price_delta_pct,
    price_last_updated = now()
FROM upd u
WHERE dst.lot_id = u.lot_id AND dst.broker_id = u.broker_id;
"""

def update_price_delta():
    with engine.begin() as conn:
        for t in SNAP_TABLES:
            conn.execute(text(PRICE_UPDATE_TMPL.format(table=t)))

# ---------- 4) Main loop with different refresh rates ----------
def run_loop():
    ensure_columns()
    counter = 0
    while True:
        counter += 1

        # Every cycle (default ~15s for live, 60s for others)
        if counter % 1 == 0:  # every loop → tbl_live
            update_hotness()
            update_price_delta()

        if counter % 4 == 0:  # every 4 loops (~60s) → tbl_next_24h, tbl_next_2h
            update_hotness()
            update_price_delta()

        # quick stats
        with engine.begin() as conn:
            counts = []
            for t in SNAP_TABLES:
                r = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                counts.append(f"{t}:{r}")
        print("✅ Phase5 refresh:", ", ".join(counts))

        time.sleep(15)  # loop every 15 seconds

if __name__ == "__main__":
    run_loop()
