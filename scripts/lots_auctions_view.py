# live_snapshots.py
# Python 3.13
# Maintains tbl_live (cars currently running) as a separate, faster loop.
# Compatible with Phase 3 schema (plain TEXT *_norm columns, populated at refresh)

import time
import os
import pandas as pd
from sqlalchemy import create_engine, text

POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=True, future=True)

CREATE_EXTENSIONS_SQL = """
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

CREATE_TBL_LIVE_SQL = """
CREATE TABLE IF NOT EXISTS tbl_live (
    lot_id TEXT NOT NULL,
    broker_id INT NOT NULL,

    vin TEXT,
    year INT,
    make TEXT,
    model TEXT,
    trim TEXT,
    body TEXT,
    color TEXT,
    auction_datetime_utc TIMESTAMPTZ,
    yard TEXT,
    state TEXT,
    run_number TEXT,
    title_status TEXT,
    odometer NUMERIC,
    damage_primary TEXT,
    damage_secondary TEXT,
    keys BOOLEAN,
    engine TEXT,
    transmission TEXT,
    drive TEXT,
    fuel TEXT,
    est_repair_cost NUMERIC,
    retail_value NUMERIC,
    images TEXT[],
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,

    broker_name TEXT,
    image_count INT,
    hours_to_run DOUBLE PRECISION,
    age_of_listing DOUBLE PRECISION,
    latest_prebid NUMERIC,
    latest_buy_now NUMERIC,
    latest_price_ts_utc TIMESTAMPTZ,
    currency TEXT,
    snapshot_refreshed_at TIMESTAMPTZ DEFAULT now(),

    -- normalized fields (plain TEXT, updated at refresh time)
    make_norm TEXT,
    model_norm TEXT,
    trim_norm TEXT,
    body_norm TEXT,
    color_norm TEXT,
    state_norm TEXT,
    yard_norm TEXT,
    damage_primary_norm TEXT,

    PRIMARY KEY (lot_id, broker_id)
);
"""

CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS live_idx_auction_dt ON tbl_live (auction_datetime_utc);
CREATE INDEX IF NOT EXISTS live_idx_hours_to_run ON tbl_live (hours_to_run);
CREATE INDEX IF NOT EXISTS live_idx_vin ON tbl_live (vin);
CREATE INDEX IF NOT EXISTS live_idx_year ON tbl_live (year);
CREATE INDEX IF NOT EXISTS live_idx_latest_prebid ON tbl_live (latest_prebid);
CREATE INDEX IF NOT EXISTS live_idx_latest_buynow ON tbl_live (latest_buy_now);
CREATE INDEX IF NOT EXISTS live_idx_odometer ON tbl_live (odometer);
CREATE INDEX IF NOT EXISTS live_idx_make_model_year ON tbl_live (make_norm, model_norm, year);
CREATE INDEX IF NOT EXISTS live_idx_state_yard ON tbl_live (state_norm, yard_norm);
CREATE INDEX IF NOT EXISTS live_idx_status ON tbl_live (status);
CREATE INDEX IF NOT EXISTS live_idx_damage ON tbl_live (damage_primary_norm);
CREATE INDEX IF NOT EXISTS live_gin_make_norm  ON tbl_live USING GIN (make_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS live_gin_model_norm ON tbl_live USING GIN (model_norm gin_trgm_ops);
"""

SELECT_LIVE_CANDIDATES = """
SELECT
    l.lot_id, l.broker_id, l.vin, l.year, l.make, l.model, l.trim, l.body, l.color,
    l.auction_datetime_utc, l.yard, l.state, l.run_number, l.title_status, l.odometer,
    l.damage_primary, l.damage_secondary, l.keys, l.engine, l.transmission, l.drive,
    l.fuel, l.est_repair_cost, l.retail_value, l.images, l.status,
    l.created_at, l.updated_at,
    b.name AS broker_name,
    array_length(l.images, 1) AS image_count,
    0.0 AS hours_to_run,  -- live → already started
    EXTRACT(EPOCH FROM (now() - l.created_at)) / 3600 AS age_of_listing,
    p.prebid AS latest_prebid,
    p.buy_now AS latest_buy_now,
    p.ts_utc AS latest_price_ts_utc,
    p.currency AS currency,
    now() AS snapshot_refreshed_at,

    -- normalized fields computed at refresh
    lower(unaccent(l.make)) AS make_norm,
    lower(unaccent(l.model)) AS model_norm,
    lower(unaccent(l.trim)) AS trim_norm,
    lower(unaccent(l.body)) AS body_norm,
    lower(unaccent(l.color)) AS color_norm,
    lower(unaccent(l.state)) AS state_norm,
    lower(unaccent(l.yard)) AS yard_norm,
    lower(unaccent(l.damage_primary)) AS damage_primary_norm

FROM lots l
JOIN brokers b ON b.id = l.broker_id
LEFT JOIN (
    SELECT DISTINCT ON (lot_id, broker_id)
        lot_id, broker_id, prebid, buy_now, ts_utc, currency
    FROM prices_timeseries
    ORDER BY lot_id, broker_id, ts_utc DESC
) p ON p.lot_id = l.lot_id AND p.broker_id = l.broker_id
WHERE l.auction_datetime_utc <= now()
  AND l.status NOT IN ('SOLD','CLOSED');
"""

def ensure_schema():
    with engine.begin() as conn:
        conn.execute(text(CREATE_EXTENSIONS_SQL))
        conn.execute(text(CREATE_TBL_LIVE_SQL))
        conn.execute(text(CREATE_INDEXES_SQL))
    print("✅ tbl_live & indexes ensured.")

def refresh_live_once():
    """
    Efficient refresh:
      1) Build candidates into a TEMP table
      2) DELETE rows from tbl_live that are no longer live
      3) MERGE candidates into tbl_live (insert/update only changed/new)
    """
    with engine.begin() as conn:
        # 1) temp candidates
        conn.execute(text("DROP TABLE IF EXISTS tmp_live_candidates;"))
        conn.execute(text("CREATE TEMP TABLE tmp_live_candidates AS " + SELECT_LIVE_CANDIDATES))

        # 2) delete non-live rows
        conn.execute(text("""
            DELETE FROM tbl_live t
            WHERE NOT EXISTS (
                SELECT 1 FROM tmp_live_candidates s
                WHERE s.lot_id = t.lot_id AND s.broker_id = t.broker_id
            );
        """))

        # 3) merge upserts (requires Postgres 15+)
        conn.execute(text("""
            MERGE INTO tbl_live AS t
            USING tmp_live_candidates AS s
            ON (t.lot_id = s.lot_id AND t.broker_id = s.broker_id)
            WHEN MATCHED THEN UPDATE SET
                vin = s.vin, year = s.year, make = s.make, model = s.model, trim = s.trim,
                body = s.body, color = s.color, auction_datetime_utc = s.auction_datetime_utc,
                yard = s.yard, state = s.state, run_number = s.run_number, title_status = s.title_status,
                odometer = s.odometer, damage_primary = s.damage_primary, damage_secondary = s.damage_secondary,
                keys = s.keys, engine = s.engine, transmission = s.transmission, drive = s.drive,
                fuel = s.fuel, est_repair_cost = s.est_repair_cost, retail_value = s.retail_value,
                images = s.images, status = s.status, created_at = s.created_at, updated_at = s.updated_at,
                broker_name = s.broker_name, image_count = s.image_count, hours_to_run = s.hours_to_run,
                age_of_listing = s.age_of_listing, latest_prebid = s.latest_prebid, latest_buy_now = s.latest_buy_now,
                latest_price_ts_utc = s.latest_price_ts_utc, currency = s.currency,
                snapshot_refreshed_at = s.snapshot_refreshed_at,
                make_norm = s.make_norm, model_norm = s.model_norm, trim_norm = s.trim_norm,
                body_norm = s.body_norm, color_norm = s.color_norm, state_norm = s.state_norm,
                yard_norm = s.yard_norm, damage_primary_norm = s.damage_primary_norm
            WHEN NOT MATCHED THEN INSERT (
                lot_id, broker_id, vin, year, make, model, trim, body, color, auction_datetime_utc, yard, state,
                run_number, title_status, odometer, damage_primary, damage_secondary, keys, engine, transmission, drive,
                fuel, est_repair_cost, retail_value, images, status, created_at, updated_at,
                broker_name, image_count, hours_to_run, age_of_listing, latest_prebid, latest_buy_now,
                latest_price_ts_utc, currency, snapshot_refreshed_at,
                make_norm, model_norm, trim_norm, body_norm, color_norm, state_norm, yard_norm, damage_primary_norm
            ) VALUES (
                s.lot_id, s.broker_id, s.vin, s.year, s.make, s.model, s.trim, s.body, s.color, s.auction_datetime_utc, s.yard, s.state,
                s.run_number, s.title_status, s.odometer, s.damage_primary, s.damage_secondary, s.keys, s.engine, s.transmission, s.drive,
                s.fuel, s.est_repair_cost, s.retail_value, s.images, s.status, s.created_at, s.updated_at,
                s.broker_name, s.image_count, s.hours_to_run, s.age_of_listing, s.latest_prebid, s.latest_buy_now,
                s.latest_price_ts_utc, s.currency, s.snapshot_refreshed_at,
                s.make_norm, s.model_norm, s.trim_norm, s.body_norm, s.color_norm, s.state_norm, s.yard_norm, s.damage_primary_norm
            );
        """))

def get_live():
    return pd.read_sql("SELECT * FROM tbl_live;", engine)

def run_loop(interval_seconds=10):
    ensure_schema()
    while True:
        refresh_live_once()
        with engine.begin() as conn:
            cnt = conn.execute(text("SELECT count(*) FROM tbl_live")).scalar()
        print(f"⚡ LIVE refresh | rows: {cnt}")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_loop(interval_seconds=10)
