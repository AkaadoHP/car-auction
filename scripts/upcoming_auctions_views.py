# auctions_snapshots.py
# Python 3.13
# Maintains physical snapshot tables:
#   - tbl_next_24h : auctions starting within next 24 hours
#   - tbl_next_2h  : auctions starting within next 2 hours
#
# Design:
#   • Physical tables (easy analytics)
#   • Every 30s: DELETE rows that no longer belong + UPSERT current rows
#   • Rich analytics columns + normalized fields for fast filtering
#   • Strong indexing (btree + trigram) for speed
#
# Phase-1 source tables (not modified): brokers, lots, prices_timeseries, sales_history

import time
import os
import pandas as pd
from sqlalchemy import create_engine, text

POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=True, future=True)

SNAPSHOT_TABLES = ["tbl_next_24h", "tbl_next_2h"]

LOTS_BASE_COLS = [
    "lot_id","broker_id","vin","year","make","model","trim","body","color",
    "auction_datetime_utc","yard","state","run_number","title_status","odometer",
    "damage_primary","damage_secondary","keys","engine","transmission","drive",
    "fuel","est_repair_cost","retail_value","images","status",
    "created_at","updated_at"
]

EXTRA_COLS = [
    "broker_name","image_count","hours_to_run","age_of_listing",
    "latest_prebid","latest_buy_now","latest_price_ts_utc","currency",
    "snapshot_refreshed_at",
    # normalized columns also inserted/updated
    "make_norm","model_norm","trim_norm","body_norm","color_norm",
    "state_norm","yard_norm","damage_primary_norm"
]

NORM_COLS = [
    ("make_norm","make"),("model_norm","model"),("trim_norm","trim"),
    ("body_norm","body"),("color_norm","color"),
    ("state_norm","state"),("yard_norm","yard"),
    ("damage_primary_norm","damage_primary"),
]

CREATE_EXTENSIONS_SQL = """
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

def _create_snapshot_table_sql(table_name: str) -> str:
    norm_defs = ",\n    ".join(f"{norm} TEXT" for norm, _ in NORM_COLS)
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    -- Identity
    lot_id TEXT NOT NULL,
    broker_id INT NOT NULL,

    -- From lots
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

    -- Analytics
    broker_name TEXT,
    image_count INT,
    hours_to_run DOUBLE PRECISION,
    age_of_listing DOUBLE PRECISION,
    latest_prebid NUMERIC,
    latest_buy_now NUMERIC,
    latest_price_ts_utc TIMESTAMPTZ,
    currency TEXT,
    snapshot_refreshed_at TIMESTAMPTZ DEFAULT now(),

    -- Normalized fields (plain text, filled during refresh)
    {norm_defs},

    PRIMARY KEY (lot_id, broker_id)
);
"""

CREATE_INDEXES_SQL = """
-- time/priority
CREATE INDEX IF NOT EXISTS {t}_idx_auction_dt ON {t} (auction_datetime_utc);
CREATE INDEX IF NOT EXISTS {t}_idx_hours_to_run ON {t} (hours_to_run);

-- ids/prices
CREATE INDEX IF NOT EXISTS {t}_idx_vin ON {t} (vin);
CREATE INDEX IF NOT EXISTS {t}_idx_year ON {t} (year);
CREATE INDEX IF NOT EXISTS {t}_idx_latest_prebid ON {t} (latest_prebid);
CREATE INDEX IF NOT EXISTS {t}_idx_latest_buynow ON {t} (latest_buy_now);
CREATE INDEX IF NOT EXISTS {t}_idx_odometer ON {t} (odometer);

-- composites
CREATE INDEX IF NOT EXISTS {t}_idx_make_model_year ON {t} (make_norm, model_norm, year);
CREATE INDEX IF NOT EXISTS {t}_idx_state_yard ON {t} (state_norm, yard_norm);
CREATE INDEX IF NOT EXISTS {t}_idx_status ON {t} (status);
CREATE INDEX IF NOT EXISTS {t}_idx_damage ON {t} (damage_primary_norm);

-- fuzzy search
CREATE INDEX IF NOT EXISTS {t}_gin_make_norm  ON {t} USING GIN (make_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS {t}_gin_model_norm ON {t} USING GIN (model_norm gin_trgm_ops);
"""

def ensure_schema():
    with engine.begin() as conn:
        conn.execute(text(CREATE_EXTENSIONS_SQL))
        for t in SNAPSHOT_TABLES:
            conn.execute(text(_create_snapshot_table_sql(t)))
        for t in SNAPSHOT_TABLES:
            conn.execute(text(CREATE_INDEXES_SQL.replace("{t}", t)))
    print("✅ 24h/2h tables & indexes ensured.")

INSERT_COLUMNS = LOTS_BASE_COLS + EXTRA_COLS

def _select_base(window_predicate_on_lots: str) -> str:
    norm_selects = ",\n        ".join(
        f"lower(unaccent(l.{src})) AS {norm}" for norm, src in NORM_COLS
    )
    return f"""
    SELECT
        l.lot_id, l.broker_id, l.vin, l.year, l.make, l.model, l.trim, l.body, l.color,
        l.auction_datetime_utc, l.yard, l.state, l.run_number, l.title_status, l.odometer,
        l.damage_primary, l.damage_secondary, l.keys, l.engine, l.transmission, l.drive,
        l.fuel, l.est_repair_cost, l.retail_value, l.images, l.status,
        l.created_at, l.updated_at,
        b.name AS broker_name,
        array_length(l.images, 1) AS image_count,
        EXTRACT(EPOCH FROM (l.auction_datetime_utc - now())) / 3600 AS hours_to_run,
        EXTRACT(EPOCH FROM (now() - l.created_at)) / 3600 AS age_of_listing,
        p.prebid AS latest_prebid,
        p.buy_now AS latest_buy_now,
        p.ts_utc AS latest_price_ts_utc,
        p.currency AS currency,
        now() AS snapshot_refreshed_at,
        {norm_selects}
    FROM lots l
    JOIN brokers b
      ON b.id = l.broker_id
    LEFT JOIN (
        SELECT DISTINCT ON (lot_id, broker_id)
            lot_id, broker_id, prebid, buy_now, ts_utc, currency
        FROM prices_timeseries
        ORDER BY lot_id, broker_id, ts_utc DESC
    ) p
      ON p.lot_id = l.lot_id AND p.broker_id = l.broker_id
    WHERE {window_predicate_on_lots}
    """

def _upsert_sql(table: str, window_predicate_on_target: str, window_predicate_on_lots: str) -> str:
    cols_csv = ", ".join(INSERT_COLUMNS)
    update_set = ", ".join([f"{c}=EXCLUDED.{c}" for c in INSERT_COLUMNS if c not in ["lot_id","broker_id"]])
    return f"""
    -- 1) Remove rows that no longer belong by checking target's own columns
    DELETE FROM {table}
    WHERE NOT ({window_predicate_on_target});

    -- 2) Upsert current set from source (lots + latest prices + normalized fields)
    INSERT INTO {table} ({cols_csv})
    { _select_base(window_predicate_on_lots) }
    ON CONFLICT (lot_id, broker_id) DO UPDATE SET
        {update_set};
    """

def refresh_next_24h():
    target_pred = "auction_datetime_utc BETWEEN now() AND (now() + interval '24 hours')"
    source_pred = "l.auction_datetime_utc BETWEEN now() AND (now() + interval '24 hours')"
    with engine.begin() as conn:
        conn.execute(text(_upsert_sql("tbl_next_24h", target_pred, source_pred)))

def refresh_next_2h():
    target_pred = "auction_datetime_utc BETWEEN now() AND (now() + interval '2 hours')"
    source_pred = "l.auction_datetime_utc BETWEEN now() AND (now() + interval '2 hours')"
    with engine.begin() as conn:
        conn.execute(text(_upsert_sql("tbl_next_2h", target_pred, source_pred)))

def get_next_24h():
    return pd.read_sql("SELECT * FROM tbl_next_24h ORDER BY hours_to_run ASC;", engine)

def get_next_2h():
    return pd.read_sql("SELECT * FROM tbl_next_2h ORDER BY hours_to_run ASC;", engine)

def run_loop(interval_seconds=30):
    ensure_schema()
    while True:
        refresh_next_24h()
        refresh_next_2h()
        with engine.begin() as conn:
            c24 = conn.execute(text("SELECT count(*) FROM tbl_next_24h")).scalar()
            c2  = conn.execute(text("SELECT count(*) FROM tbl_next_2h")).scalar()
        print(f"⏰ Snapshot refresh | 2h: {c2} | 24h: {c24}")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_loop(interval_seconds=30)
