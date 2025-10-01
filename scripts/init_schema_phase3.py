# init_phase3.py
# Creates only Phase 3 snapshot tables for monitoring:
#   - tbl_next_24h : auctions starting within next 24 hours
#   - tbl_next_2h  : auctions starting within next 2 hours
#   - tbl_live     : auctions currently running
#
# Each table has:
#   - full lot details
#   - analytics columns (hours_to_run, image_count, etc.)
#   - normalized fields for fast filtering (as plain TEXT columns)
#   - strong indexes (btree + trigram)

import os
from sqlalchemy import create_engine, text

# ✅ Update DB connection string if needed
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=True, future=True)

# Extensions required for normalization & fuzzy search
CREATE_EXTENSIONS = """
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

# Normalized columns (plain text, no GENERATED ALWAYS)
NORM_COLS = [
    "make_norm",
    "model_norm",
    "trim_norm",
    "body_norm",
    "color_norm",
    "state_norm",
    "yard_norm",
    "damage_primary_norm"
]

NORM_DEF = ",\n    ".join(f"{n} TEXT" for n in NORM_COLS)

# Base snapshot schema
SNAPSHOT_BASE = f"""
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

    -- analytics
    broker_name TEXT,
    image_count INT,
    hours_to_run DOUBLE PRECISION,
    age_of_listing DOUBLE PRECISION,
    latest_prebid NUMERIC,
    latest_buy_now NUMERIC,
    latest_price_ts_utc TIMESTAMPTZ,
    currency TEXT,
    snapshot_refreshed_at TIMESTAMPTZ DEFAULT now(),

    -- normalized fields (plain text, filled during refresh)
    {NORM_DEF},

    PRIMARY KEY (lot_id, broker_id)
"""

CREATE_TBL_24H  = f"CREATE TABLE IF NOT EXISTS tbl_next_24h (\n{SNAPSHOT_BASE}\n);"
CREATE_TBL_2H   = f"CREATE TABLE IF NOT EXISTS tbl_next_2h  (\n{SNAPSHOT_BASE}\n);"
CREATE_TBL_LIVE = f"CREATE TABLE IF NOT EXISTS tbl_live     (\n{SNAPSHOT_BASE}\n);"

# Index template
INDEXES_TMPL = """
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

def run():
    with engine.begin() as conn:
        conn.execute(text(CREATE_EXTENSIONS))

        conn.execute(text(CREATE_TBL_24H))
        conn.execute(text(CREATE_TBL_2H))
        conn.execute(text(CREATE_TBL_LIVE))

        for t in ("tbl_next_24h", "tbl_next_2h", "tbl_live"):
            conn.execute(text(INDEXES_TMPL.replace("{t}", t)))

    print("✅ Phase 3 snapshot tables created successfully:")
    print("   tbl_next_24h, tbl_next_2h, tbl_live")

if __name__ == "__main__":
    run()
