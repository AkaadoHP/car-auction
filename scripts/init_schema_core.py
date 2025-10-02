import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, Text, Numeric, Boolean,
    TIMESTAMP, ForeignKey, BigInteger, ARRAY, text
)
from sqlalchemy.dialects.postgresql import JSON
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 1. Load .env for DB connection
# ─────────────────────────────────────────────
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)

engine = create_engine(POSTGRES_URI, echo=True, future=True)
metadata = MetaData()

# ─────────────────────────────────────────────
# 2. Define Tables
# ─────────────────────────────────────────────

# Brokers (list of auction sources)
brokers = Table(
    "brokers", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, unique=True, nullable=False)
)

# Raw JSON dumps
lots_raw = Table(
    "lots_raw", metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("broker_id", Integer, ForeignKey("brokers.id")),
    Column("fetched_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("payload", JSON, nullable=False)
)

# Normalized lots
lots = Table(
    "lots", metadata,
    Column("lot_id", Text, primary_key=True, nullable=False),
    Column("broker_id", Integer, ForeignKey("brokers.id"), primary_key=True, nullable=False),
    Column("vin", Text),
    Column("year", Integer),
    Column("make", Text),
    Column("model", Text),
    Column("trim", Text),
    Column("body", Text),
    Column("color", Text),
    Column("auction_datetime_utc", TIMESTAMP(timezone=True)),
    Column("yard", Text),
    Column("state", Text),
    Column("run_number", Text),

    # Titles
    Column("title_status", Text),        # raw Copart title
    Column("title_norm", Text),          # normalized category (SAL, REB, CLEAN, etc.)

    # Odometer
    Column("odometer", Numeric),         # raw Copart value
    Column("odometer_miles", Numeric),   # normalized to miles
    Column("odometer_km", Numeric),      # normalized to km

    # Damages
    Column("damage_primary", Text),          # raw Copart value
    Column("damage_primary_norm", Text),     # normalized category
    Column("damage_secondary", Text),        # raw Copart value
    Column("damage_secondary_norm", Text),   # normalized category

    Column("keys", Boolean),
    Column("engine", Text),
    Column("transmission", Text),
    Column("drive", Text),
    Column("fuel", Text),
    Column("est_repair_cost", Numeric),
    Column("retail_value", Numeric),
    Column("images", ARRAY(Text)),
    Column("status", Text),

    Column("created_at", TIMESTAMP(timezone=True), server_default=text("NOW()")),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=text("NOW()"))
)

# Prices over time
prices_timeseries = Table(
    "prices_timeseries", metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("lot_id", Text, nullable=False),
    Column("broker_id", Integer, ForeignKey("brokers.id")),
    Column("ts_utc", TIMESTAMP(timezone=True), nullable=False),
    Column("prebid", Numeric),
    Column("buy_now", Numeric),
    Column("currency", Text)
)

# Final sales history (expanded with full specs for analytics/comps)
sales_history = Table(
    "sales_history", metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("lot_id", Text),
    Column("vin", Text),
    Column("broker_id", Integer, ForeignKey("brokers.id")),

    # Sale details
    Column("sale_date_utc", TIMESTAMP(timezone=True)),
    Column("sale_price", Numeric),
    Column("sale_type", Text),
    Column("high_bid", Numeric),
    Column("pure_sale_flag", Boolean),

    # Vehicle specs (copied from lots at time of sale)
    Column("year", Integer),
    Column("make", Text),
    Column("model", Text),
    Column("trim", Text),
    Column("body", Text),
    Column("color", Text),
    Column("yard", Text),
    Column("state", Text),

    # Titles
    Column("title_status", Text),
    Column("title_norm", Text),

    # Odometer
    Column("odometer", Numeric),
    Column("odometer_miles", Numeric),
    Column("odometer_km", Numeric),

    # Damages
    Column("damage_primary", Text),
    Column("damage_primary_norm", Text),
    Column("damage_secondary", Text),
    Column("damage_secondary_norm", Text),

    Column("keys", Boolean),
    Column("engine", Text),
    Column("transmission", Text),
    Column("drive", Text),
    Column("fuel", Text),
    Column("est_repair_cost", Numeric),
    Column("retail_value", Numeric),
    Column("images", ARRAY(Text)),

    Column("created_at", TIMESTAMP(timezone=True), server_default=text("NOW()"))
)

# ─────────────────────────────────────────────
# 3. Create Tables & Indexes
# ─────────────────────────────────────────────
def init_db():
    metadata.create_all(engine)

    with engine.begin() as conn:
        # Create indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lots_auctiondate ON lots (auction_datetime_utc)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lots_vin ON lots (vin)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lots_make_model_year ON lots (make, model, year)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prices_lot_broker ON prices_timeseries (lot_id, broker_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sales_vin ON sales_history (vin)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sales_make_model_year ON sales_history (make, model, year)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_history (sale_date_utc)"))

        # Seed Copart broker if not exists
        conn.execute(text("INSERT INTO brokers (name) VALUES ('COPART') ON CONFLICT (name) DO NOTHING"))

    print("✅ Schema created with extended sales_history for full analytics + Copart broker inserted.")


if __name__ == "__main__":
    init_db()
