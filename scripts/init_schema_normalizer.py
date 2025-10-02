########################

#to be added to fetcher later on

########################
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 1. Load .env and DB connection
# ─────────────────────────────────────────────
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:A31242001a@localhost:5432/car-auctions"
)
engine = create_engine(POSTGRES_URI, echo=True, future=True)

# ─────────────────────────────────────────────
# 2. Normalization dictionaries
# ─────────────────────────────────────────────

TITLE_MAP = {
    "SALVAGE": "SAL",
    "SALVAGE TITLE": "SAL",
    "REBUILT": "REB",
    "CLEAN": "CLEAN",
    "CERTIFICATE OF DESTRUCTION": "COD",
    "PARTS ONLY": "PARTS"
}

DAMAGE_MAP = {
    "FRONT END": "Front End",
    "REAR END": "Rear End",
    "SIDE": "Side",
    "ALL OVER": "All Over",
    "TOP/ROOF": "Roof",
    "HAIL": "Hail",
    "FLOOD": "Flood",
    "BIOHAZARD/CHEMICAL": "Biohazard",
    "MECHANICAL": "Mechanical",
    "BURN - ENGINE": "Burn",
    "BURN - INTERIOR": "Burn",
    "MINOR DENT/SCRATCHES": "Minor Damage",
    "NORMAL WEAR": "Normal Wear",
    "ROLLOVER": "Rollover",
    "VANDALISM": "Vandalism",
    "UNDERCARRIAGE": "Undercarriage",
    "STRIPPED": "Stripped"
}

# ─────────────────────────────────────────────
# 3. Helper: odometer normalization
# ─────────────────────────────────────────────
def normalize_odometer(value, unit="MI"):
    """
    Normalize odometer to miles and km.
    Default Copart unit = miles (MI).
    """
    if value is None:
        return None, None
    try:
        val = float(value)
    except (ValueError, TypeError):
        return None, None

    if unit.upper() in ("MI", "MILES"):
        return val, val * 1.60934
    elif unit.upper() in ("KM", "KILOMETERS"):
        return val / 1.60934, val
    else:
        return None, None

# ─────────────────────────────────────────────
# 4. Insert / Update lots with normalized data
# ─────────────────────────────────────────────
def insert_normalized_lot(lot: dict):
    """
    Insert or update one lot into the lots table with normalized fields.
    Expects dict with Copart fields.
    """
    # Normalize odometer
    odo_mi, odo_km = normalize_odometer(lot.get("odometer"), lot.get("odometer_unit", "MI"))

    # Normalize title
    raw_title = lot.get("title_status", "")
    title_norm = TITLE_MAP.get(raw_title.upper(), raw_title)

    # Normalize damages
    raw_dmg1 = lot.get("damage_primary", "")
    raw_dmg2 = lot.get("damage_secondary", "")
    dmg1_norm = DAMAGE_MAP.get(raw_dmg1.upper(), raw_dmg1)
    dmg2_norm = DAMAGE_MAP.get(raw_dmg2.upper(), raw_dmg2)

    sql = text("""
        INSERT INTO lots (
            lot_id, broker_id, vin, year, make, model, trim, body, color,
            auction_datetime_utc, yard, state, run_number,
            title_status, title_norm,
            odometer, odometer_miles, odometer_km,
            damage_primary, damage_primary_norm,
            damage_secondary, damage_secondary_norm,
            keys, engine, transmission, drive, fuel,
            est_repair_cost, retail_value, images, status,
            created_at, updated_at
        )
        VALUES (
            :lot_id, :broker_id, :vin, :year, :make, :model, :trim, :body, :color,
            :auction_datetime_utc, :yard, :state, :run_number,
            :title_status, :title_norm,
            :odometer, :odometer_miles, :odometer_km,
            :damage_primary, :damage_primary_norm,
            :damage_secondary, :damage_secondary_norm,
            :keys, :engine, :transmission, :drive, :fuel,
            :est_repair_cost, :retail_value, :images, :status,
            NOW(), NOW()
        )
        ON CONFLICT (lot_id, broker_id)
        DO UPDATE SET
            vin = EXCLUDED.vin,
            year = EXCLUDED.year,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            trim = EXCLUDED.trim,
            body = EXCLUDED.body,
            color = EXCLUDED.color,
            auction_datetime_utc = EXCLUDED.auction_datetime_utc,
            yard = EXCLUDED.yard,
            state = EXCLUDED.state,
            run_number = EXCLUDED.run_number,
            title_status = EXCLUDED.title_status,
            title_norm = EXCLUDED.title_norm,
            odometer = EXCLUDED.odometer,
            odometer_miles = EXCLUDED.odometer_miles,
            odometer_km = EXCLUDED.odometer_km,
            damage_primary = EXCLUDED.damage_primary,
            damage_primary_norm = EXCLUDED.damage_primary_norm,
            damage_secondary = EXCLUDED.damage_secondary,
            damage_secondary_norm = EXCLUDED.damage_secondary_norm,
            keys = EXCLUDED.keys,
            engine = EXCLUDED.engine,
            transmission = EXCLUDED.transmission,
            drive = EXCLUDED.drive,
            fuel = EXCLUDED.fuel,
            est_repair_cost = EXCLUDED.est_repair_cost,
            retail_value = EXCLUDED.retail_value,
            images = EXCLUDED.images,
            status = EXCLUDED.status,
            updated_at = NOW();
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "lot_id": lot.get("lot_id"),
            "broker_id": lot.get("broker_id"),
            "vin": lot.get("vin"),
            "year": lot.get("year"),
            "make": lot.get("make"),
            "model": lot.get("model"),
            "trim": lot.get("trim"),
            "body": lot.get("body"),
            "color": lot.get("color"),
            "auction_datetime_utc": lot.get("auction_datetime_utc"),
            "yard": lot.get("yard"),
            "state": lot.get("state"),
            "run_number": lot.get("run_number"),
            "title_status": raw_title,
            "title_norm": title_norm,
            "odometer": lot.get("odometer"),
            "odometer_miles": odo_mi,
            "odometer_km": odo_km,
            "damage_primary": raw_dmg1,
            "damage_primary_norm": dmg1_norm,
            "damage_secondary": raw_dmg2,
            "damage_secondary_norm": dmg2_norm,
            "keys": lot.get("keys"),
            "engine": lot.get("engine"),
            "transmission": lot.get("transmission"),
            "drive": lot.get("drive"),
            "fuel": lot.get("fuel"),
            "est_repair_cost": lot.get("est_repair_cost"),
            "retail_value": lot.get("retail_value"),
            "images": lot.get("images"),
            "status": lot.get("status")
        })
    print(f"✅ Inserted/Updated lot {lot.get('lot_id')} with normalization.")
