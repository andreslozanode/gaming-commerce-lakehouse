-- OLTP schema behind the CDC path: purchases (physical/digital), memberships and console SKUs.
-- Also creates the least-privilege Debezium role and the heartbeat table.

CREATE TABLE IF NOT EXISTS customers (
    player_id        TEXT PRIMARY KEY,
    country          TEXT,
    signup_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    primary_platform TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consoles (
    console_sku    TEXT PRIMARY KEY,       -- PS5-SLIM-DISC, XSX-1TB, ...
    console_family TEXT NOT NULL,          -- PlayStation | Xbox
    generation     SMALLINT NOT NULL,
    launch_year    SMALLINT NOT NULL,
    msrp_usd       NUMERIC(10,2),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT PRIMARY KEY,
    player_id    TEXT REFERENCES customers(player_id),
    channel_code TEXT NOT NULL,            -- physical_retail | digital_store | membership_fee | console_hardware
    platform     TEXT,
    order_status TEXT NOT NULL DEFAULT 'PLACED',
    order_total  NUMERIC(12,2) NOT NULL,
    currency     TEXT NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id      TEXT REFERENCES orders(order_id),
    product_id    TEXT NOT NULL,
    product_type  TEXT NOT NULL,           -- GAME | CONSOLE | MEMBERSHIP | DLC
    title         TEXT,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2) NOT NULL,
    discount_pct  NUMERIC(4,3) DEFAULT 0,
    is_preowned   BOOLEAN DEFAULT FALSE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id TEXT PRIMARY KEY,
    player_id       TEXT REFERENCES customers(player_id),
    membership_tier TEXT NOT NULL,         -- PS_PLUS_ESSENTIAL | GAME_PASS_ULTIMATE | ...
    status          TEXT NOT NULL,         -- ACTIVE | CANCELLED | LAPSED
    started_at      TIMESTAMPTZ NOT NULL,
    renews_at       TIMESTAMPTZ,
    mrr             NUMERIC(8,2) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id INT PRIMARY KEY DEFAULT 1,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO debezium_heartbeat (id) VALUES (1) ON CONFLICT DO NOTHING;

-- updated_at maintenance -> Airbyte's incremental cursor depends on it.
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['customers','consoles','orders','order_items','subscriptions'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_touch ON %I', t, t);
    EXECUTE format('CREATE TRIGGER trg_%s_touch BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION touch_updated_at()', t, t);
  END LOOP;
END $$;

-- --------------------------------------------------------------------------------
-- Least-privilege CDC role: replication + SELECT on exactly five tables. Nothing else.
-- --------------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'change-me-in-secret-manager';
  END IF;
END $$;

GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON customers, consoles, orders, order_items, subscriptions, debezium_heartbeat TO debezium;
GRANT UPDATE, INSERT ON debezium_heartbeat TO debezium;

DROP PUBLICATION IF EXISTS gc_pub;
CREATE PUBLICATION gc_pub FOR TABLE customers, consoles, orders, order_items, subscriptions;

-- Seed a handful of console SKUs spanning the three eras in scope.
INSERT INTO consoles (console_sku, console_family, generation, launch_year, msrp_usd) VALUES
    ('PS1-ORIGINAL', 'PlayStation', 5, 1994, 299.00),
    ('PS2-SLIM',     'PlayStation', 6, 2000, 299.00),
    ('PS3-SLIM',     'PlayStation', 7, 2006, 499.00),
    ('PS4-PRO',      'PlayStation', 8, 2013, 399.00),
    ('PS5-SLIM',     'PlayStation', 9, 2020, 499.00),
    ('XBOX-ORIGINAL','Xbox',        6, 2001, 299.00),
    ('X360-ELITE',   'Xbox',        7, 2005, 399.00),
    ('XONE-S',       'Xbox',        8, 2013, 499.00),
    ('XSX-1TB',      'Xbox',        9, 2020, 499.00)
ON CONFLICT DO NOTHING;
