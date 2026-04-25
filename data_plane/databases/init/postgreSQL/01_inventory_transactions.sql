-- Initializes PostgreSQL schema and CDC prerequisites for inventory transactions.

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_user') THEN
        CREATE ROLE etl_user LOGIN PASSWORD 'etl_password';
    END IF;
END
$$;

ALTER ROLE etl_user WITH LOGIN;

GRANT ALL PRIVILEGES ON DATABASE supply_chain_db TO etl_user;
GRANT ALL ON SCHEMA public TO etl_user;

CREATE TABLE IF NOT EXISTS inventory_transactions (
    transaction_id      VARCHAR(50)  PRIMARY KEY,
    product_id          VARCHAR(50)  NOT NULL,
    warehouse_location  VARCHAR(100) NOT NULL,
    transaction_type    VARCHAR(20)  NOT NULL CHECK (transaction_type IN ('IN','OUT','ADJUSTMENT','RETURN','WRITE_OFF')),
    quantity_change     INTEGER      NOT NULL,
    timestamp           TIMESTAMPTZ  NOT NULL,
    reference_order_id  VARCHAR(50),
    created_by          VARCHAR(100) NOT NULL DEFAULT 'system',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON TABLE inventory_transactions TO etl_user;

CREATE INDEX IF NOT EXISTS idx_inventory_transactions_product_id
    ON inventory_transactions (product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_warehouse_location
    ON inventory_transactions (warehouse_location);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_timestamp
    ON inventory_transactions (timestamp);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_created_at
    ON inventory_transactions (created_at);
CREATE INDEX IF NOT EXISTS idx_inventory_transactions_transaction_type
    ON inventory_transactions (transaction_type);

CREATE OR REPLACE FUNCTION set_inventory_transactions_updated_at()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_inventory_transactions_updated_at ON inventory_transactions;
CREATE TRIGGER trg_inventory_transactions_updated_at
BEFORE UPDATE ON inventory_transactions
FOR EACH ROW
EXECUTE FUNCTION set_inventory_transactions_updated_at();

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supply_chain_pub') THEN
        CREATE PUBLICATION supply_chain_pub FOR TABLE inventory_transactions;
    END IF;
END
$$;

SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);
