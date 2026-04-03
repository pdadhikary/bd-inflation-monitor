import logging

import psycopg
from psycopg.sql import SQL

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.logging import setup_logging

logger = logging.getLogger(__name__)

query_string = SQL("""
CREATE TABLE IF NOT EXISTS cpi_index_lookup (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS cpi_region_lookup (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name varchar(255)
);

CREATE TABLE IF NOT EXISTS wri_sector_lookup (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS wri_region_lookup (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS cpi_data (
    record_date DATE,
    region_id INTEGER REFERENCES cpi_region_lookup(id),
    index_id INTEGER REFERENCES cpi_index_lookup(id),
    cpi DECIMAL,
    source VARCHAR,
    PRIMARY KEY (record_date, region_id, index_id)
);

CREATE TABLE IF NOT EXISTS wri_data (
    record_date DATE,
    region_id INTEGER REFERENCES wri_region_lookup(id),
    sector_id INTEGER REFERENCES wri_sector_lookup(id),
    wri DECIMAL,
    source VARCHAR,
    PRIMARY KEY (record_date, region_id, sector_id)
);

INSERT INTO cpi_index_lookup (name) VALUES
	('general index'),
	('food index'),
	('non-food index');

INSERT INTO cpi_region_lookup (name) VALUES
	('national'),
	('urban'),
	('rural');

INSERT INTO wri_region_lookup (name) VALUES
    ('national'),
    ('dhaka'),
    ('chattogram'),
    ('rajshahi'),
    ('rangpur'),
    ('khulna'),
    ('barishal'),
    ('sylhet'),
    ('mymensingh');

INSERT INTO wri_sector_lookup (name) VALUES
    ('general'),
    ('agriculture'),
    ('industry'),
    ('service');
""")


def initdb():
    logging.debug("Connecting to databse.")
    with psycopg.connect(conninfo=settings.database_info) as conn:
        logging.debug("Successfully logged into database.")
        with conn.cursor() as cur:
            logging.info("Initializing database.")
            cur.execute(query_string)
            logging.info("Successfully initialized database.")
            conn.commit()
            logging.info("Successfully saved changes.")
    logging.debug("Database connection closed.")


def main():
    setup_logging()
    initdb()


if __name__ == "__main__":
    main()
