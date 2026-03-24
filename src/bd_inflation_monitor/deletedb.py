import logging

import psycopg
from psycopg.sql import SQL

from bd_inflation_monitor.config import settings

from .logging import setup_logging

logger = logging.getLogger(__name__)

query_string = SQL("""
DROP TABLE IF EXISTS cpi_data;

DROP TABLE IF EXISTS wri_data;

DROP TABLE IF EXISTS cpi_index_lookup;

DROP TABLE IF EXISTS cpi_region_lookup;

DROP TABLE IF EXISTS wri_region_lookup;

DROP TABLE IF EXISTS wri_sector_lookup;
""")


def deletedb():
    logging.debug("Connecting to databse.")
    with psycopg.connect(conninfo=settings.database_info) as conn:
        logging.debug("Successfully logged into database.")
        with conn.cursor() as cur:
            logging.info("Dropping database.")
            cur.execute(query_string)
            logging.info("Successfully dropped database.")
            conn.commit()
            logging.info("Successfully saved changes.")
    logging.debug("Database connection closed.")


def main():
    setup_logging()
    deletedb()


if __name__ == "__main__":
    main()
