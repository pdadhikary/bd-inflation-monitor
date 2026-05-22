import logging

import psycopg
from psycopg.sql import SQL

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.logging import setup_logging

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
    logger.debug("Connecting to database.")
    with psycopg.connect(conninfo=settings.database_info) as conn:
        logger.debug("Successfully connected to database.")
        with conn.cursor() as cur:
            logger.info("Dropping tables.")
            cur.execute(query_string)
            conn.commit()
            logger.info("Successfully dropped tables and committed.")
    logger.debug("Database connection closed.")


def main():
    setup_logging()
    deletedb()


if __name__ == "__main__":
    main()
