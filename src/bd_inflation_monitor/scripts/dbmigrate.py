import argparse
import logging

import psycopg

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.logging import setup_logging

logger = logging.getLogger(__name__)


def initialize_arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dbmigrate", description="Migrate data from one system to another"
    )

    parser.add_argument(
        "--db_host", required=True, help="URL of database to migrate data from"
    )
    parser.add_argument("--db_name", required=True, help="Database name")
    parser.add_argument("--db_user", required=True, help="Database user name")
    parser.add_argument("--db_pass", required=True, help="Database password")
    parser.add_argument("--db_port", required=True, help="Database port")

    return parser


def migrate_cpi_data(
    db_host: str, db_name: str, db_user: str, db_pass: str, db_port: str
):
    source_db_info = (
        f"dbname={db_name} "
        f"user={db_user} "
        f"password={db_pass} "
        f"host={db_host} "
        f"port={db_port}"
    )

    select_query = """
    SELECT
        record_date, region_id, index_id, cpi, source
    FROM
        cpi_data;"""

    insert_query = """
    INSERT INTO cpi_data (
        record_date,
        region_id,
        index_id,
        cpi,
        source
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (record_date, region_id, index_id)
    DO UPDATE SET
        cpi = EXCLUDED.cpi,
        source = EXCLUDED.source;"""

    logger.info("Connecting to source database")
    with psycopg.connect(conninfo=source_db_info) as conn:
        with conn.cursor() as cur:
            logger.info("Grabbing rows from source database")
            cur.execute(select_query)
            results = cur.fetchall()
    logger.info("Successfully grabbed rows from source database")

    logger.info("Connecting to destination database")
    with psycopg.connect(
        conninfo=settings.database_info, prepare_threshold=None
    ) as conn:
        with conn.cursor() as cur:
            logger.info("Inserting rows into destination databaase")
            cur.executemany(insert_query, results)
        conn.commit()
    logger.info("Successfully inserted rows into destination databaase")


def migrate_wri_data(
    db_host: str, db_name: str, db_user: str, db_pass: str, db_port: str
):
    source_db_info = (
        f"dbname={db_name} "
        f"user={db_user} "
        f"password={db_pass} "
        f"host={db_host} "
        f"port={db_port}"
    )

    select_query = """
    SELECT
        record_date, region_id, sector_id, wri, source
    FROM
        wri_data;"""

    insert_query = """
    INSERT INTO wri_data (
        record_date,
        region_id,
        sector_id,
        wri,
        source
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (record_date, region_id, sector_id)
    DO UPDATE SET
        wri = EXCLUDED.wri,
        source = EXCLUDED.source;"""

    logger.info("Connecting to source database")
    with psycopg.connect(conninfo=source_db_info) as conn:
        with conn.cursor() as cur:
            logger.info("Grabbing rows from source database")
            cur.execute(select_query)
            results = cur.fetchall()
    logger.info("Successfully grabbed rows from source database")

    logger.info("Connecting to destination database")
    with psycopg.connect(
        conninfo=settings.database_info, prepare_threshold=None
    ) as conn:
        with conn.cursor() as cur:
            logger.info("Inserting rows into destination databaase")
            cur.executemany(insert_query, results)
        conn.commit()
    logger.info("Successfully inserted rows into destination databaase")


def main():
    setup_logging()
    parser = initialize_arguments()
    args = parser.parse_args()

    logger.info("Migrating cpi_data")
    migrate_cpi_data(
        args.db_host, args.db_name, args.db_user, args.db_pass, args.db_port
    )
    logger.info("Successfully migrated cpi_data")

    logger.info("Migrating wri_data")
    migrate_wri_data(
        args.db_host, args.db_name, args.db_user, args.db_pass, args.db_port
    )
    logger.info("Successfully migrated wri_data")


if __name__ == "__main__":
    main()
