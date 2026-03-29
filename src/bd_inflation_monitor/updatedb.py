import logging
from pathlib import Path

import polars as pl
import psycopg
from psycopg.sql import SQL

from bd_inflation_monitor.config import settings
from bd_inflation_monitor.etl.extraction import (
    extract_monthly_cpi_data,
    extract_monthly_wri_data,
)

from .logging import setup_logging

logger = logging.getLogger(__name__)

staged_path = Path(settings.stage_dir)
processed_path = Path(settings.processed_dir)

cpi_insert_query = SQL("""
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
    source = EXCLUDED.source;
""")

wri_insert_query = SQL("""
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
    source = EXCLUDED.source;
""")


def extract_and_update_data():
    logger.debug("Connecting to database.")
    with psycopg.connect(conninfo=settings.database_info) as conn:
        logger.debug("Successfully connected to database.")
        with conn.cursor() as cur:
            logger.info("Fetching index and region mappings from database.")
            cur.execute("SELECT name, id from cpi_index_lookup;")
            indexes = cur.fetchall()

            cur.execute("SELECT name, id from cpi_region_lookup;")
            regions = cur.fetchall()

            cur.execute("SELECT name, id from wri_region_lookup;")
            wri_regions = cur.fetchall()

            cur.execute("SELECT name, id from wri_sector_lookup;")
            wri_sectors = cur.fetchall()
    logger.debug("Database connection closed.")
    logger.info("Successfully fetched index and region mappings from database.")

    logger.debug("Converting fetched mappings to dictionary.")
    index_mapping = dict(indexes)
    region_mapping = dict(regions)
    wri_region_mapping = dict(wri_regions)
    wri_sector_mappping = dict(wri_sectors)
    logger.debug("Successfully converted fetched mappings to dictionary.")

    for staged_file in staged_path.glob("*.xlsx"):
        logger.info(f"Found staged file for extraction: {staged_file}")
        logger.debug("Connecting to database.")
        with psycopg.connect(conninfo=settings.database_info) as conn:
            with conn.cursor() as cur:
                logger.debug("Successfully connected to database.")
                try:
                    if staged_file.name.startswith("Legacy"):
                        logger.info("Extracting Legacy CPI data from file.")
                        df_cpi = pl.read_excel(staged_file, sheet_name="CPI")
                        logger.info("Successfully extracted Legacy CPI data from file.")

                        logger.debug("Extracting dates.")
                        df_cpi = df_cpi.with_columns(
                            pl.col("record_date").str.replace("’", "'")
                        )
                        df_cpi = df_cpi.with_columns(
                            pl.col("record_date").str.strptime(pl.Date, format="%b'%y")
                        )

                        logger.debug("Applying category mapping.")
                        df_cpi = df_cpi.with_columns(
                            pl.col("region").replace(region_mapping).cast(pl.Int32)
                        )

                        logger.debug("Applying index mapping.")
                        df_cpi = df_cpi.with_columns(
                            pl.col("index").replace(index_mapping).cast(pl.Int32)
                        )

                        logger.info("Extracting Legacy WRI data from file.")
                        df_wri = pl.read_excel(staged_file, sheet_name="WRI")
                        logger.info("Successfully extracted WRI data from file.")

                        logger.debug("Extracting dates.")
                        df_wri = df_wri.with_columns(
                            pl.col("record_date").str.replace("’", "'")
                        )
                        df_wri = df_wri.with_columns(
                            pl.col("record_date").str.strptime(pl.Date, format="%b'%y")
                        )

                        logger.debug("Applying region mapping.")
                        df_wri = df_wri.with_columns(
                            pl.col("region").replace(wri_region_mapping).cast(pl.Int32)
                        )

                        logger.debug("Applying sector mapping.")
                        df_wri = df_wri.with_columns(
                            pl.col("sector").replace(wri_sector_mappping).cast(pl.Int32)
                        )

                    else:
                        logger.info("Extracting CPI data from file.")
                        df_cpi = extract_monthly_cpi_data(str(staged_file))
                        logger.info("Successfully extracted CPI data from file.")

                        logger.debug("Applying category mapping.")
                        df_cpi = df_cpi.with_columns(
                            pl.col("region").replace(region_mapping).cast(pl.Int32)
                        )

                        logger.debug("Applying index mapping.")
                        df_cpi = df_cpi.with_columns(
                            pl.col("index").replace(index_mapping).cast(pl.Int32)
                        )

                        logger.info("Extracting WRI data from file.")
                        df_wri = extract_monthly_wri_data(str(staged_file))
                        logger.info("Successfully extracted WRI data from file.")

                        logger.debug("Applying region mapping.")
                        df_wri = df_wri.with_columns(
                            pl.col("region").replace(wri_region_mapping).cast(pl.Int32)
                        )

                        logger.debug("Applying sector mapping.")
                        df_wri = df_wri.with_columns(
                            pl.col("sector").replace(wri_sector_mappping).cast(pl.Int32)
                        )

                    logger.info("Inserting CPI data into database.")
                    cur.executemany(cpi_insert_query, df_cpi.rows())
                    logger.info("Successfully inserted CPI data into database.")

                    logger.info("Inserting WRI data into database.")
                    cur.executemany(wri_insert_query, df_wri.rows())
                    logger.info("Successfully WRI inserted data into database.")

                except Exception:
                    logger.exception("Couldn't insert data into database.")
                    continue

        destination = processed_path / staged_file.name
        logging.debug(f"Moving file to {destination}.")
        staged_file.rename(destination)
        logging.debug("Successfully moved file to processed directory.")


def main():
    setup_logging()
    extract_and_update_data()


if __name__ == "__main__":
    main()
