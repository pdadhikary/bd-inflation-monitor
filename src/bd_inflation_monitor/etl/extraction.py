import os

import fastexcel
import numpy as np
import polars as pl

CPI_REGIONS = ["national", "rural", "urban"]
VALID_CPI_INDEXES = [
    "general index",
    "inflation",
    "food index",
    "non-food index",
]
WRI_REGIONS = [
    "National",
    "Dhaka",
    "Chattogram",
    "Rajshahi",
    "Rangpur",
    "Khulna",
    "Barishal",
    "Sylhet",
    "Mymensingh",
]
VALID_WRI_SECTORS = ["general", "1. agriculture", "2. industry", "3. service"]
WRI_SECTOR_MAPPINGS = dict(
    zip(VALID_WRI_SECTORS, ["general", "agriculture", "industry", "service"])
)


def extract_monthly_cpi_data(filename: str) -> pl.DataFrame:
    df = pl.read_excel(filename, read_options={"header_row": 2})
    df = df.rename({"CPI Classification": "index"})
    column_name_mapping = {
        col: df[1, i] for i, col in enumerate(df.columns[4:], start=4)
    }
    df = df.rename(column_name_mapping)
    num_months = len(df.columns) - 4

    df = df.with_columns(df["index"].str.to_lowercase().str.strip_chars())
    df_monthly = df.unpivot(
        index="index",
        on=df.columns[4:],
        variable_name="record_date",
        value_name="cpi",
    )

    df_monthly = df_monthly.drop_nulls()

    df_monthly = df_monthly.filter(pl.col("index").is_in(VALID_CPI_INDEXES))

    df_monthly = df_monthly.with_columns(
        pl.Series(
            "region",
            np.array([[x] * 6 for x in CPI_REGIONS] * num_months).flatten(),
        )
    )

    inflation_rows = df_monthly["index"].is_in(["inflation"])
    df_monthly = df_monthly.filter(~inflation_rows)

    df_monthly = df_monthly.with_columns(pl.col("record_date").str.replace("’", "'"))
    df_monthly = df_monthly.with_columns(
        pl.col("record_date").str.strptime(pl.Date, format="%b'%y")
    )
    df_monthly = df_monthly.with_columns(pl.col("cpi").cast(pl.Float32))
    df_monthly = df_monthly.select(
        pl.col("record_date"), pl.col("region"), pl.col("index"), pl.col("cpi")
    )
    df_monthly = df_monthly.with_columns(
        pl.lit(os.path.basename(filename)).alias("source")
    )
    return df_monthly


def extract_monthly_wri_data(filename: str) -> pl.DataFrame:
    workbook = fastexcel.read_excel(filename)
    region_workbook_sheets = {
        region: sheet
        for region in WRI_REGIONS
        for sheet in workbook.sheet_names
        if sheet.endswith(f"WRI_{region}")
    }

    output_dataframes = []

    for region, sheet in region_workbook_sheets.items():
        df = pl.read_excel(filename, read_options={"header_row": 1}, sheet_name=sheet)
        if df.columns[0] != "Sector":
            df = pl.read_excel(
                filename, read_options={"header_row": 2}, sheet_name=sheet
            )

        df = df.drop(df.columns[1:4])
        df = df.rename({"Sector": "sector"})
        df = df.with_columns(df["sector"].str.to_lowercase().str.strip_chars())
        df_monthly = df.unpivot(
            index="sector",
            on=df.columns[1:],
            variable_name="record_date",
            value_name="wri",
        )
        df_monthly = df_monthly.drop_nulls()
        df_monthly = df_monthly.filter(pl.col("sector").is_in(VALID_WRI_SECTORS))

        df_monthly = df_monthly.with_columns(
            pl.col("sector").replace(WRI_SECTOR_MAPPINGS)
        )
        df_monthly = df_monthly.with_columns(
            pl.col("record_date").str.replace("’", "'")
        )
        df_monthly = df_monthly.with_columns(
            pl.col("record_date").str.strptime(pl.Date, format="%b'%y")
        )
        df_monthly = df_monthly.with_columns(pl.col("wri").cast(pl.Float32))
        df_monthly = df_monthly.with_columns(pl.lit(region.lower()).alias("region"))
        df_monthly = df_monthly.select(
            pl.col("record_date"), pl.col("region"), pl.col("sector"), pl.col("wri")
        )
        output_dataframes.append(df_monthly)

    output = pl.concat(output_dataframes, how="vertical")
    output = output.with_columns(pl.lit(os.path.basename(filename)).alias("source"))
    return output
