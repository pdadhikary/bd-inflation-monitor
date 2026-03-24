from fastapi import APIRouter, Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from bd_inflation_monitor.db import get_db

app = FastAPI()

router_v1 = APIRouter(prefix="/api/v1", tags=["v1"])


@app.get("/")
async def root():
    return {"message": "Welcome to Inflation Teacker API!"}


@router_v1.get("/cpi")
async def get_cpi(db: Session = Depends(get_db)):
    query_string = """
    SELECT
        record_date,
        cat.name AS region,
        cl.name AS index,
        cpi,
        100.0 * (cpi - LAG(cpi, 1) OVER w) / LAG(cpi, 1) OVER w AS mom_inflation,
        100.0 * (cpi - LAG(cpi, 12) OVER w) / LAG(cpi, 12) OVER w AS yoy_inflation
    FROM cpi_data
    JOIN cpi_index_lookup cl
        ON cpi_data.index_id = cl.id
    JOIN cpi_region_lookup cat
        ON cpi_data.region_id = cat.id
    WINDOW w AS (
        PARTITION BY index_id, region_id
        ORDER BY record_date
    );
    """
    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


@router_v1.get("/wri")
async def get_wri(db: Session = Depends(get_db)):
    query_string = """
    SELECT
        record_date,
        r.name AS region,
        s.name AS sector,
        wri,
        100.0 * (wri - LAG(wri, 1) OVER w) / LAG(wri, 1) OVER w AS mom_wri_growth,
        100.0 * (wri - LAG(wri, 12) OVER w) / LAG(wri, 12) OVER w AS yoy_wri_growth
    FROM wri_data
    JOIN wri_sector_lookup s
        ON wri_data.sector_id = s.id
    JOIN wri_region_lookup r
        ON wri_data.region_id = r.id
    WINDOW w AS (
        PARTITION BY region_id, sector_id
        ORDER BY record_date
    );
    """
    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


@router_v1.get("/wri_by_region")
async def get_wri_by_region(db: Session = Depends(get_db)):
    query_string = """
    WITH wri_calc AS (
        SELECT
            record_date,
            r.name AS region,
            s.name AS sector,
            wri,
            100.0 * (wri - LAG(wri, 12) OVER w) / LAG(wri, 12) OVER w AS wri_growth,
            ROW_NUMBER() OVER (
                PARTITION BY wri_data.region_id, wri_data.sector_id
                ORDER BY record_date DESC
            ) AS rn
        FROM wri_data
        JOIN wri_sector_lookup s
            ON wri_data.sector_id = s.id
        JOIN wri_region_lookup r
            ON wri_data.region_id = r.id
        WINDOW w AS (
            PARTITION BY wri_data.region_id, wri_data.sector_id
            ORDER BY record_date
        )
    )

    SELECT
        record_date,
        region,
        sector,
        wri,
        wri_growth
    FROM wri_calc
    WHERE rn = 1
    AND wri_growth IS NOT NULL;
    """

    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


@router_v1.get("/wri_moving_avg")
async def get_wri_moving_avg(db: Session = Depends(get_db)):
    query_string = """
    WITH yoy AS (
        SELECT
            d.record_date,
            d.region_id,
            d.sector_id,
            r.name AS region,
            s.name AS sector,
            ROUND(
                (d.wri - LAG(d.wri, 12) OVER w) / LAG(d.wri, 12) OVER w * 100,
                2
            ) AS yoy_growth
        FROM wri_data d
        JOIN wri_region_lookup r ON d.region_id = r.id
        JOIN wri_sector_lookup s ON d.sector_id = s.id
        WINDOW w AS (
            PARTITION BY d.region_id, d.sector_id
            ORDER BY d.record_date
        )
    )
    SELECT
        record_date,
        region,
        sector,
        ROUND(
            AVG(yoy_growth) OVER (
                PARTITION BY region_id, sector_id
                ORDER BY record_date
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS yoy_12m_moving_avg
    FROM yoy
    ORDER BY record_date, region, sector;
    """

    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


@router_v1.get("/cpi_moving_avg")
async def get_cpi_moving_avg(db: Session = Depends(get_db)):
    query_string = """
    WITH yoy AS (
        SELECT
            d.record_date,
            d.region_id,
            d.index_id,
            r.name AS region,
            i.name AS index,
            ROUND(
                (d.cpi - LAG(d.cpi, 12) OVER w) / LAG(d.cpi, 12) OVER w * 100,
                2
            ) AS yoy_inflation
        FROM cpi_data d
        JOIN cpi_region_lookup r ON d.region_id = r.id
        JOIN cpi_index_lookup i ON d.index_id = i.id
        WINDOW w AS (
            PARTITION BY d.region_id, d.index_id
            ORDER BY d.record_date
        )
    )
    SELECT
        record_date,
        region,
        index,
        ROUND(
            AVG(yoy_inflation) OVER (
                PARTITION BY region_id, index_id
                ORDER BY record_date
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS yoy_12m_moving_avg
    FROM yoy
    ORDER BY record_date, region, index;
    """
    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


@router_v1.get("/real_wage_growth")
def get_real_wage_growth(db: Session = Depends(get_db)):
    query_string = """
    WITH cpi_yoy AS (
        SELECT
            record_date,
            100.0 * (cpi - LAG(cpi, 12) OVER (ORDER BY record_date))
                / LAG(cpi, 12) OVER (ORDER BY record_date) AS cpi_yoy
        FROM cpi_data
        WHERE region_id = 1
        AND index_id = 1
    ),

    wri_yoy AS (
        SELECT
            record_date,
            100.0 * (wri - LAG(wri, 12) OVER (ORDER BY record_date))
                / LAG(wri, 12) OVER (ORDER BY record_date) AS wri_yoy
        FROM wri_data
        WHERE region_id = 1
        AND sector_id = 1
    )

    SELECT
        c.record_date,
        c.cpi_yoy,
        w.wri_yoy,
        (w.wri_yoy - c.cpi_yoy) AS real_wage_growth
    FROM cpi_yoy c
    JOIN wri_yoy w
        ON c.record_date = w.record_date
    ORDER BY c.record_date;
    """

    result = db.execute(text(query_string))
    rows = result.mappings().all()
    return rows


app.include_router(router_v1)

__all__ = ["app"]
