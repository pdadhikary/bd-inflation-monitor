import json
import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from bd_inflation_monitor.config import settings

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Bangladesh Inflation Monitor",
    page_icon=":bar_chart:",
    layout="wide",
)

_, socials_col = st.columns([6, 1])

with socials_col:
    st.markdown("""
        [![GitHub](https://img.shields.io/badge/GitHub-View%20Repo-black?logo=github)](https://github.com/pdadhikary/bd-inflation-monitor)
        [![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/pdeepta-adhikary/)
        """)

st.title("Bangladesh Inflation Monitor")

st.caption("Adhikary, Prachurya Deepta")

BD_DIVISIONS = [
    {"name": "Barishal", "lat": 22.701002, "long": 90.353451},
    {"name": "Chattogram", "lat": 22.638722, "long": 92.025556},
    {"name": "Dhaka", "lat": 23.810332, "long": 90.412518},
    {"name": "Khulna", "lat": 23.07150, "long": 89.35186},
    {"name": "Rajshahi", "lat": 24.64425, "long": 89.01406},
    {"name": "Rangpur", "lat": 25.743892, "long": 89.275227},
    {"name": "Sylhet", "lat": 24.894929, "long": 91.868706},
    {"name": "Mymensingh", "lat": 24.747149, "long": 90.420273},
]


def _get(url: str, params: dict | None = None) -> requests.Response:
    """Centralised GET with timeout."""
    return requests.get(url, params=params, timeout=10)


def _fetch_df(
    endpoint: str, latest: str | None, date_cols: list[str] | None = None
) -> pd.DataFrame | None:
    """
    Fetch a JSON endpoint and return a DataFrame.
    Passes ?record_date=<latest> when latest is provided.
    Returns None and shows a user-friendly warning on any failure.
    """
    try:
        params = {"record_date": latest} if latest else {}
        response = _get(f"{settings.api_url}{endpoint}", params=params)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        for col in date_cols or []:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
        return df
    except requests.exceptions.ConnectionError:
        logger.error("Connection error fetching %s", endpoint)
        st.warning("Could not connect to the API. Please try again later.")
    except requests.exceptions.Timeout:
        logger.error("Timeout fetching %s", endpoint)
        st.warning("The API request timed out. Please try again later.")
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP %s error fetching %s", e.response.status_code, endpoint)
        st.warning("The API returned an unexpected error. Please try again later.")
    except Exception:
        logger.exception("Unexpected error fetching %s", endpoint)
        st.warning("An unexpected error occurred. Please try again later.")
    return None


@st.cache_data(ttl=3600)
def get_latest_date() -> str | None:
    """Checks for a new month every hour — cheap version query."""
    try:
        response = _get(f"{settings.api_url}/api/v1/latest_date")
        response.raise_for_status()
        return response.text.strip('"')
    except Exception:
        logger.exception("Failed to fetch latest date")
        st.warning("Could not connect to API. Please try again later.")
        return None


@st.cache_data
def get_cpi_data(latest: str | None) -> pd.DataFrame | None:
    """Cached forever — busts automatically when latest changes."""
    return _fetch_df("/api/v1/cpi", latest, date_cols=["record_date"])


@st.cache_data
def get_wri_data(latest: str | None) -> pd.DataFrame | None:
    return _fetch_df("/api/v1/wri", latest, date_cols=["record_date"])


@st.cache_data
def get_cpi_moving_avg_data(latest: str | None) -> pd.DataFrame | None:
    df = _fetch_df("/api/v1/cpi_moving_avg", latest, date_cols=["record_date"])
    if df is None:
        return None
    df["end_date"] = df["record_date"]
    df["start_date"] = df["end_date"] - pd.DateOffset(months=11)
    df["window_label"] = (
        df["start_date"].dt.strftime("%b'%y")
        + "-"
        + df["end_date"].dt.strftime("%b'%y")
    )
    return df


@st.cache_data
def get_wri_moving_avg_data(latest: str | None) -> pd.DataFrame | None:
    df = _fetch_df("/api/v1/wri_moving_avg", latest, date_cols=["record_date"])
    if df is None:
        return None
    df["end_date"] = df["record_date"]
    df["start_date"] = df["end_date"] - pd.DateOffset(months=11)
    df["window_label"] = (
        df["start_date"].dt.strftime("%b'%y")
        + "-"
        + df["end_date"].dt.strftime("%b'%y")
    )
    return df


@st.cache_data
def get_real_wage_growth_data(latest: str | None) -> pd.DataFrame | None:
    return _fetch_df("/api/v1/real_wage_growth", latest, date_cols=["record_date"])


@st.cache_data
def get_wri_by_region_data(latest: str | None) -> pd.DataFrame | None:
    return _fetch_df("/api/v1/wri_by_region", latest, date_cols=["record_date"])


@st.cache_data
def get_bd_division_geojson_data() -> dict | None:
    try:
        with open("geojson/bd_divisions_fixed.geojson") as f:
            return json.load(f)
    except Exception:
        logger.exception("Failed to load GeoJSON")
        st.warning("Could not load map data.")
        return None


def apply_common_layout(fig, ymin=100, ymax=150):
    fig.update_layout(
        height=450,
        margin=dict(l=60, r=20, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(range=[ymin, ymax], automargin=False, domain=[0, 1])
    return fig


@st.cache_data
def get_current_inflation_indicator(
    latest: str | None, current_month: str, resolution: str
):
    df_cpi = get_cpi_data(latest)
    df_mavg = get_cpi_moving_avg_data(latest)

    if df_cpi is None or df_mavg is None:
        return go.Figure()

    df_cpi = df_cpi[
        (df_cpi["region"] == "national") & (df_cpi["index"] == "general index")
    ]
    df_mavg = df_mavg[
        (df_mavg["region"] == "national") & (df_mavg["index"] == "general index")
    ]

    current_row = df_cpi[df_cpi["record_date"] == current_month]
    if current_row.empty:
        st.warning("No CPI data available for the current month.")
        return go.Figure()

    current_cpi = current_row["cpi"].iloc[0]

    if resolution == "YoY":
        current_inflation = current_row["yoy_inflation"].iloc[0]
    elif resolution == "MoM":
        current_inflation = current_row["mom_inflation"].iloc[0]
    else:
        mavg_row = df_mavg[df_mavg["record_date"] == current_month]
        if mavg_row.empty:
            return go.Figure()
        current_inflation = mavg_row["yoy_12m_moving_avg"].iloc[0]

    reference = current_cpi / (1 + current_inflation / 100)

    fig = go.Figure()
    fig.add_trace(
        go.Indicator(
            title={"text": "National Price Inflation<br>CPI"},
            mode="number+delta",
            value=current_cpi,
            delta={
                "reference": reference,
                "relative": True,
                "valueformat": ".2%",
                "font": {"size": 48},
                "increasing": {"color": "#9ccfd8"},
                "decreasing": {"color": "#eb6f92"},
            },
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_cpi["record_date"],
            y=df_cpi["cpi"],
            hovertemplate="%{x|%b %Y}<br>CPI: %{y:.2f}<extra></extra>",
            fill="tonexty",
            fillcolor="rgba(196, 167, 231, 0.3)",
            line_color="rgba(196, 167, 231, 1.0)",
            mode="lines",
        )
    )
    fig.update_xaxes(visible=False, showgrid=False, zeroline=False)
    fig.update_yaxes(visible=False, showgrid=False, zeroline=False, range=[100, 170])
    return fig


@st.cache_data
def get_current_wri_indicator(latest: str | None, current_month: str, resolution: str):
    df_wri = get_wri_data(latest)
    df_mavg = get_wri_moving_avg_data(latest)

    if df_wri is None or df_mavg is None:
        return go.Figure()

    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]
    df_mavg = df_mavg[
        (df_mavg["region"] == "national") & (df_mavg["sector"] == "general")
    ]

    current_row = df_wri[df_wri["record_date"] == current_month]
    if current_row.empty:
        st.warning("No WRI data available for the current month.")
        return go.Figure()

    current_wri = current_row["wri"].iloc[0]

    if resolution == "MoM":
        current_wage_growth = current_row["mom_wri_growth"].iloc[0]
    elif resolution == "YoY":
        current_wage_growth = current_row["yoy_wri_growth"].iloc[0]
    else:
        mavg_row = df_mavg[df_mavg["record_date"] == current_month]
        if mavg_row.empty:
            return go.Figure()
        current_wage_growth = mavg_row["yoy_12m_moving_avg"].iloc[0]

    reference = current_wri / (1 + current_wage_growth / 100)

    fig = go.Figure()
    fig.add_trace(
        go.Indicator(
            title="National Wage Growth<br>WRI",
            mode="number+delta",
            value=current_wri,
            delta={
                "reference": reference,
                "relative": True,
                "valueformat": ".2%",
                "font": {"size": 48},
                "increasing": {"color": "#f6c177"},
                "decreasing": {"color": "#eb6f92"},
            },
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_wri["record_date"],
            y=df_wri["wri"],
            fill="tozeroy",
            hovertemplate="%{x|%b %Y}<br>WRI: %{y:.2f}<extra></extra>",
            name="WRI",
            fillcolor="rgba(196, 167, 231, 0.3)",
            line_color="rgba(196, 167, 231, 1.0)",
            mode="lines",
        )
    )
    fig.update_xaxes(visible=False, showgrid=False, zeroline=False)
    fig.update_yaxes(visible=False, showgrid=False, zeroline=False, range=[100, 170])
    return fig


@st.cache_data
def get_wri_vs_cpi_plot(latest: str | None):
    df_wri = get_wri_data(latest)
    df_cpi = get_cpi_data(latest)

    if df_wri is None or df_cpi is None:
        return go.Figure()

    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]
    df_cpi = df_cpi[
        (df_cpi["region"] == "national") & (df_cpi["index"] == "general index")
    ]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_cpi["record_date"],
            y=df_cpi["cpi"],
            name="CPI",
            mode="lines+markers",
            line_color="rgba(156, 207, 216, 1.0)",
            hovertemplate="Date: %{x|%b %Y}<br>CPI: %{y:.2f}",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_wri["record_date"],
            y=df_wri["wri"],
            name="WRI",
            mode="lines+markers",
            line_color="rgba(246, 193, 119, 1.0)",
            hovertemplate="Date: %{x|%b %Y}<br>WRI: %{y:.2f}",
        )
    )
    apply_common_layout(fig)
    fig.update_yaxes(title_text="Index Value", range=[100, 150])
    return fig


@st.cache_data
def get_cpi_food_vs_nonfood_plot(latest: str | None):
    df_cpi = get_cpi_data(latest)
    if df_cpi is None:
        return go.Figure()

    df_cpi = df_cpi[
        (df_cpi["region"] == "national")
        & (df_cpi["index"].isin(["food index", "non-food index"]))
    ]
    df_cpi["index"] = df_cpi["index"].replace(
        {"food index": "Food", "non-food index": "Non-Food"}
    )

    plot = px.line(
        data_frame=df_cpi,
        x="record_date",
        y="cpi",
        color="index",
        markers=True,
        color_discrete_map={"Food": "#eb6f92", "Non-Food": "#3e8fb0"},
    )
    plot.update_layout(legend_title_text=None)
    plot.update_yaxes(title_text="CPI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate="Date: %{x|%b %Y}<br>CPI: %{y:.2f}")
    apply_common_layout(plot)
    return plot


@st.cache_data
def get_cpi_rural_vs_urban_plot(latest: str | None):
    df_cpi = get_cpi_data(latest)
    if df_cpi is None:
        return go.Figure()

    df_cpi = df_cpi[
        (df_cpi["region"].isin(["rural", "urban"]))
        & (df_cpi["index"] == "general index")
    ]
    df_cpi["region"] = df_cpi["region"].replace({"urban": "Urban", "rural": "Rural"})

    plot = px.line(
        data_frame=df_cpi,
        x="record_date",
        y="cpi",
        color="region",
        markers=True,
        color_discrete_map={"Urban": "#f4ede8", "Rural": "#c4a7e7"},
    )
    plot.update_layout(legend_title_text=None)
    plot.update_yaxes(title_text="CPI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate="Date: %{x|%b %Y}<br>CPI: %{y:.2f}")
    apply_common_layout(plot)
    return plot


@st.cache_data
def get_wri_sectors_plot(latest: str | None):
    df_wri = get_wri_data(latest)
    if df_wri is None:
        return go.Figure()

    df_wri = df_wri[
        (df_wri["region"] == "national")
        & (df_wri["sector"].isin(["agriculture", "industry", "service"]))
    ]
    df_wri["sector"] = df_wri["sector"].replace(
        {
            "agriculture": "Agriculture",
            "industry": "Industry",
            "service": "Service",
        }
    )

    plot = px.line(
        data_frame=df_wri,
        x="record_date",
        y="wri",
        color="sector",
        markers=True,
        color_discrete_map={
            "Agriculture": "#31748f",
            "Industry": "#9ccfd8",
            "Service": "#c4a7e7",
        },
    )
    plot.update_layout(legend_title_text=None)
    plot.update_yaxes(title_text="WRI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate="Date: %{x|%b %Y}<br>WRI: %{y:.2f}")
    apply_common_layout(plot)
    return plot


@st.cache_data
def get_wri_growth_vs_inflation_plot(latest: str | None, resolution: str):
    df_wri = get_wri_data(latest)
    df_cpi = get_cpi_data(latest)
    df_wri_mavg = get_wri_moving_avg_data(latest)
    df_cpi_mavg = get_cpi_moving_avg_data(latest)

    if any(df is None for df in [df_wri, df_cpi, df_wri_mavg, df_cpi_mavg]):
        return go.Figure()

    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]
    df_cpi = df_cpi[
        (df_cpi["region"] == "national") & (df_cpi["index"] == "general index")
    ]
    df_wri_mavg = df_wri_mavg[
        (df_wri_mavg["region"] == "national") & (df_wri_mavg["sector"] == "general")
    ]
    df_cpi_mavg = df_cpi_mavg[
        (df_cpi_mavg["region"] == "national")
        & (df_cpi_mavg["index"] == "general index")
    ]

    fig = go.Figure()
    custom_data1 = custom_data2 = None

    if resolution == "YoY":
        df_wri = df_wri.dropna(subset=["yoy_wri_growth"])
        df_cpi = df_cpi.dropna(subset=["yoy_inflation"])
        title, ymin, ymax = "Year-on-Year", 6, 13
        x1, y1, name1 = df_cpi["record_date"], df_cpi["yoy_inflation"], "Inflation"
        x2, y2, name2 = df_wri["record_date"], df_wri["yoy_wri_growth"], "Wage Growth"
        ht1 = ht2 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

    elif resolution == "MoM":
        df_wri = df_wri.dropna(subset=["mom_wri_growth"])
        df_cpi = df_cpi.dropna(subset=["mom_inflation"])
        title, ymin, ymax = "Month-on-Month", -2, 5
        x1, y1, name1 = df_cpi["record_date"], df_cpi["mom_inflation"], "Inflation"
        x2, y2, name2 = df_wri["record_date"], df_wri["mom_wri_growth"], "Wage Growth"
        ht1 = ht2 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

    else:
        df_wri_mavg = df_wri_mavg.dropna(subset=["yoy_12m_moving_avg"])
        df_cpi_mavg = df_cpi_mavg.dropna(subset=["yoy_12m_moving_avg"])
        title, ymin, ymax = "12 Month Moving Average", 6, 13
        x1, y1, name1 = (
            df_cpi_mavg["record_date"],
            df_cpi_mavg["yoy_12m_moving_avg"],
            "Avg Inflation",
        )
        x2, y2, name2 = (
            df_wri_mavg["record_date"],
            df_wri_mavg["yoy_12m_moving_avg"],
            "Avg Wage Growth",
        )
        custom_data1 = df_cpi_mavg[["start_date", "end_date"]]
        custom_data2 = df_wri_mavg[["start_date", "end_date"]]
        ht1 = ht2 = (
            "Start Date: %{customdata[0]|%b %Y}<br>End Date: %{customdata[1]|%b %Y}<br>Change: %{y:.2f}%"
        )
        fig.update_xaxes(
            tickmode="array",
            tickvals=df_cpi_mavg["record_date"],
            ticktext=df_cpi_mavg["window_label"],
            tickangle=-45,
        )

    fig.add_trace(
        go.Scatter(
            x=x1,
            y=y1,
            name=name1,
            mode="lines+markers",
            customdata=custom_data1,
            hovertemplate=ht1,
            line_color="rgba(156, 207, 216, 1.0)",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x2,
            y=y2,
            name=name2,
            mode="lines+markers",
            customdata=custom_data2,
            hovertemplate=ht2,
            line_color="rgba(246, 193, 119, 1.0)",
        )
    )

    fig.update_layout(title=title)
    fig.update_yaxes(title_text="% Change")
    apply_common_layout(fig, ymin=ymin, ymax=ymax)
    return fig


@st.cache_data
def get_real_wage_growth_plot(latest: str | None):
    df = get_real_wage_growth_data(latest)
    if df is None:
        return go.Figure()

    df = df.dropna(subset=["real_wage_growth"])
    plot = px.area(
        title="Real Wage Growth", data_frame=df, x="record_date", y="real_wage_growth"
    )
    plot.update_yaxes(title_text="% Change")
    plot.update_xaxes(title_text=None)
    plot.update_traces(
        fillcolor="rgba(235, 111, 146, 0.2)",
        line_color="rgba(235, 111, 146, 1.0)",
        hovertemplate="Date: %{x|%b %Y}<br>Real Wage Growth: %{y:.2f}%",
    )
    apply_common_layout(plot, ymin=-5, ymax=5)
    return plot


@st.cache_data
def get_wri_by_region_choropleth(latest: str | None, sector: str):
    df_wri = get_wri_by_region_data(latest)
    bd_div_json = get_bd_division_geojson_data()

    if df_wri is None or bd_div_json is None:
        return go.Figure()

    df_wri["region"] = df_wri["region"].str.title()
    df_wri = df_wri[
        (df_wri["region"] != "National") & (df_wri["sector"] == sector.lower())
    ]

    fig = px.choropleth(
        data_frame=df_wri,
        geojson=bd_div_json,
        locations="region",
        color="wri_growth",
        featureidkey="properties.ADM1_EN",
        color_continuous_scale=[[0.0, "#9ccfd8"], [1.0, "#31748f"]],
    )

    division_names = [d["name"] for d in BD_DIVISIONS]
    division_lats = [d["lat"] for d in BD_DIVISIONS]
    division_longs = [d["long"] for d in BD_DIVISIONS]

    fig.add_scattergeo(
        lon=division_longs,
        lat=division_lats,
        text=division_names,
        mode="text",
        textfont=dict(size=13, color="#e0def4", family="Inter", weight="bold"),
        hoverinfo="skip",
    )
    fig.update_geos(visible=False, fitbounds="locations", bgcolor="rgba(0,0,0,0)")
    fig.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        autosize=True,
        dragmode=False,
        coloraxis_colorbar=dict(title="Wage Growth (%)"),
    )
    fig.data[0].update(
        marker_line_color="#232136",
        marker_line_width=2,
        hovertemplate="<b>%{location}</b><br>Wage Growth: %{z:.2f}%<extra></extra>",
    )
    return fig


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    # Checked every hour — cheap version query drives all cache busting below
    latest = get_latest_date()

    if latest is None:
        st.error(
            "Could not determine the latest data date. Some charts may be unavailable."
        )
        return

    latest_dt = pd.to_datetime(latest, format="%Y-%m-%d")
    current_month = latest_dt.strftime("%Y-%m-%d")

    st.write(f"Using Latest Report from **{latest_dt.strftime('%B %Y')}**")

    resolution = st.pills(
        label="Resolution",
        options=["MoM", "YoY", "12 Month Moving Average"],
        selection_mode="single",
        default="YoY",
    )
    if resolution is None:
        resolution = "YoY"

    col11, col12 = st.columns([3, 4])

    with col11:
        subcol1, subcol2 = st.columns([1, 1])
        left_cell = subcol1.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        left_cell.plotly_chart(
            get_current_inflation_indicator(latest, current_month, resolution)
        )
        right_cell = subcol2.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        right_cell.plotly_chart(
            get_current_wri_indicator(latest, current_month, resolution)
        )

    with col12:
        top_right_cell = col12.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        top_right_cell.plotly_chart(
            get_wri_growth_vs_inflation_plot(latest, resolution)
        )

    plot_type = st.pills(
        label="Plot Type",
        options=[
            "CPI vs. WRI",
            "CPI: Food vs Non-Food",
            "CPI: Rural vs Urban",
            "WRI: Sectors",
        ],
        selection_mode="single",
        default="CPI vs. WRI",
    )
    if plot_type is None:
        plot_type = "CPI vs. WRI"

    col21, col22 = st.columns([3, 4])

    with col21:
        bottom_left_cell = col21.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        if plot_type == "CPI vs. WRI":
            bottom_left_cell.plotly_chart(get_wri_vs_cpi_plot(latest))
        elif plot_type == "CPI: Food vs Non-Food":
            bottom_left_cell.plotly_chart(get_cpi_food_vs_nonfood_plot(latest))
        elif plot_type == "CPI: Rural vs Urban":
            bottom_left_cell.plotly_chart(get_cpi_rural_vs_urban_plot(latest))
        elif plot_type == "WRI: Sectors":
            bottom_left_cell.plotly_chart(get_wri_sectors_plot(latest))

    with col22:
        bottom_right_cell = col22.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        bottom_right_cell.plotly_chart(get_real_wage_growth_plot(latest))

    col31, col32, col33 = st.columns([1, 3, 1])

    with col32:
        sector_selection = st.pills(
            label="WRI Sector",
            options=["General", "Agriculture", "Industry", "Service"],
            selection_mode="single",
            default="General",
        )
        if sector_selection is None:
            sector_selection = "General"

        choroplet_cell = col32.container(
            border=True, height="stretch", vertical_alignment="center"
        )
        choroplet_cell.plotly_chart(
            get_wri_by_region_choropleth(latest, sector_selection)
        )


main()
