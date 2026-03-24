import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from bd_inflation_monitor.config import settings

_, socials_col = st.columns([6, 1])

with socials_col:
    st.markdown("""
        [![GitHub](https://img.shields.io/badge/GitHub-View%20Repo-black?logo=github)](https://github.com/pdadhikary/bd-inflation-monitor)
        [![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/pdeepta-adhikary/)
        """)

st.set_page_config(
    page_title="Bangladsh Inflation Monitor",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("Bangladesh Inflation Monitor")

st.caption("Adhikary, Prachurya Deepta")

# st.write("""
# # Bangladesh Inflation Monitor
#
# **Author:** Prachurya Deepta Adhikary \\
# **Email:** deeptoahdikary@gmail.com \\
# **Data Source:** [Bangladesh Bureau of Statistics](https://bbs.gov.bd/pages/static-pages/6922de7a933eb65569e1ae8f) \\
# **Source Code:** [GitHub](https://github.com/pdadhikary) / [Codeberg](https://codeberg.org/dewpta)
# """)

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


def apply_common_layout(fig, ymin=100, ymax=150):
    fig.update_layout(
        height=450,
        margin=dict(l=60, r=20, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(range=[ymin, ymax], automargin=False, domain=[0, 1])
    return fig


@st.cache_data
def get_cpi_data():
    response = requests.request(method="GET", url=f"{settings.api_url}/api/v1/cpi")
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")
    return df


@st.cache_data
def get_wri_data():
    response = requests.request(method="GET", url=f"{settings.api_url}/api/v1/wri")
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")
    return df


@st.cache_data
def get_cpi_moving_avg_data():
    response = requests.request(
        method="GET", url=f"{settings.api_url}/api/v1/cpi_moving_avg"
    )
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")

    df["end_date"] = df["record_date"]
    df["start_date"] = df["end_date"] - pd.DateOffset(months=11)
    df["window_label"] = (
        df["start_date"].dt.strftime("%b’%y")
        + "-"
        + df["end_date"].dt.strftime("%b’%y")
    )

    return df


@st.cache_data
def get_wri_moving_avg_data():
    response = requests.request(
        method="GET", url=f"{settings.api_url}/api/v1/wri_moving_avg"
    )
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")

    df["end_date"] = df["record_date"]
    df["start_date"] = df["end_date"] - pd.DateOffset(months=11)
    df["window_label"] = (
        df["start_date"].dt.strftime("%b’%y")
        + "-"
        + df["end_date"].dt.strftime("%b’%y")
    )

    return df


@st.cache_data
def get_real_wage_growth_data():
    response = requests.request(
        method="GET", url=f"{settings.api_url}/api/v1/real_wage_growth"
    )
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")

    return df


@st.cache_data
def get_wri_by_region_data():
    response = requests.request(
        method="GET", url=f"{settings.api_url}/api/v1/wri_by_region"
    )
    df = pd.DataFrame(response.json())
    df["record_date"] = pd.to_datetime(df["record_date"], format="%Y-%m-%d")
    return df


@st.cache_data
def get_bd_division_geojson_data():
    with open("geojson/bd_divisions_fixed.geojson") as jsonFile:
        bd_div_json = json.load(jsonFile)

    return bd_div_json


@st.cache_resource
def get_current_inflation_indicator(current_month, resolution: str):
    df_cpi = get_cpi_data()
    df_cpi = df_cpi[
        (df_cpi["region"] == "national") & (df_cpi["index"] == "general index")
    ]

    df_mavg = get_cpi_moving_avg_data()
    df_mavg = df_mavg[
        (df_mavg["region"] == "national") & (df_mavg["index"] == "general index")
    ]

    current_row = df_cpi[(df_cpi["record_date"] == current_month)]
    current_cpi = current_row["cpi"].iloc[0]  # pyright: ignore

    if resolution == "YoY":
        current_inflation = current_row["yoy_inflation"].iloc[0]  # pyright: ignore
    elif resolution == "MoM":
        current_inflation = current_row["mom_inflation"].iloc[0]  # pyright: ignore
    else:
        current_row = df_mavg[(df_mavg["record_date"] == current_month)]
        current_inflation = current_row["yoy_12m_moving_avg"].iloc[0]  # pyright: ignore

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


@st.cache_resource
def get_current_wri_indicator(current_month, resolution: str):
    df_wri = get_wri_data()
    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]

    df_mavg = get_wri_moving_avg_data()
    df_mavg = df_mavg[
        (df_mavg["region"] == "national") & (df_mavg["sector"] == "general")
    ]

    current_row = df_wri[(df_wri["record_date"] == current_month)]
    current_wri = current_row["wri"].iloc[0]  # pyright: ignore

    if resolution == "MoM":
        current_wage_growth = current_row["mom_wri_growth"].iloc[0]  # pyright: ignore
    elif resolution == "YoY":
        current_wage_growth = current_row["yoy_wri_growth"].iloc[0]  # pyright: ignore
    else:
        current_row = df_mavg[(df_mavg["record_date"] == current_month)]
        current_wage_growth = current_row["yoy_12m_moving_avg"].iloc[  # pyright: ignore
            0
        ]

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


@st.cache_resource
def get_wri_vs_cpi_plot():
    df_wri = get_wri_data()
    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]

    df_cpi = get_cpi_data()
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


@st.cache_resource
def get_cpi_food_vs_nonfood__plot():
    df_cpi = get_cpi_data()
    df_cpi = df_cpi[
        (df_cpi["region"] == "national")
        & (df_cpi["index"].isin(["food index", "non-food index"]))
    ]

    df_cpi["index"] = df_cpi["index"].replace(  # pyright: ignore
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

    plot.update_layout(
        legend_title_text=None,
    )
    plot.update_yaxes(title_text="CPI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate=("Date: %{x|%b %Y}<br>" + "CPI: %{y:.2f}"))
    apply_common_layout(plot)

    return plot


@st.cache_resource
def get_cpi_rural_vs_urban_plot():
    df_cpi = get_cpi_data()
    df_cpi = df_cpi[
        (df_cpi["region"].isin(["rural", "urban"]))
        & (df_cpi["index"] == "general index")
    ]
    df_cpi["region"] = df_cpi["region"].replace(  # pyright: ignore
        {"urban": "Urban", "rural": "Rural"}
    )

    plot = px.line(
        data_frame=df_cpi,
        x="record_date",
        y="cpi",
        color="region",
        markers=True,
        color_discrete_map={"Urban": "#f4ede8", "Rural": "#c4a7e7"},
    )

    plot.update_layout(
        legend_title_text=None,
    )
    plot.update_yaxes(title_text="CPI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate=("Date: %{x|%b %Y}<br>" + "CPI: %{y:.2f}"))
    apply_common_layout(plot)
    return plot


def get_wri_sectors_plot():
    df_wri = get_wri_data()
    df_wri = df_wri[
        (df_wri["region"] == "national")
        & (df_wri["sector"].isin(["agriculture", "industry", "service"]))
    ]
    df_wri["sector"] = df_wri["sector"].replace(  # pyright: ignore
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

    plot.update_layout(
        legend_title_text=None,
    )
    plot.update_yaxes(title_text="WRI", range=[100, 150])
    plot.update_xaxes(title_text=None)
    plot.update_traces(hovertemplate=("Date: %{x|%b %Y}<br>" + "WRI: %{y:.2f}"))
    apply_common_layout(plot)

    return plot


@st.cache_resource
def get_wri_by_region_choropleth(sector: str):
    df_wri = get_wri_by_region_data()
    bd_div_json = get_bd_division_geojson_data()

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

    division_names = [division["name"] for division in BD_DIVISIONS]
    division_lats = [division["lat"] for division in BD_DIVISIONS]
    division_longs = [division["long"] for division in BD_DIVISIONS]

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


@st.cache_resource
def get_wri_growth_vs_inflation_plot(resolution: str):
    df_wri = get_wri_data()
    df_wri = df_wri[(df_wri["region"] == "national") & (df_wri["sector"] == "general")]

    df_cpi = get_cpi_data()
    df_cpi = df_cpi[
        (df_cpi["region"] == "national") & (df_cpi["index"] == "general index")
    ]

    df_wri_mavg = get_wri_moving_avg_data()
    df_wri_mavg = df_wri_mavg[
        (df_wri_mavg["region"] == "national") & (df_wri_mavg["sector"] == "general")
    ]

    df_cpi_mavg = get_cpi_moving_avg_data()
    df_cpi_mavg = df_cpi_mavg[
        (df_cpi_mavg["region"] == "national")
        & (df_cpi_mavg["index"] == "general index")
    ]

    fig = go.Figure()

    custom_data1 = None
    custom_data2 = None

    if resolution == "YoY":
        df_wri = df_wri.dropna(subset=["yoy_wri_growth"])  # pyright: ignore
        df_cpi = df_cpi.dropna(subset=["yoy_inflation"])  # pyright: ignore

        title = "Year-on-Year"
        ymin = 6
        ymax = 13

        x1_data = df_cpi["record_date"]
        y1_data = df_cpi["yoy_inflation"]
        name1 = "Inflation"
        hover_template1 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

        x2_data = df_wri["record_date"]
        y2_data = df_wri["yoy_wri_growth"]
        name2 = "Wage Growth"
        hover_template2 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

    elif resolution == "MoM":
        df_wri = df_wri.dropna(subset=["mom_wri_growth"])  # pyright: ignore
        df_cpi = df_cpi.dropna(subset=["mom_inflation"])  # pyright: ignore

        title = "Month-on-Month"
        ymin = -2
        ymax = 5

        x1_data = df_cpi["record_date"]
        y1_data = df_cpi["mom_inflation"]
        name1 = "Inflation"
        hover_template1 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

        x2_data = df_wri["record_date"]
        y2_data = df_wri["mom_wri_growth"]
        name2 = "Wage Growth"
        hover_template2 = "Date: %{x|%b %Y}<br>Change: %{y:.2f}%"

    else:
        df_wri_mavg = df_wri_mavg.dropna(
            subset=["yoy_12m_moving_avg"]
        )  # pyright: ignore
        df_cpi_mavg = df_cpi_mavg.dropna(
            subset=["yoy_12m_moving_avg"]
        )  # pyright: ignore

        title = "12 Month Moving Average"
        ymin = 6
        ymax = 13

        x1_data = df_cpi_mavg["record_date"]
        y1_data = df_cpi_mavg["yoy_12m_moving_avg"]
        name1 = "Avg Inflation"
        custom_data1 = df_cpi_mavg[["start_date", "end_date"]]
        hover_template1 = (
            "Start Date: %{customdata[0]|%b %Y}<br>"
            + "End Date: %{customdata[1]|%b %Y}<br>"
            + "Change: %{y:.2f}%"
        )

        x2_data = df_wri_mavg["record_date"]
        y2_data = df_wri_mavg["yoy_12m_moving_avg"]
        name2 = "Avg Wage Growth"
        custom_data2 = df_wri_mavg[["start_date", "end_date"]]
        hover_template2 = (
            "Start Date: %{customdata[0]|%b %Y}<br>"
            + "End Date: %{customdata[1]|%b %Y}<br>"
            + "Change: %{y:.2f}%"
        )

        fig.update_xaxes(
            tickmode="array",
            tickvals=df_cpi_mavg["record_date"],
            ticktext=df_cpi_mavg["window_label"],
            tickangle=-45,
        )

    fig.add_trace(
        go.Scatter(
            x=x1_data,
            y=y1_data,
            name=name1,
            mode="lines+markers",
            customdata=custom_data1,
            hovertemplate=hover_template1,
            line_color="rgba(156, 207, 216, 1.0)",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x2_data,
            y=y2_data,
            name=name2,
            mode="lines+markers",
            customdata=custom_data2,
            hovertemplate=hover_template2,
            line_color="rgba(246, 193, 119, 1.0)",
        )
    )

    fig.update_layout(title=title)

    fig.update_yaxes(title_text="% Change")

    apply_common_layout(fig, ymin=ymin, ymax=ymax)

    return fig


@st.cache_resource
def get_real_wage_growth_plot():
    df_wage_growth = get_real_wage_growth_data()
    df_wage_growth = df_wage_growth.dropna(subset=["real_wage_growth"])
    plot = px.area(
        title="Real Wage Growth",
        data_frame=df_wage_growth,
        x="record_date",
        y="real_wage_growth",
    )
    plot.update_yaxes(title_text="% Change")
    plot.update_xaxes(title_text=None)
    plot.update_traces(
        fillcolor="rgba(235, 111, 146, 0.2)",
        line_color="rgba(235, 111, 146, 1.0)",
        hovertemplate=("Date: %{x|%b %Y}<br>" + "Real Wage Growth: %{y:.2f}%"),
    )
    apply_common_layout(plot, ymin=-5, ymax=5)
    return plot


today = pd.Timestamp.today()
end_date = today.replace(day=1).normalize() - pd.DateOffset(months=1)
start_date = end_date - pd.DateOffset(years=1)

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
        get_current_inflation_indicator(end_date.strftime("%Y-%m-%d"), resolution)
    )
    right_cell = subcol2.container(
        border=True, height="stretch", vertical_alignment="center"
    )
    right_cell.plotly_chart(
        get_current_wri_indicator(end_date.strftime("%Y-%m-%d"), resolution)
    )


with col12:
    top_right_cell = col12.container(
        border=True, height="stretch", vertical_alignment="center"
    )
    top_right_cell.plotly_chart(get_wri_growth_vs_inflation_plot(resolution))


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
        bottom_left_cell.plotly_chart(get_wri_vs_cpi_plot())
    elif plot_type == "CPI: Food vs Non-Food":
        bottom_left_cell.plotly_chart(get_cpi_food_vs_nonfood__plot())
    elif plot_type == "CPI: Rural vs Urban":
        bottom_left_cell.plotly_chart(get_cpi_rural_vs_urban_plot())
    elif plot_type == "WRI: Sectors":
        bottom_left_cell.plotly_chart(get_wri_sectors_plot())

with col22:
    bottom_right_cell = col22.container(
        border=True, height="stretch", vertical_alignment="center"
    )
    bottom_right_cell.plotly_chart(get_real_wage_growth_plot())


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
    choroplet_cell.plotly_chart(get_wri_by_region_choropleth(sector_selection))
