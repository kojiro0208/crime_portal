# -*- coding: utf-8 -*-
# Copyright 2018-2022 Streamlit Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example of showing geographic data."""

#import altair as alt
from tracemalloc import start
from turtle import width
import numpy as np
import pandas as pd
from datetime import date, datetime

import plotly.express as px
import folium
from folium import plugins
import streamlit as st
from streamlit_folium import folium_static

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="オープンデータ", page_icon=":taxi:")

# LOAD DATA ONCE
# @st.experimental_singleton
def load_aggdata():
    data = pd.read_csv("./tmp.csv")
    data["year_month"] = pd.to_datetime(data["year_month"])
    return data


def cnt_rawdata(start_date, end_date, crimes, pref=None):
    data = pd.read_csv("./tmp2.csv")
    data["発生年月日"] = pd.to_datetime(data["発生年月日"], errors="coerce")
    data = data[(data["発生年月日"].dt.date >= start_date) & (data["発生年月日"].dt.date <= end_date)]
    data = data[data["手口"].isin(crimes)]
    return data["address"].nunique()

def load_rawdata(start_date, end_date, crimes):
    data = pd.read_csv("./tmp2.csv")
    data["発生年月日"] = pd.to_datetime(data["発生年月日"], errors="coerce")
    data = data[(data["発生年月日"].dt.date >= start_date) & (data["発生年月日"].dt.date <= end_date)]
    data = data[data["手口"].isin(crimes)]
    return data


# FUNCTION FOR AIRPORT MAPS
def map(data, lat=None, lon=None, zoom=None):
    # 地図生成（新宿駅中心）
    folium_map = folium.Map(location=[35.9, 139.5], zoom_start=12)
    folium.plugins.HeatMap(
        data = data[["lat", "lng", "cnt"]].values, # ２次元を渡す
        radius=15,    
    ).add_to(folium_map)
    return folium_map


# FILTER DATA FOR A SPECIFIC HOUR, CACHE
@st.experimental_memo
def filterdata(df, hour_selected):
    return df[df["date/time"].dt.hour == hour_selected]


# CALCULATE MIDPOINT FOR GIVEN SET OF DATA
@st.experimental_memo
def mpoint(lat, lon):
    return (np.average(lat), np.average(lon))


# FILTER DATA BY HOUR
@st.experimental_memo
def histdata(df, hr):
    filtered = data[
        (df["date/time"].dt.hour >= hr) & (df["date/time"].dt.hour < (hr + 1))
    ]

    hist = np.histogram(filtered["date/time"].dt.minute, bins=60, range=(0, 60))[0]

    return pd.DataFrame({"minute": range(60), "pickups": hist})


# STREAMLIT APP LAYOUT
data = load_aggdata()

# LAYING OUT THE TOP SECTION OF THE APP
row1_1, row1_2 = st.columns((2, 3))

# SEE IF THERE"S A QUERY PARAM IN THE URL (e.g. ?pickup_hour=2)
# THIS ALLOWS YOU TO PASS A STATEFUL URL TO SOMEONE WITH A SPECIFIC HOUR SELECTED,
# E.G. https://share.streamlit.io/streamlit/demo-uber-nyc-pickups/main?pickup_hour=2
# if not st.session_state.get("url_synced", False):
#     try:
#         pickup_hour = int(st.experimental_get_query_params()["pickup_hour"][0])
#         st.session_state["pickup_hour"] = pickup_hour
#         st.session_state["url_synced"] = True
#     except KeyError:
#         pass

# IF THE SLIDER CHANGES, UPDATE THE QUERY PARAM
def update_query_params():
    hour_selected = st.session_state["pickup_hour"]
    st.experimental_set_query_params(pickup_hour=hour_selected)


with row1_1:
    st.title("NYC Uber Ridesharing Data")
    # hour_selected = st.slider(
    #     "Select hour of pickup", 0, 23, key="pickup_hour", on_change=update_query_params
    # )


with row1_2:
    st.write(
        """
    ##
    Examining how Uber pickups vary over time in New York City"s and at its major regional airports.
    By sliding the slider on the left you can view different slices of time and explore different transportation trends.
    """
    )
    


all_crimes = data["手口"].unique()
crimes = st.multiselect("Choose stocks to visualize", all_crimes, all_crimes)

start_date = st.date_input(
        "Select start date",
        date(2020, 1, 1),
        min_value=datetime.strptime("2018-01-01", "%Y-%m-%d"),
        max_value=datetime.now(),
    )

end_date = st.date_input(
        "Select end date",
        date(2020, 2, 29),
        #datetime.now(),
        min_value=datetime.strptime("2018-01-01", "%Y-%m-%d"),
        max_value=datetime.now(),
    )
# LAYING OUT THE MIDDLE SECTION OF THE APP WITH THE MAPS
col1, col2 = st.columns(2)
# # SETTING THE ZOOM LOCATIONS FOR THE AIRPORTS
# la_guardia = [40.7900, -73.8700]
# jfk = [40.6650, -73.7821]
# newark = [40.7090, -74.1805]
# zoom_level = 12
# midpoint = mpoint(data["lat"], data["lon"])

#with col1:
new_data = data[(data["year_month"].dt.date >= start_date) & (data["year_month"].dt.date <= end_date)]

folium_map = folium.Map(location=[35.9, 139.5], zoom_start=12)
 
if cnt_rawdata(start_date, end_date, crimes) <= 300:
    raw_data = load_rawdata(start_date, end_date, crimes)
    raw_data["発生年月日"] = raw_data["発生年月日"].astype(str)
    # マーカープロット
    for key, row in raw_data.groupby("address"):
        iframe = folium.IFrame(row[["発生年月日", "発生時", "手口"]].sort_values("発生年月日", ascending=False).to_html(index=False))
        popup = folium.Popup(iframe,
                            min_width=300,
                            max_width=400)
        folium.Marker(
            location=[row["lat"].values[0], row["lng"].values[0]],
            popup=popup,
            icon=folium.Icon(color="red")
        ).add_to(folium_map)
    folium_static(folium_map)
    
    st.write(raw_data[["発生年月日", "発生時", "手口", "address"]])
else:
    agg_new_data = new_data[new_data["手口"].isin(crimes)].groupby(["都道府県", "address", "nendo"]).sum().reset_index()
    folium.plugins.HeatMap(
        data = data[["lat", "lng", "cnt"]].values, # ２次元を渡す
        radius=15,    
    ).add_to(folium_map)
    folium_static(folium_map)
    
    #with col2:
    # all_crimes2 = data["手口"].unique()
    # crimes2 = st.multiselect("Choose stocks to visualize", all_crimes2, all_crimes2[:3])
    vis = new_data[new_data["手口"].isin(crimes)].groupby(["都道府県", "year_month"])["cnt"].sum().reset_index()
    #data = data[data["都道府県"].isin(["東京都", "埼玉県"])]
    fig = px.line(vis, x="year_month", y="cnt", color="都道府県", line_group="都道府県")
    fig.update_xaxes(tickformat = "%Y/%m", dtick="M3")
    st.plotly_chart(fig, use_container_width=True)



# with row2_3:
#     st.write("**JFK Airport**")
#     map(filterdata(data, hour_selected), jfk[0], jfk[1], zoom_level)

# with row2_4:
#     st.write("**Newark Airport**")
#     map(filterdata(data, hour_selected), newark[0], newark[1], zoom_level)

# # CALCULATING DATA FOR THE HISTOGRAM
# chart_data = histdata(data, hour_selected)

# # LAYING OUT THE HISTOGRAM SECTION
# st.write(
#     f"""**Breakdown of rides per minute between {hour_selected}:00 and {(hour_selected + 1) % 24}:00**"""
# )

# st.altair_chart(
#     alt.Chart(chart_data)
#     .mark_area(
#         interpolate="step-after",
#     )
#     .encode(
#         x=alt.X("minute:Q", scale=alt.Scale(nice=False)),
#         y=alt.Y("pickups:Q"),
#         tooltip=["minute", "pickups"],
#     )
#     .configure_mark(opacity=0.2, color="red"),
#     use_container_width=True,
# )