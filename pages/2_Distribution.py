import streamlit as st
import datetime
from datetime import time
import pandas as pd
import numpy as np

st.set_page_config(page_title="Distribution Overview", layout="wide")
st.header("Distribution Overview ")

df_daily = pd.read_csv("output_daily.csv", sep=";", index_col=0, parse_dates=True)
#Filters out days without confirmation
df_daily = df_daily[df_daily.first_breakout_time.notna()]

select1, select2 = st.columns(2)

with select1:
    data_filter = st.selectbox("How do you want to filter your data?", (["Total Dataset", "By Day", "By Month", "By Year"]))

with select2:
    if data_filter == "Total Dataset":
        st.empty()
    elif data_filter == "By Day":
        day_options = {0: "Monday", 1: "Thuesday", 2: "Wendnesday", 3: "Thursday", 4: "Friday"}
        day = st.selectbox("Select day?", np.unique(df_daily.index.weekday), format_func=lambda x: day_options.get(x))
        df_daily = df_daily[df_daily.index.weekday == day]
    elif data_filter == "By Month":
        month_options = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July", 8: "August", 9: "September", 10: "Oktober", 11: "November", 12: "December"}
        month = st.selectbox("Select month?", np.unique(df_daily.index.month), format_func=lambda x: month_options.get(x))
        df_daily = df_daily[df_daily.index.month == month]
    else:
        year = st.selectbox("Select year?", np.unique(df_daily.index.year))
        df_daily = df_daily[df_daily.index.year == year]


st.divider()
##########################################################
st.write("Do you want to narrow your data down further?")
col1, col2, col3 = st.columns(3)

with col1:
    dr_side = st.radio("DR Confirmation side", ("All", "Long", "Short"))
    if dr_side == "Long":
        df_daily = df_daily[df_daily.dr_up_day == True]
    elif dr_side == "Short":
        df_daily = df_daily[df_daily.dr_up_day == False]
    else:
        st.empty()

with col2:
    dr_true = st.radio("DR Rule holds true", ("All", "True", "False"))
    if dr_true == "True":
        df_daily = df_daily[df_daily.DR_true == True]
    elif dr_true == "False":
        df_daily = df_daily[df_daily.DR_true == False]
    else:
        st.empty()

with col3:
    win_radio = st.radio("Price closed outside DR (win)", ("All", "True", "False"))
    if win_radio == "True":
        df_daily = df_daily[df_daily.price_outside_dr_eod == True]
    elif win_radio == "False":
        df_daily = df_daily[df_daily.price_outside_dr_eod == False]


time_windows = np.unique(df_daily.breakout_window)
confirmation_time = st.multiselect("Confirmation time of the day", time_windows, default=time_windows)
df_daily = df_daily[df_daily.breakout_window.isin(confirmation_time)]

st.write(f"Selected data points: :green[{len(df_daily.index)}]")

st.divider()
#####################################################################
expansion_table = df_daily.groupby("expantion_level").agg({"dr_profit_points": "mean", "retracement_into_dr": "mean", "first_breakout_time": "count"})
expansion_table = expansion_table.rename(columns={"first_breakout_time": "count"})



retracement_table = df_daily.groupby("retracement_level").agg({"dr_profit_points": "mean", "retracement_into_dr": "mean", "first_breakout_time": "count"})
retracement_table = retracement_table.rename(columns={"first_breakout_time": "count"})


retr_time_table = df_daily.groupby("max_retracement_time").agg({"max_retracement_value": "count",})
retr_time_table = retr_time_table.rename(columns={"max_retracement_value": "count"})

col3, col4, col5 = st.columns(3)

with col3:
    breakout_time = int(df_daily['first_breakout_time_seconds'].median())
    st.metric("Median expansion", value=df_daily.expantion_level.median(),
              delta=f"Median breakout time: {breakout_time // 3600}:{str(breakout_time % 3600 // 60).zfill(2)}")
    expansion = st.button("See Distribution", key="expansion")

with col4:
    retracement_time = int(df_daily['max_retracement_time_seconds'].mean())
    st.metric("Median Retracement before HoS/LoS", value=df_daily.retracement_level.median(),
              delta=f"Mean time of max retracement: {retracement_time // 3600}:{str(retracement_time % 3600 // 60).zfill(2)}",
              delta_color="inverse")

    retracement = st.button("See Distribution", key="retracement")

with col5:
    #st.write("Distribution of max retracement time")
    retracement_time_median = int(df_daily['max_retracement_time_seconds'].median())
    st.metric("Distribution of max tetracement time before HoS/LoS", value="-",
                delta = f"Median time of max retracement: {retracement_time_median // 3600}:{str(retracement_time_median % 3600 // 60).zfill(2)}",
                delta_color = "inverse")
    retracement_time = st.button("See distribution", key="retracement_time")
    st.write("    ")
st.divider()


if expansion or (expansion == False and retracement == False and retracement_time == False ):
    st.subheader("Expansion distribution")
    st.bar_chart(expansion_table, y="count")
elif retracement:
    st.subheader("Retracement distribution")
    st.bar_chart(retracement_table, y="count")
elif retracement_time:
    st.subheader("Distribution of max retracement")
    st.bar_chart(retr_time_table, y="count")


def calc_reversal_prob(df):


    #Calulates the prob that price closes outside DR
    df = df.groupby(["retracement_level", "price_outside_dr_eod"]).agg({"dr_up_day": "count"})
    df = df.reset_index().set_index("retracement_level")

    for index in np.unique(df.index):
        #print(f"fib level is: {index}")
        true_count = df[(df.index == index) & (df.price_outside_dr_eod)]["dr_up_day"].sum()

        total_count = df[(df.index == index)]["dr_up_day"].sum()
        win_percentage = true_count / total_count

        print(f"win rate at {index} is: {win_percentage}")
        df.at[index, "close_rate_outside_dr_after_retracement"] = win_percentage
        df.at[index, "sample_size"] = total_count

    df = df.groupby(df.index).agg({"close_rate_outside_dr_after_retracement": "first", "sample_size": "first"})
    return df

if dr_side != "Both Sides" and retracement:

    with st.expander("Show reversal probabilities"):
        reversal_df = calc_reversal_prob(df_daily)
        st.subheader("Probability that the price closes outside DR depending on the retracement level")
        st.bar_chart(reversal_df, y="close_rate_outside_dr_after_retracement")



st.divider()
### How high is the chance that price closes above DR when reaching a specific Retracemnt level?
## How high is the chance that price breakes DR Rule?

