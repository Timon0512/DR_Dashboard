import streamlit as st
import datetime
from datetime import time
import pandas as pd
import numpy as np

st.title("My DR Lens Dashboard")
st.subheader(f"Today is: {datetime.date.today()}")
#st.write("This is :green[test]")
st.divider()


df_daily = pd.read_csv("output_daily.csv", sep=";", index_col=0)

def create_propability_table(df_daily):
    # Filters out days without confirmation
    df_daily = df_daily[df_daily.first_breakout_time.notna()]

    timestamps = np.unique(df_daily.first_breakout_time)
    winning_propability = []
    avg_points = []
    sample_size = []

    for time in timestamps:
        filter_df = df_daily[df_daily.first_breakout_time >= time]
        count_true = filter_df[filter_df.price_outside_dr_eod].dr_up_day.count()
        win_prob = count_true / len(filter_df.dr_up_day)
        winning_propability.append(win_prob)
        avg_points.append(filter_df.dr_profit_points.mean())
        sample_size.append(len(filter_df.dr_up_day))

    entry_prob = pd.DataFrame(
        {"time": timestamps, "winning_propability": winning_propability, "avg_points": avg_points, "sample_size": sample_size})

    return entry_prob

#df_entry_prob = create_propability_table(df_daily)
long_short_select = st.selectbox("Select your Confirmation Side", ["All Days", "Long Confirmation Days", "Short Confirmation Days"])


if long_short_select == "Long Confirmation Days":
    df_daily = df_daily[df_daily.dr_up_day == True]
elif long_short_select == "Short Confirmation Days":
    df_daily = df_daily[df_daily.dr_up_day == False]

col1, col2, col3, col4 = st.columns(4)

df_daily_len = len(df_daily.index)

with col1:
    count_dr_confirmed = len(df_daily[df_daily['dr_confirmed'] == True])
    confirmed_dr = count_dr_confirmed / df_daily_len
    st.metric("DR is confirmed", f"{confirmed_dr:.1%}")

with col2:
    count_dr_true = len(df_daily[df_daily['DR_true'] == True])
    dr_true = count_dr_true / df_daily_len
    st.metric("DR rule holds True", f"{dr_true:.1%}")


with col3:
    count_dr_long = len(df_daily[df_daily['dr_up_day'] == True])
    dr_conf_long = count_dr_long / df_daily_len
    if long_short_select == "All Days":
        st.metric("Long DR days", f"{dr_conf_long:.1%}")
    elif long_short_select == "Long Confirmation Days":
        st.metric("Long DR days", f"{1:.0%}")
    else:
        st.metric("Long DR days", f"{0:.0%}")


with col4:

    breakout_time = int(df_daily['first_breakout_time_seconds'].median())
    #breakout_time = time(breakout_time // 3600, (breakout_time % 3600) // 60, breakout_time % 60)
    #st.metric("Median breakout time", breakout_time, help="Median Time of DR Breakout")
    st.write(f"Median Breakout time:")
    st.write(f"{breakout_time // 3600}:{str(breakout_time % 3600 // 60).zfill(2)}")
#
#############################################################################################
col5, col6, col7, col8 = st.columns(4)

with col5:
    count_days_with_retracement = len(df_daily[df_daily['retracement_into_dr']])
    dr_retracement = count_days_with_retracement / df_daily_len
    st.metric("Retracement days into DR", f"{dr_retracement:.1%}",
              help="% of days with retracement into DR range before the high/low of the day happens")

with col6:
    count_days_with_retracement_idr = len(df_daily[df_daily['retracement_into_idr']])
    idr_retracement = count_days_with_retracement_idr / df_daily_len
    st.metric("Retracement days into iDR", f"{idr_retracement:.1%}",
              help="% of days with retracement into iDR range before the high/low of the day happens")

with col7:
    count_dr_winning = len(df_daily[df_daily.dr_profit_points > 0])
    dr_winning_days = count_dr_winning / df_daily_len
    st.metric("Price closes outside DR", f"{dr_winning_days:.1%}")

with col8:
    retracement_time = int(df_daily['max_retracement_time_seconds'].median())
    st.write(f"Median retracement time:")
    st.write(f"{retracement_time // 3600}:{str(retracement_time % 3600 // 60).zfill(2)}")



# #st.dataframe(df_daily)
st.write(f"{df_daily_len} data entries selected")
st.divider()
#
# # Create Breakout time distribution table
#
st.subheader("Distribution of DR Breakout time")

tog1, tog2, select3 = st.columns(3)

with tog1:
    hist_toggle = st.toggle('Relative / Absolut Values')
with tog2:
    win_toggle = st.toggle('Only winning trades')

select_options = np.unique(df_daily.breakout_window.dropna())
breakout_window = st.multiselect("Select DR breakout window:", options=select_options, default=select_options)
df = df_daily[df_daily.breakout_window.isin(breakout_window)]


if win_toggle:
    df = df[df.price_outside_dr_eod == True].groupby("first_breakout_time").agg({"DR_true": "count", "retracement_into_dr": "mean"})
    df = pd.DataFrame(df).rename(columns={"DR_true": "Count of Breakouts"})
    df["Breakout Distribution"] = df["Count of Breakouts"] / df["Count of Breakouts"].sum()
else:
    #df = df_daily.groupby("first_breakout_time").DR_true.count()
    df = df.groupby("first_breakout_time").agg({"DR_true": "count", "retracement_into_dr": "mean"})
    df = pd.DataFrame(df).rename(columns={"DR_true": "Count of Breakouts"})
    df["Breakout Distribution"] = df["Count of Breakouts"] / df["Count of Breakouts"].sum()


if hist_toggle:
    st.bar_chart(df, y="Count of Breakouts")

else:
    st.bar_chart(df, y="Breakout Distribution")


st.bar_chart(df, y="retracement_into_dr")

st.divider()
####################################################



st.divider()
####################################################

st.subheader("Distribution of retracement time before high/low of the day")

retracement_df = df_daily.groupby("first_breakout_time").retracement_into_dr.mean()
st.dataframe(df)

st.bar_chart(df, y="retracement_into_dr")
