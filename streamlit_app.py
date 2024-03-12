import streamlit as st
import datetime
import pytz
import os
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots

st.set_page_config(page_title="Defined Range Trading Dashboard", layout="wide")

@st.cache_data
def load_data(file_path):
    df = pd.read_csv(file_path, sep=";", index_col=0, parse_dates=True)
    return df

def median_time_calcualtion(time_array):
    def parse_to_time(value):
        if pd.isna(value):
            return None
        try:
            return datetime.datetime.strptime(value, "%H:%M:%S").time()
        except ValueError:
            raise ValueError("Ungültiges Format. Erwartet wird ein String im Format 'Stunde:Minute:Sekunde'.")

    def time_to_seconds(time_obj):

        return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second

    def seconds_to_time(seconds):
        return datetime.time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

    # Parsen zu datetime.time
    parsed_times = [parse_to_time(value) for value in time_array]

    valid_times = [time_obj for time_obj in parsed_times if not pd.isna(time_obj)]

    # Konvertieren zu Sekunden
    seconds_list = [time_to_seconds(time_obj) for time_obj in valid_times]

    # Median berechnen
    median_seconds = sorted(seconds_list)[len(seconds_list) // 2]

    # Zurückkonvertieren zu datetime.time
    median_time = seconds_to_time(median_seconds)

    return median_time


with st.sidebar:

    symbol_dict = {"NQ": "Nasdaq 100 Futures",
                   "ES": "S&P 500 Futures",
                   "YM": "Dow Jones Futures",
                   "CL": "Light Crude Oil Futures",
                   "GC": "Gold Futures",
                   "EURUSD": "Euro / US- Dollar",
                   "GBPUSD": "British Pound / US- Dollar",
                   }

    symbol = st.sidebar.selectbox(
        "Choose your Symbol?",
        symbol_dict.keys()
    )

    session = st.radio("Choose your Session",
                       ["DR", "oDR"])

    file = os.path.join("dr_data", f"{symbol.lower()}_{session.lower()}.csv")

    df = load_data(file)


st.header(f"DR Analytics Dashboard")
st.write(f':red[{symbol_dict.get(symbol)} ]')

select1, select2 = st.columns(2)

with select1:
    data_filter = st.selectbox("How do you want to filter your data?",
                                (["Total Dataset", "By Day", "By Month", "By Year"]))

with select2:
    if data_filter == "Total Dataset":
        st.empty()
    elif data_filter == "By Day":
        day_options = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday"}
        day = st.selectbox("Select day?", np.unique(df.index.weekday), format_func=lambda x: day_options.get(x))
        df = df[df.index.weekday == day]
    elif data_filter == "By Month":
        month_options = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July",
                         8: "August", 9: "September", 10: "Oktober", 11: "November", 12: "December"}
        month = st.selectbox("Select month?", np.unique(df.index.month), format_func=lambda x: month_options.get(x))
        df = df[df.index.month == month]
    else:
        year = st.selectbox("Select year?", np.unique(df.index.year))
        df = df[df.index.year == year]

st.write("Do you want to narrow down your data further?")
col1, col2 = st.columns(2)

with col1:
    dr_side = st.radio("DR Confirmation side", ("All", "Long", "Short"))
    if dr_side == "Long":
        df = df[df.dr_upday == True]
    elif dr_side == "Short":
        df = df[(df.dr_upday == False) & (df["down_confirmation"].notna())]
    else:
        pass

with col2:
    greenbox = st.radio("Greenbox true", ("All", "True", "False"))
    if greenbox == "True":
        df = df[df.greenbox]
    elif greenbox == "False":
        df = df[df.greenbox == False]
    else:
        st.empty()

time_windows = np.unique(df.breakout_window)
confirmation_time = st.multiselect("Confirmation time of the day", time_windows, default=time_windows)
df = df[df.breakout_window.isin(confirmation_time)]

data_points = len(df.index)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["General Statistics", "Distribution", "Scenario Manager", "FAQ", "Disclaimer"])


def create_plot_df(groupby_column, inverse_percentile=False):
    plot_df = df.groupby(groupby_column).agg({"breakout_window": "count"})
    plot_df = plot_df.rename(columns={"breakout_window": "count"})
    plot_df["pct"] = plot_df["count"] / plot_df["count"].sum()
    plot_df["percentile"] = plot_df["pct"].cumsum()

    if inverse_percentile:
        plot_df["percentile"] = 1- plot_df["percentile"]

    return plot_df


def create_plotly_plot(df, title, x_title, y1_name="Pct", y2_name="Overall likelihood", y1="pct", y2="percentile",
                       line_color="red"):
    subfig = make_subplots(specs=[[{"secondary_y": True}]])

    fig1 = px.bar(df, x=df.index, y=y1)
    fig2 = px.line(df, x=df.index, y=y2, color_discrete_sequence=[line_color])

    fig2.update_traces(yaxis="y2")
    subfig.add_traces(fig1.data + fig2.data)

    subfig.layout.xaxis.title = x_title
    subfig.layout.yaxis.title = y1_name
    subfig.layout.yaxis2.title = y2_name
    subfig.layout.title = title
    subfig.layout.yaxis2.showgrid = False

    return subfig

with tab1:
    st.write("General Statistics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        count_dr_confirmed = len(df[df['dr_confirmed']])
        confirmed_dr = count_dr_confirmed / data_points
        st.metric("DR is confirmed", f"{confirmed_dr:.1%}")

    with col2:
        count_dr_true = len(df[df['dr_true']])
        dr_true = count_dr_true / data_points
        st.metric("DR rule holds True", f"{dr_true:.1%}")

    with col3:
        count_dr_long = len(df[df['dr_upday']])
        dr_conf_long = count_dr_long / data_points
        if dr_side == "All":
            st.metric("Long DR days", f"{dr_conf_long:.1%}")
        elif dr_side == "Long":
            st.metric("Long DR days", f"{1:.0%}")
        else:
            st.metric("Long DR days", f"{0:.0%}")

    with col4:

        st.empty()

    col5, col6, col7, col8 = st.columns(4)

    with col5:
        count_days_with_retracement = len(df[df['retrace_into_dr']])
        dr_retracement = count_days_with_retracement / data_points
        st.metric("Retracement days into DR", f"{dr_retracement:.1%}",
                  help="% of days with retracement into DR range before the high/low of the day happens")

    with col6:
        count_days_with_retracement_idr = len(df[df['retrace_into_idr']])
        idr_retracement = count_days_with_retracement_idr / data_points
        st.metric("Retracement days into iDR", f"{idr_retracement:.1%}",
                  help="% of days with retracement into iDR range before the high/low of the day happens")

    with col7:
        count_dr_winning = len(df[df.close_outside_dr])
        dr_winning_days = count_dr_winning / data_points
        st.metric("Price closes outside DR", f"{dr_winning_days:.1%}",
                  help="In direction of DR confirmation")

    with col8:
        st.empty()

with tab2:

    col3, col4, col5 = st.columns(3)

    with col3:
        median_time = median_time_calcualtion(df["breakout_time"])
        # median_time = statistics.median(df2["breakout_time"])
        st.metric("Median breakout time", value=str(median_time))
        breakout = st.button("See Breakout Distribution", key="breakout")

    with col4:
        median_retracement = median_time_calcualtion(df["max_retracement_time"])
        st.metric("Median retracement before HoS/LoS", value=str(median_retracement),
                  delta=f"Median retracement value: {df.retracement_level.median()}",
                  delta_color="inverse")
        retracement = st.button("See Distribution", key="retracement")

    with col5:

        median_expansion = median_time_calcualtion(df["max_expansion_time"])
        st.metric("Median time of max expansion", value=str(median_expansion),
                  delta=f"Median expansion value: {df.expansion_level.median()}",
                  )
        expansion = st.button("See distribution", key="expansion_time")


    if breakout or (not expansion and not retracement and not breakout):
        st.subheader("Distribution of DR confirmation")
        st.bar_chart(create_plot_df("breakout_window"), y="pct")
    elif retracement:

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])
        if dr_side == "Short":
            df2 = create_plot_df("retracement_level", inverse_percentile=True)
        else:
            df2 = create_plot_df("retracement_level")

        with tab_chart:
            fig = create_plotly_plot(df2, "Distribution of max retracement", "Retracement Level")
            st.plotly_chart(fig, use_container_width=True)
        with tab_data:
            st.dataframe(df2)

    elif expansion:

        if dr_side == "Short":
            df2 = create_plot_df("expansion_level", inverse_percentile=True)
        else:
            df2 = create_plot_df("expansion_level", inverse_percentile=False)

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])

        with tab_chart:
            fig = create_plotly_plot(df2, "Distribution of max expansion", "Expansion Level")
            st.plotly_chart(fig, use_container_width=True)
        with tab_data:
            st.dataframe(df2)

with tab3:

    if dr_side == "All":
        st.warning("Select a DR confirmation side to get useful results.")

    col9, col10, col11 = st.columns(3)

    with col9:
        cur_rt_lvl = round(st.number_input("What is your current level of retracement?", value=0.5, step=0.1), 2)

    with col10:
        ny_time = datetime.datetime.now(pytz.timezone('America/New_York'))
        #cur_time = st.time_input("What is your current time?", value=ny_time)
        st.empty()

    with col11:
        st.empty()

    if dr_side == "Long":
        df_sub = df[df.retracement_level <= cur_rt_lvl]
    else:
        df_sub = df[df.retracement_level >= cur_rt_lvl]

    sub_data_points = len(df_sub.index)

    st.divider()

    col12, col13, col14, col15 = st.columns(4)

    with col12:
        count_dr_true_sub = len(df_sub[df_sub['dr_true']])
        dr_true_sub = count_dr_true_sub / sub_data_points
        st.metric("Probability that DR rule holds True", f"{dr_true_sub:.1%}")

    with col13:
        count_close_outside_dr = len(df_sub[df_sub.close_outside_dr])
        dr_winning_days_sub = count_close_outside_dr / sub_data_points
        st.metric("Probability that price closes outside DR", f"{dr_winning_days_sub:.1%}",
                  help="In direction of DR confirmation")

    with col14:
        median_scenario_ret = df_sub.retracement_level.median()
        median_retracement_sub = median_time_calcualtion(df_sub["max_retracement_time"])
        st.metric(f"median retracement time for this scenario is", str(median_retracement_sub),
                  delta=f"Median retracement level: {median_scenario_ret}")

    with col15:

        median_scenario_exp = df_sub.expansion_level.median()
        median_expansion_sub = median_time_calcualtion(df_sub["max_expansion_time"])

        st.metric(f"median expansion time for this scenario is", str(median_expansion_sub),
                  delta=f"Median expansion level: {median_scenario_exp}")

    col1, col2 = st.columns(2)

    st.write(f"Subset of :red[{len(df_sub)}] datapoints are used for this scenario.")


with tab4:

    dr = st.expander("What does DR/IDR stand for?")
    dr.write("DR stands for defining range and refers to the price range that the price covers within the first hour of trading after the stock exchange opens.")
    dr.write("IDR stands for implied defining range (body close/open high/lows) and refers to the price range that the price covers within the first hour of trading after the stock exchange opens.")

    dr_confirmation = st.expander("What is a DR confirmation (Long/Short)")
    dr_confirmation.write("A DR confirmation refers to the closing of a 5-minute candle above or below the DR high / DR low price level. A close above the DR high is a long confirmation and a close below the DR low level is a short confirmation. ")

    dr_rule = st.expander("What is the DR Rule?")
    dr_rule.write("The DR Rule states that it is very unlikely that the price will close below/above the other side of the DR Range after it has confirmed one side. "
                  "The historical percentages for this can be found in this dashboard.")

    dr_rule.write("No trading recommendation can be derived from this. Please read the disclaimer very carefully.")

    greenbox_rule = st.expander("What is a greenbox?")
    greenbox_rule.write("The greenbox is defined by the opening price and the closing price of the DR range. If the closing price is quoted above the opening price, then the DR range is a green box.")

    get_rich = st.expander("Will this dashboard help me get rich quick?")
    get_rich.write("No, definitely not!")
    get_rich.write("You should definitely read the disclaimer.")

with tab5:
    st.write(
        "The information provided on this website is for informational purposes only and should not be considered as financial advice. "
        "The trading-related statistics presented on this homepage are intended to offer general insights into market trends and patterns. ")

    st.write("However, it is crucial to understand that past performance is not indicative of future results.")

    st.write(
        "Trading and investing involve inherent risks, and individuals should carefully consider their financial situation, risk tolerance, and investment objectives before making any decisions." 
        "The content on this website does not constitute personalized financial advice and should not be interpreted as such.")

    st.write(
        "The website owner and contributors do not guarantee the accuracy, completeness, or timeliness of the information presented. They shall not be held responsible for any errors, omissions, or any actions taken based on the information provided on this website. "
        "Users are strongly advised to consult with a qualified financial advisor or conduct thorough research before making any investment decisions. It is important to be aware of the potential risks and to exercise due diligence when engaging in trading activities.")

    st.write(
        "The website owner and contributors disclaim any liability for any direct, indirect, incidental, or consequential damages arising from the use or reliance upon the information provided on this website. Users assume full responsibility for their actions and are encouraged to seek professional advice when necessary."
        "By accessing this website, you acknowledge and agree to the terms of this disclaimer. The content on this homepage is subject to change without notice."
    )

st.divider()
st.write(f"Statistics based on data points: :red[{len(df)}]")