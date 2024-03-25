import streamlit as st
from datetime import datetime, time
import pytz
import os
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots

st.set_page_config(page_title="Defining Range Trading Dashboard", layout="wide")

@st.cache_data

def load_data(file_path):
    df = pd.read_csv(file_path, sep=";", index_col=0, parse_dates=True)
    return df


def median_time_calcualtion(time_array):
    def parse_to_time(value):
        if pd.isna(value):
            return None
        if isinstance(value, time):
            return value
        else:
            try:
                return datetime.strptime(value, "%H:%M:%S").time()
            except ValueError:
                raise ValueError("Ungültiges Format. Erwartet wird ein String im Format 'Stunde:Minute:Sekunde'.")

    def time_to_seconds(time_obj):

        return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second

    def seconds_to_time(seconds):
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

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
                   "FDAX": "DAX Futures"
                   }

    symbol = st.sidebar.selectbox(
        "Choose your Symbol?",
        symbol_dict.keys()
    )

    session = st.radio("Choose your Session",
                       ["DR", "oDR"])

    file = os.path.join("dr_data", f"{symbol.lower()}_{session.lower()}.csv")

    df = load_data(file)

    st.divider()


breakout = True

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
inv_param = [False if dr_side == "Long" else True][0]

general_tab, distribution_tab, scenario_manager, faq_tab, disclaimer= st.tabs(["General Statistics", "Distribution", "Scenario Manager", "FAQ", "Disclaimer"])


def create_plot_df(df, groupby_column, inverse_percentile=False, ascending=True):
    plot_df = df.groupby(groupby_column).agg({"breakout_window": "count"})
    plot_df = plot_df.rename(columns={"breakout_window": "count"})
    plot_df["pct"] = plot_df["count"] / plot_df["count"].sum()
    plot_df["percentile"] = plot_df["pct"].cumsum()

    if inverse_percentile:
        plot_df["percentile"] = 1- plot_df["percentile"]

    if not ascending:
        plot_df = plot_df.sort_index(ascending=False)
    return plot_df


def create_plotly_plot(df, title, x_title, y1_name="Pct", y2_name="Overall likelihood", y1="pct", y2="percentile",
                       line_color="red", reversed_x_axis=False):
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

    if reversed_x_axis:
        subfig.update_layout(
            xaxis=dict(autorange="reversed")
        )

    return subfig

def create_join_table(first_symbol, second_symbol):

    cols_to_use = ["date", "greenbox", "breakout_time", "dr_upday", "max_retracement_time", "max_expansion_time", "retracement_level", "expansion_level", "closing_level"]

    first_symbol = first_symbol.lower()
    second_symbol = second_symbol.lower()

    file1 = os.path.join("dr_data", f"{first_symbol}_{session.lower()}.csv")
    file2 = os.path.join("dr_data", f"{second_symbol}_{session.lower()}.csv")

    if first_symbol == second_symbol:
        pass

    df1 = pd.read_csv(file1, sep=";", index_col=[0], usecols=cols_to_use)
    df2 = pd.read_csv(file2, sep=";", index_col=[0], usecols=cols_to_use)

    df_join = df1.join(df2, lsuffix=f"_{first_symbol}", rsuffix=f"_{second_symbol}", how="left")



    # df[f"breakout_time_{first_symbol}"] = pd.to_datetime(df[f"breakout_time_{first_symbol}"], format="%H:%M:%S")
    # df[f"breakout_time_{second_symbol}"] = pd.to_datetime(df[f"breakout_time_{second_symbol}"], format="%H:%M:%S")

    # df["breakout_dif"] = (df[f"breakout_time_{second_symbol}"] - df[f"breakout_time_{first_symbol}"]).dt.total_seconds()/60
    # df["retracement_dif"] = df[f"retracement_level_es"] - df["retracement_level_nq"]
    # df["expansion_dif"] = df["expansion_level_es"] - df["expansion_level_nq"]

    return df_join

with general_tab:

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

        if dr_side == "Long":

            breach_count = len(df[df['breached_dr_low']])
            breach_pct = 1 - (breach_count / data_points)
            st.metric("DR low unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks below DR low")

        elif dr_side == "Short":
            breach_count = len(df[df['breached_dr_high']])
            breach_pct = 1 - (breach_count / data_points)
            st.metric("DR high unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks above DR high")

        else:
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

with distribution_tab:

    col3, col4, col5 = st.columns(3)

    with col3:
        median_time = median_time_calcualtion(df["breakout_time"])
        # median_time = statistics.median(df2["breakout_time"])
        st.metric("Median confirmation time:", value=str(median_time))
        breakout = st.button("See Confirmation Distribution", key="breakout")

    with col4:
        median_retracement = median_time_calcualtion(df["max_retracement_time"])
        st.metric("Median retracement before HoS/LoS:", value=str(median_retracement),
                  delta=f"Median retracement value: {df.retracement_level.median()}",
                  delta_color="inverse")
        retracement = st.button("See Distribution", key="retracement")

    with col5:

        median_expansion = median_time_calcualtion(df["max_expansion_time"])
        st.metric("Median time of max expansion:", value=str(median_expansion),
                  delta=f"Median expansion value: {df.expansion_level.median()}",
                  )
        expansion = st.button("See distribution", key="expansion_time")


    #if breakout or (not expansion and not retracement and not breakout):
    if breakout:
        st.write("**Distribution of DR confirmation**")
        st.bar_chart(create_plot_df(df, "breakout_window"), y="pct")

    elif retracement:

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])
        if dr_side == "Short":
            df2 = create_plot_df(df, "retracement_level", inverse_percentile=False, ascending=True)
        else:
            df2 = create_plot_df(df, "retracement_level", inverse_percentile=True)


        with tab_chart:
            if dr_side == "Short":
                fig = create_plotly_plot(df2, "Distribution of max retracement before high/low of the session", "Retracement Level", reversed_x_axis=False)
            else:
                fig = create_plotly_plot(df2, "Distribution of max retracement before high/low of the session", "Retracement Level", reversed_x_axis=True)
            st.plotly_chart(fig, use_container_width=True)

            if not breakout:
                st.caption(
                    "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
                st.caption(
                    "Level :red[0] is the low of the DR range and level :red[1] is the high of the DR range (wicks).")
            st.divider()
            st.write("**Distribution of max retracement time before high/low of the session**")
            st.bar_chart(df.groupby("max_retracement_time").agg({"max_retracement_value": "count"}), use_container_width=True)

        with tab_data:
            st.dataframe(df2)

    elif expansion:

        if dr_side == "Short":
            df2 = create_plot_df(df, "expansion_level", inverse_percentile=True, ascending=False)
        else:
            df2 = create_plot_df(df, "expansion_level", inverse_percentile=False)

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])

        with tab_chart:
            if dr_side == "Short":
                fig = create_plotly_plot(df2, "Distribution of max expansion before high/low of the session", "Expansion Level", reversed_x_axis=True)
            else:
                fig = create_plotly_plot(df2, "Distribution of max expansion before high/low of the session", "Expansion Level", reversed_x_axis=False)
            st.plotly_chart(fig, use_container_width=True)

            if not breakout:
                st.caption(
                    "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
                st.caption(
                    "Level :red[0] is the low of the DR range and level :red[1] is the high of the DR range (wicks).")
            st.divider()

            st.write("**Distribution of max expansion time before high/low of the session**")
            st.bar_chart(df.groupby("max_expansion_time").agg({"max_expansion_value": "count"}), use_container_width=True)
        with tab_data:
            st.dataframe(df2)


with scenario_manager:
    ny_time = datetime.now(pytz.timezone('America/New_York'))
    col_1, col_2 = st.columns(2)

    with col_1:
        dr_conf = st.checkbox("DR Confirmation", value=True)

    with col_2:
        time_mode = st.checkbox("Consider time as filter option")

    col9, col10, col11 = st.columns(3)
    #Confirmation Scenario
    if dr_conf:
        if dr_side == "All":
            st.error("Please select DR confirmation side for useful results!")

        with col9:
            cur_rt_lvl = round(st.number_input("What is your current level of retracement?", value=0.5, step=0.1,
                                               help="Deselects datapoints that show a less strong retracenent"), 2)
            if dr_side == "Long":
                df_sub = df[df.retracement_level <= cur_rt_lvl]
            else:
                df_sub = df[df.retracement_level >= cur_rt_lvl]

        with col10:

            if time_mode:

                cur_time = st.time_input("Deselect max retracement times before:" , value=ny_time,
                                         help="Deselects datapoints where the maximum of retracement already happend")

                df_sub['max_retracement_time'] = pd.to_datetime(df_sub['max_retracement_time'], format='%H:%M:%S').dt.time

                df_sub = df_sub[df_sub.max_retracement_time >= cur_time]
            else:
                st.empty()
        with col11:
            st.empty()

    # Scenario before Confirmation
    else:
        df_sub = df
        if (dr_side != "All") or (greenbox == "All"):
            st.error("Please select confirmation side option \"All\" and a greenbox side for correct results!")
        if time_mode:
            col_3, col_4 = st.columns(2)
            with col_3:
                cur_time = st.time_input("Deselect with confirmation before:", value=ny_time,
                                         help="Deselects datapoints where the confirmation already happend")

                df_sub['max_retracement_time'] = pd.to_datetime(df_sub['breakout_time'], format='%H:%M:%S').dt.time

                df_sub = df_sub[df_sub.max_retracement_time >= cur_time]
            with col_4:
                st.empty()


        st.empty()

    st.divider()

    col12, col13, col14, col15 = st.columns(4)

    sub_data_points = len(df_sub.index)

    with col12:
        if dr_conf:
            count_dr_true_sub = len(df_sub[df_sub['dr_true']])
            dr_true_sub = count_dr_true_sub / sub_data_points
            st.metric("Probability that DR rule holds True", f"{dr_true_sub:.1%}")
        else:
            count_dr_up_sub = len(df_sub[df_sub['dr_upday']])
            st.metric("Probability of Long confirmation", f"{count_dr_up_sub/ sub_data_points:.1%}")

    with col13:
        if dr_conf:
            count_close_outside_dr = len(df_sub[df_sub.close_outside_dr])
            dr_winning_days_sub = count_close_outside_dr / sub_data_points
            st.metric("Probability that price closes outside DR", f"{dr_winning_days_sub:.1%}",
                      help="In direction of DR confirmation")

        else:
           # count_dr_up_sub = len(df_sub[df_sub['dr_upday']])
            st.metric("Probability of Short confirmation", f"{1 - (count_dr_up_sub/ sub_data_points):.1%}")

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

    col16, col17 = st.columns(2)

    with col16:
        if dr_side == "Long":

            breach_count = len(df_sub[df_sub['breached_dr_low']])
            breach_pct = 1 - (breach_count / sub_data_points)
            st.metric("DR low unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks below DR low")

        elif dr_side == "Short":
            breach_count = len(df_sub[df_sub['breached_dr_high']])
            breach_pct = 1 - (breach_count / sub_data_points)
            st.metric("DR high unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks above DR high")

        else:
            st.empty()

    # Plotting Area
    col_5, col_6 = st.columns(2)
    if dr_conf:

        with col_5:
            df2_sub = create_plot_df(df_sub, "retracement_level", inverse_percentile=inv_param)
            fig = create_plotly_plot(df2_sub, "Distribution of max retracement", "Retracement Level")
            st.plotly_chart(fig, use_container_width=True)
        with col_6:
            df2_sub = create_plot_df(df_sub, "expansion_level", inverse_percentile=not inv_param)
            fig = create_plotly_plot(df2_sub, "Distribution of max expansion", "Expansion Level")
            st.plotly_chart(fig, use_container_width=True)


    else:
        conf_plot = df_sub.groupby("closing_level").agg(
            {"dr_upday": "mean", "dr_true": "count"}).rename(
            columns={"dr_upday": "pct", "dr_true": "count"})

        if greenbox == "False":
            conf_plot[" "] = 1 - conf_plot["pct"]

        fig = px.bar(conf_plot, y='pct', x=conf_plot.index, text='count')
        st.plotly_chart(fig, use_container_width=True)

    st.write(f"Subset of :red[{len(df_sub)}] datapoints are used for this scenario.")

with faq_tab:

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

    indicator = st.expander("Is there a good TradingView indicator?")
    indicator.write("I personally like the TheMas7er scalp (US equity) 5min [promuckaj] indicator. It comes with a lot of features but there are plenty of other free indicators available")

    data_refresh = st.expander("How often is the data updated?")
    data_refresh.write("At the moment the collection of data is a time intensive manual process. Therefore there is no regular interval. I will update the data once in while.")

    get_rich = st.expander("Will this dashboard help me get rich quick?")
    get_rich.write("No, definitely not!")
    get_rich.write("You should definitely read the disclaimer.")

with disclaimer:
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

# with test:
#
#     st.empty()

st.divider()
start_date = df.index[0].strftime("%Y-%m-%d")
end_date = df.index[-1].strftime("%Y-%m-%d")
st.write(f"Statistics based on :red[{len(df)}] data points from :red[{start_date}] to :red[{end_date}]")

#st.write(create_join_table(symbol, "es"))
