import streamlit as st
from datetime import datetime, time
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pickle

st.set_page_config(page_title="Opening Range Breakout Analytics", layout="wide")

bar_color = "#223459"
line_color = "#FF4B4B"

if 'retracement_button' not in st.session_state:
    st.session_state['retracement_button'] = False

if 'expansion_button' not in st.session_state:
    st.session_state['expansion_button'] = False

if 'range_button' not in st.session_state:
    st.session_state['range_button'] = False

if 'breakout_button' not in st.session_state:
    st.session_state['breakout_button'] = True

if 'use_orb_body' not in st.session_state:
    st.session_state['use_orb_body'] = False

@st.cache_data
def load_data(file_path):
    df = pd.read_csv(file_path, sep=";", index_col=0, parse_dates=True)
    date_cols = ["up_confirmation", "down_confirmation", "breakout_time", "max_retracement_time", "max_expansion_time"]
    df[date_cols] = df[date_cols].apply(pd.to_datetime, unit="us", utc=True)
    df[date_cols] = df[date_cols].apply(lambda x: x.dt.tz_convert('America/New_York'))
    df[date_cols] = df[date_cols].apply(lambda x: x.dt.time)

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


def create_plot_df(df, groupby_column, inverse_percentile=False, ascending=True):
    plot_df = df.groupby(groupby_column).agg({"breakout_window": "count"})
    plot_df = plot_df.rename(columns={"breakout_window": "count"})
    plot_df["pct"] = plot_df["count"] / plot_df["count"].sum()
    plot_df["percentile"] = plot_df["pct"].cumsum()

    if inverse_percentile:
        plot_df["percentile"] = 1 - plot_df["percentile"]

    if not ascending:
        plot_df = plot_df.sort_index(ascending=False)
    return plot_df


def create_plotly_plot(df, title, x_title, y1_name="Pct", y2_name="Overall likelihood", y1="pct", y2="percentile",
                       line_color="red", reversed_x_axis=False):
    subfig = make_subplots(specs=[[{"secondary_y": True}]])

    fig1 = px.bar(df, x=df.index, y=y1, color_discrete_sequence=[bar_color])
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
    cols_to_use = ["date", "greenbox", "breakout_time", "upday", "max_retracement_time", "max_expansion_time",
                   "retracement_level", "expansion_level", "closing_level"]

    first_symbol = first_symbol.lower()
    second_symbol = second_symbol.lower()

    file1 = os.path.join("data", f"{first_symbol}_{session.lower()}.csv")
    file2 = os.path.join("data", f"{second_symbol}_{session.lower()}.csv")

    if first_symbol == second_symbol:
        pass

    df1 = pd.read_csv(file1, sep=";", index_col=[0], usecols=cols_to_use)
    df2 = pd.read_csv(file2, sep=";", index_col=[0], usecols=cols_to_use)

    df_join = df1.join(df2, lsuffix=f"_{first_symbol}", rsuffix=f"_{second_symbol}", how="left")

    return df_join


def load_ml_model(symbol):
    # load model
    # try:
    filepath_ml_model = os.path.join("ml_models",
                                     f"{symbol.lower()}_{session_dict.get(session)}_simple_confirmation_bias_model.pickle")
    filepath_ml_scaler = os.path.join("ml_models",
                                      f"{symbol.lower()}_{session_dict.get(session)}_simple_confirmation_bias_scaler.pickle")

    try:
        loaded_model = pickle.load(open(filepath_ml_model, "rb"))
        loaded_scaler = pickle.load(open(filepath_ml_scaler, "rb"))

    except FileNotFoundError:
        return 0, f"No trained model for {symbol} available"

    return loaded_model, loaded_scaler


with st.sidebar:
    symbol_dict = {"NQ": "Nasdaq 100 Futures",
                   "ES": "S&P 500 Futures",
                   "YM": "Dow Jones Futures",
                   "CL": "Light Crude Oil Futures",
                   "GC": "Gold Futures",
                   "EURUSD": "Euro / US- Dollar",
                   "GBPUSD": "British Pound / US- Dollar",
                   "AUDJPY": "Australian Dollar / Japanese Yen",
                   "FDAX": "DAX Futures"
                   }

    symbol = st.sidebar.selectbox(
        "Choose your Symbol?",
        symbol_dict.keys())

    session_dict = {"New York (9:30 - 16:00 EST)": "ny",
                    "London (3:00 - 8:30 EST)": "ldn",
                    "Tokyo (09:30 - 14:30 JST)": "asia"}

    session = st.radio("Choose your Session",
                       ["New York (9:30 - 16:00 EST)",
                        "London (3:00 - 8:30 EST)",
                        "Tokyo (09:30 - 14:30 JST)"])

    orb_duration = st.sidebar.selectbox("Choose Opening Range Duration", [60, 30])

    file = os.path.join("data", f"{symbol.lower()}_{session_dict.get(session)}_{orb_duration}.csv")
    df = load_data(file)
    st.divider()

breakout = True

st.header(f"Opening Range Breakout Analytics")
st.write(f':red[{symbol_dict.get(symbol)} ]')

select1, select2, select3 = st.columns(3)

with select1:
    data_filter = st.selectbox("How do you want to filter your data?",
                               (["Total Dataset", "By Day", "By Month", "By Year"]))

with select2:
    if data_filter == "Total Dataset":
        st.empty()
        # st.selectbox("Select day?", ["None"])
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

with select3:
    st.empty()
    # model_radio = st.checkbox("Filter by current Session Model", [True, False])
    # if model_radio:
    #     model_filter = st.multiselect("Filter by Session Model",
    #                                   df.model.unique(),
    #                                   default=None,
    #                                   placeholder="All Models")


st.write("Do you want to narrow down your data further?")
col1, col2, col3 = st.columns(3)

with col1:
    orb_side = st.radio("Range breakout side", ("All", "Long", "Short"))
    if orb_side == "Long":
        df = df.loc[df.upday]

    elif orb_side == "Short":
        df = df[(~df.upday) & (df["down_confirmation"].notna())]
    else:
        pass

with col2:
    greenbox = st.radio("Greenbox true", ("All", "True", "False"))
    if greenbox == "True":
        df = df[df.greenbox]
    elif greenbox == "False":
        df = df[~df.greenbox]
    else:
        st.empty()

with col3:

    model_list = list(df.model.dropna().unique()) + ["All Models", "All Upside Models", "All Downside Models", "Upside + Expansion", "Downside + Expansion"]
    model_list.sort()

    model_filter = st.selectbox("Filter by Session Model",
                                model_list,
                                index=1,
                                placeholder="All Session Models selected"
                                      )

    if model_filter == "All Upside Models":
        model_filter = ["Strong Uptrend", "Medium Uptrend", "Weak Uptrend",]
    elif model_filter == "All Downside Models":
        model_filter = ["Strong Downtrend", "Medium Downtrend", "Weak Downtrend", ]
    elif model_filter == "Upside + Expansion":
        model_filter = ["Strong Uptrend", "Medium Uptrend", "Weak Uptrend", "Expansion"]
    elif model_filter == "Downside + Expansion":
        model_filter = ["Strong Downtrend", "Medium Downtrend", "Weak Downtrend", "Expansion"]
    elif model_filter == "All Models":
        model_filter = ["Strong Downtrend", "Medium Downtrend", "Weak Downtrend", "Expansion", "Contraction", "Strong Uptrend", "Medium Uptrend", "Weak Uptrend"]
    else:
        model_filter = [model_filter]

model_filter = model_filter + ["No Model"]

df["breakout_window"] = df["breakout_window"].fillna("No Breakout")
df["model"] = df["model"].fillna("No Model")
time_windows = (df["breakout_window"].unique())
breakout_time = st.multiselect("Breakout time of the day", time_windows, default=time_windows)

df = df[(df.breakout_window.isin(breakout_time)) &
        (df.model.isin(model_filter))]


data_points = len(df.index)
inv_param = [False if orb_side == "Long" else True][0]

general_tab, distribution_tab, model, strategy_tester, strategy_rules, faq_tab, disclaimer, ml, = \
    st.tabs(["General Statistics", "Distribution", "Model Section", "Stategy Backtester", "Strategy Rules", "FAQ",
             "Disclaimer", "Machine Learning", ])

if len(df) == 0:
    st.error("No data has been selected. Please change the filter settings .")
    st.stop()

with general_tab:
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        count_range_confirmed = len(df[df['range_confirmed']])
        confirmed_orb = count_range_confirmed / data_points
        st.metric("Range breakouts", f"{confirmed_orb:.1%}")

    with col2:
        count_range_holds = len(df[df['range_holds']])
        range_holds = count_range_holds / data_points
        st.metric("Opposite Range holds (body)", f"{range_holds:.1%}",
                  help="No candle close below/above the opposite side of the confirmed range.")

    with col3:
        count_days_with_retracement = len(df[df['retrace_into_range']])
        orb_retracement = count_days_with_retracement / data_points
        st.metric("Retracement days into Range", f"{orb_retracement:.1%}",
                  help="% of days with retracement into opening range before the high/low of the day happens")

    with col4:

        count_orb_winning = len(df[df.close_outside_range])
        orb_winning_days = count_orb_winning / data_points
        st.metric("Price closes outside opening range", f"{orb_winning_days:.1%}",
                  help="In direction of opening range breakout")

    col5, col6, col7, col8 = st.columns(4)

    with col5:
        count_orb_long = len(df[df['upday']])
        orb_conf_long = count_orb_long / data_points
        if orb_side == "All":
            st.metric("Long breakout days", f"{orb_conf_long:.1%}")
        elif orb_side == "Long":
            st.metric("Long breakout days", f"{1:.0%}")
        else:
            st.metric("Long breakout days", f"{0:.0%}")

    with col6:

        breach_count = len(df[(df.breached_range_high) & df.breached_range_low])
        breach_pct = 1 - (breach_count / len(df))

        st.metric("Opposite Range holds (wick)", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks through opposite range of the orb breakout")

    with col7:
        st.metric("Median Opening Range Size Multiplier",
                  round(df.range_multiplier.median(), 1),
                  help="Compares the size of the Opening Range with the opening range size of the previous session.")

    with col8:
        st.empty()

with distribution_tab:
    use_orb_body = st.toggle("Use candle bodys for OR calculation",
                              help="Uses bodys to determine the opening range instead of wicks.")

    #Retracement/Expansion DF
    df_ret = df[["retracement_window", "max_expansion_time"]].groupby(["retracement_window"]).count()
    df_ret["pct"] = df_ret["max_expansion_time"] / df_ret["max_expansion_time"].sum()
    df_exp = df[["max_retracement_time", "expansion_window"]].groupby(["expansion_window"]).count()
    df_exp["pct"] = df_exp["max_retracement_time"] / df_exp["max_retracement_time"].sum()

    df_ret = df_ret.join(df_exp, lsuffix=" retracement", rsuffix=" expansion")
    df_ret = df_ret.fillna(0)

    if use_orb_body:
        st.session_state["use_orb_body"] = True
    else:
        st.session_state["use_orb_body"] = False

    range_dis, col3, col4, col5,  = st.columns(4)
    with col3:

        median_time = median_time_calcualtion(df["breakout_time"])
        st.metric("Median breakout time:", value=str(median_time),
                  delta=f"Mode breakout time: {df.breakout_time.mode()[0]}")
        breakout = st.button("See Distribution", key="breakout")

        if breakout:
            st.session_state['breakout_button'] = True
            st.session_state['retracement_button'] = False
            st.session_state["expansion_button"] = False
            st.session_state['range_button'] = False

    with col4:
        median_retracement = median_time_calcualtion(df["max_retracement_time"])
        if st.session_state["use_orb_body"]:
            median_retracement_value = df.retracement_level_body.median()
        else:
            median_retracement_value = df.retracement_level.median()

        st.metric("Median retracement before HoS/LoS:", value=str(median_retracement),
                  delta=f"Median retracement value: {median_retracement_value}",
                  )
        retracement = st.button("See Distribution", key="retracement")

        if retracement:
            st.session_state['retracement_button'] = True
            st.session_state["expansion_button"] = False
            st.session_state['breakout_button'] = False
            st.session_state['range_button'] = False

    with col5:

        median_expansion = median_time_calcualtion(df["max_expansion_time"])

        if st.session_state["use_orb_body"]:
            median_expansion_value = df.expansion_level_body.median()
        else:
            median_expansion_value = df.expansion_level.median()

        st.metric("Median time of max expansion:", value=str(median_expansion),
                  delta=f"Median expansion value: {median_expansion_value}",
                  )
        expansion = st.button("See distribution", key="expansion")

        if expansion:
            st.session_state["expansion_button"] = True
            st.session_state['retracement_button'] = False
            st.session_state['breakout_button'] = False
            st.session_state['range_button'] = False

    with range_dis:

        median_range = df["range_multiplier"].median()
        avg_range = df["range_multiplier"].mean().round(1)

        st.metric("Median Range Expansion", value=median_range,
                  delta=f"Average Range expansion: {avg_range}")
        range_distribution = st.button("See Distribution", key="range_expansion")

        if range_distribution:
            st.session_state['range_button'] = True
            st.session_state["expansion_button"] = False
            st.session_state['retracement_button'] = False
            st.session_state['breakout_button'] = False

    if st.session_state['breakout_button']:
        st.write("**Distribution of opening range breakout**")
        st.bar_chart(create_plot_df(df, "breakout_window"), y="pct", color=bar_color)
    elif st.session_state['retracement_button']:

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])

        if orb_side == "Short":
            if not st.session_state['use_orb_body']:
                df2 = create_plot_df(df, "retracement_level", inverse_percentile=False, ascending=True)
            else:
                df2 = create_plot_df(df, "retracement_level_body", inverse_percentile=False, ascending=True)
        else:
            if not st.session_state["use_orb_body"]:
                df2 = create_plot_df(df, "retracement_level", inverse_percentile=True)
            else:
                df2 = create_plot_df(df, "retracement_level_body", inverse_percentile=True)

        with tab_chart:

            if orb_side == "Short":
                fig = create_plotly_plot(df2, "Distribution of max retracement before low of the session",
                                         "Retracement Level", reversed_x_axis=False)
            else:
                fig = create_plotly_plot(df2, "Distribution of max retracement before high of the session",
                                         "Retracement Level", reversed_x_axis=True)
            st.plotly_chart(fig, use_container_width=True)

            st.caption(
                "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
            st.caption(
                "Level :red[0] is the low of the opening range and level :red[1] is the high of the opening range (wicks).")
            st.divider()

            ret_df = df.groupby("max_retracement_time").agg({"max_retracement_value": "count"}).reset_index()
            ret_df["max_retracement_time"] = ret_df["max_retracement_time"].astype(str)

            ret_df["percentile"] = ret_df["max_retracement_value"].cumsum() / ret_df["max_retracement_value"].sum()
            ret_df = ret_df.set_index("max_retracement_time")

            fig2 = create_plotly_plot(df=ret_df,
                                      title="Distribution of max retracement time before high/low of the session",
                                      x_title="Max Retracement Time",
                                      y1_name="Retracement Count",
                                      y1="max_retracement_value",
                                      y2="percentile",
                                      )
            st.plotly_chart(fig2, use_container_width=True)
        with tab_data:
            st.dataframe(df2)

        st.divider()

        overtake_percentile = st.toggle("Show Percentile", value=True)
        st.write("**Retracement/Extention Time Overtake**")

        if overtake_percentile:
            #### Optional ? Overtake approach?
            df_ret["pct retracement"] = 1 - (df_ret["pct retracement"].cumsum())
            df_ret["pct expansion"] = df_ret["pct expansion"].cumsum()

        st.line_chart(df_ret[["pct retracement", "pct expansion"]], color=[bar_color, line_color])

    elif st.session_state["expansion_button"]:

        if orb_side == "Short":
            if not st.session_state['use_orb_body']:
                df2 = create_plot_df(df, "expansion_level", inverse_percentile=True, ascending=False)
            else:
                df2 = create_plot_df(df, "expansion_level_body", inverse_percentile=True, ascending=False)
        else:
            if not st.session_state['use_orb_body']:
                df2 = create_plot_df(df, "expansion_level", inverse_percentile=False)
            else:
                df2 = create_plot_df(df, "expansion_level_body", inverse_percentile=False)

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])

        with tab_chart:
            if orb_side == "Short":
                fig = create_plotly_plot(df2, "Distribution of max expansion before high/low of the session", "Expansion Level", reversed_x_axis=True)
            else:
                fig = create_plotly_plot(df2, "Distribution of max expansion before high/low of the session", "Expansion Level", reversed_x_axis=False)
            st.plotly_chart(fig, use_container_width=True)

            st.caption(
                "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
            st.caption(
                "Level :red[0] is the low of the opening range and level :red[1] is the high of the opening range (wicks).")
            st.divider()

            exp_df = df.groupby("max_expansion_time").agg({"max_expansion_value": "count"}).reset_index()
            exp_df["max_expansion_time"] = exp_df["max_expansion_time"].astype(str)
            exp_df = exp_df.set_index("max_expansion_time")
            exp_df["percentile"] = exp_df["max_expansion_value"].cumsum() / exp_df["max_expansion_value"].sum()

            fig3 = create_plotly_plot(df=exp_df,
                                      title="Distribution of max expansion time before high/low of the session",
                                      x_title="Max Expansion Time",
                                      y1_name="Expansion Count",
                                      y1="max_expansion_value",
                                      y2="percentile",
                                      )

            st.plotly_chart(fig3, use_container_width=True)
            st.write(f"Median max expansion time is: {median_time_calcualtion(df.max_expansion_time)}")

            st.divider()
            st.write("**Extention/Retracement Time Overtake**")

            st.line_chart(df_ret[["pct retracement", "pct expansion"]], color=[bar_color, line_color])


        with tab_data:
            st.dataframe(df2)
    elif st.session_state['range_button']:

        range_group = df.groupby("range_multiplier").agg({"range_size": "count"})
        range_group = range_group.rename(columns={"range_size": "count"})
        range_group["pct"] = range_group["count"] / range_group["count"].sum()
        range_group["percentile"] = range_group["pct"].cumsum()
        range_group = range_group[range_group.index <=5]
       # st.bar_chart(range_group, x="range_multiplier", y="pct")

        fig4 = create_plotly_plot(df=range_group,
                                      title="Range expansion vs. previous Session",
                                      x_title="Range Multiplier",
                                      y1_name="count",
                                      y1="count",
                                      y2="percentile",
                                      )
        st.plotly_chart(fig4, use_container_width=True)
        st.caption(
            "This Chart shows the distribution of the range size expressed by its multiplier compared to the previous session.")

with model:

    model_explain = st.expander("See explanation of the Models", expanded=False)
    with model_explain:
        os.path.join("data", )
        st.subheader("Uptrend Models")
        st.image(os.path.join("pictures", "uptrend.png"))
        st.subheader("Downrend Models")
        st.image(os.path.join("pictures", "downtrend.png"))
        st.subheader("Other Models")
        st.image(os.path.join("pictures", "others.png"))
    order = ["Weak Uptrend", "Medium Uptrend", "Strong Uptrend", "Expansion", "Contraction", "Weak Downtrend",
             "Medium Downtrend", "Strong Downtrend", ]

    if (len(model_filter)-1 is not len(order)) or \
            (orb_side != "All") or (greenbox != "All"):
        st.error("This section is used to determine the probability of the current session model. "
                   "This means that this section is only useful before or during the opening range period. "
                   "Therefore, please do not set a breakout side, greenbox or model filter. "
                   "Otherwise, these filters will reduce the amount of data from which the models can be formed and may lead to incorrect results. ")
    else:

        model_df = df[["model", "model_prev", "upday_prev", "range_holds_prev", "upday"]].dropna()

        order = ["Weak Uptrend", "Medium Uptrend", "Strong Uptrend", "Expansion", "Contraction", "Weak Downtrend",
                 "Medium Downtrend", "Strong Downtrend", ]
        model_df["model"] = pd.Categorical(model_df["model"], categories=order, ordered=True)
        model_df["model_prev"] = pd.Categorical(model_df["model_prev"], categories=order, ordered=True)

        mds_col, md_true_col, md_up_col = st.columns(3)
        with mds_col:
            prev_md = st.selectbox("Choose Previous Model", order)
        with md_true_col:
            is_md_up = st.selectbox("Was Previous Model a Long breakout?", [True, False])
        with md_up_col:
            is_md_true = st.selectbox("Has previous Session ORB rule hold true?", [True, False])

        scenario_sel = st.multiselect("Potential scenarios for current session", order, order,
                                      help="Deselect models that can not happen anymore")

        model_df = model_df[
            (model_df.model_prev == prev_md) &
            (model_df.upday_prev == is_md_up) &
            (model_df.range_holds_prev == is_md_true) &
            (model_df["model"].isin(scenario_sel))
            ]

        model_df = model_df.groupby("model").agg({"model_prev": "count", "upday": "sum"}).reset_index()
        model_df = model_df[model_df["model_prev"] != 0]
        model_df["pct"] = model_df["model_prev"] / model_df["model_prev"].sum()
        model_df["pct_upday"] = model_df["upday"] / model_df["model_prev"]
        model_sample = model_df["model_prev"].sum()

        st.divider()
        st.write("**Model Distribution based on previous Sessions Model**")
        st.write("")
        st.bar_chart(model_df, y="pct", x="model", color=bar_color)

        st.write("**Likelihood of up breakout for each model**")

        sankey_bool = st.toggle("Sankey Chart")
        if not sankey_bool:
            st.write("")
            st.write("")
            st.write("")
            st.bar_chart(model_df, y="pct_upday", x="model", color=bar_color)

        else:
            # Schritt 1: Knoten und ihre Indizes

            df_sankey = df[["model_prev", "model"]].dropna()
            df_sankey['model'] = df_sankey['model'] + "_target"
            all_labels = list(pd.concat([df_sankey['model_prev'], df_sankey['model']]).unique())  # order
            label_indices = {label: idx for idx, label in enumerate(all_labels)}

            # st.write(all_labels)
            # st.write(label_indices)

            # Schritt 2: Links erstellen (von source nach target)
            df_sankey['source'] = df_sankey['model_prev'].map(label_indices)
            df_sankey['target'] = df_sankey['model'].map(label_indices)
            df_sankey["model2"] = df_sankey['model'].replace("_target", "", regex=True)

            df_sankey = df_sankey[(df_sankey['model2'].isin(scenario_sel)) &
                                  (df_sankey['model_prev'] == prev_md)]

            # Schritt 3: Häufigkeiten der Verbindungen berechnen
            link_data = df_sankey.groupby(['source', 'target']).size().reset_index(name='value')
            # link_data["pct"] = link_data["value"] / link_data["value"].sum()


            color_dict = {
                "Weak Uptrend": "#70AD47",
                "Medium Uptrend": "#A9D08E",
                "Strong Uptrend": "#E2EFDA",
                "Contraction": "#FFD966",
                "Expansion": "#B4C6E7",
                "Weak Downtrend": "#FCE4D6",
                "Medium Downtrend": "#F8CBAD",
                "Strong Downtrend": "#C65911",
                "Weak Uptrend_target": "#70AD47",
                "Medium Uptrend_target": "#A9D08E",
                "Strong Uptrend_target": "#E2EFDA",
                "Contraction_target": "#FFD966",
                "Expansion_target": "#B4C6E7",
                "Weak Downtrend_target": "#FCE4D6",
                "Medium Downtrend_target": "#F8CBAD",
                "Strong Downtrend_target": "#C65911",

            }

            node_colors = [color_dict.get(label, "grey") for label in all_labels]
            link_colors = [node_colors[target] for target in link_data['target']]

            # Schritt 4: Sankey-Diagramm erstellen
            fig = go.Figure(go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=all_labels,
                    color=node_colors
                ),
                link=dict(
                    source=link_data['source'],
                    target=link_data['target'],
                    value=link_data['value'],
                    color=link_colors
                )
            ))

            st.plotly_chart(fig, use_container_width=True)
        st.write(f"Model prediction is based on a sample size of {model_sample}")

with strategy_tester:
    if orb_side == "All":
        st.error("Please select a breakout side! Long breakout assumes long trade and vice versa.")
    else:

        col_buy_in, col_sl, col_tp = st.columns(3)
        with col_buy_in:
            if orb_side == "Short":
                buy_in = round(st.number_input("What is your sell in level:", step=0.1, value=0.0), 2)
            else:
                buy_in = round(st.number_input("What is your buy in level:", step=0.1, value=1.0), 2)
        with col_sl:
            sl = round(st.number_input("What is your stop loss level:", step=0.1, value=0.5), 2)
        with col_tp:
            if orb_side == "Short":
                tp = st.number_input("What is your take profit level:", step=0.1, value=-0.5)
            else:
                tp = st.number_input("What is your take profit level:", step=0.1, value=1.5)

        if orb_side == "Long":
            # Filter dataframe
            # entries on retracement before hos or after high of the session
            strat_df = df[(df.retracement_level <= buy_in) |
                          ((df.retracement_level > buy_in) & (df.after_conf_min_level <= buy_in))]

            strat_df = strat_df[
                ["after_conf_max_level", "after_conf_min_level", "session_close_level", "retracement_level",
                 "expansion_level"]]
            # direct sl trades
            sl_df = strat_df[(strat_df.retracement_level <= sl) |
                             ((strat_df.retracement_level > sl) &
                              (strat_df.after_conf_min_level <= sl))
                             ]

            # tp trades
            tp_df = strat_df[(strat_df.expansion_level >= tp) & (strat_df.retracement_level > sl)]

            # delete sl from tp trades
            tp_df = tp_df.drop(sl_df.index, axis='index', errors="ignore")

            # delete sl and tp trades from overall df
            part_df = strat_df.drop(sl_df.index, axis='index')
            part_df = part_df.drop(tp_df.index, axis='index')

            # partial wins
            part_win_df = part_df[part_df.session_close_level >= buy_in]
            part_loss_df = part_df[part_df.session_close_level < buy_in]


        else:
            # Filter dataframe
            # entries on retracement before hos or after low of the session
            strat_df = df[(df.retracement_level >= buy_in) |
                          ((df.retracement_level < buy_in) & (df.after_conf_max_level > buy_in))]
            strat_df = strat_df[
                ["after_conf_max_level", "after_conf_min_level", "session_close_level", "retracement_level",
                 "expansion_level"]]

            # sl trades
            sl_df = strat_df[(strat_df.retracement_level >= sl) |
                             (strat_df.retracement_level < sl) &
                             (strat_df.after_conf_max_level >= sl)
                             ]
            # tp trades
            tp_df = strat_df[(strat_df.expansion_level <= tp) & (strat_df.retracement_level < sl)]

            # delete sl from tp trades
            tp_df = tp_df.drop(sl_df.index, axis='index', errors="ignore")

            # delete sl and tp trades from overall df
            part_df = strat_df.drop(sl_df.index, axis='index')
            part_df = part_df.drop(tp_df.index, axis='index')

            # partial wins
            part_win_df = part_df[part_df.session_close_level <= buy_in]
            part_loss_df = part_df[part_df.session_close_level > buy_in]

            # calc kpis
        trade_count = len(strat_df.index)
        sl_count = len(sl_df.index)
        tp_count = len(tp_df.index)
        part_loss_count = len(part_loss_df.index)
        part_win_count = len(part_win_df.index)

        win_rate = (part_win_count + tp_count) / trade_count
        target_tp = abs(buy_in - tp) / abs(buy_in - sl)

        trades, hit_tp, hit_sl, part_tp, part_sl = st.columns(5)

        with trades:
            st.metric("#Trades", trade_count)
        with hit_tp:
            st.metric("Take Profit Hits", tp_count)
        with hit_sl:
            st.metric("Stop Loss Hits", sl_count)
        with part_tp:
            st.metric("Partial Wins", part_win_count)
        with part_sl:
            st.metric("Partial Losses", part_loss_count)

        winrate, profit_factor, target_rr, avg_rr, real_r = st.columns(5)

        with winrate:
            st.metric("Winrate", f"{win_rate:.1%}")
        with profit_factor:
            win_r = (tp_count * target_tp) + (abs(part_win_df.session_close_level - buy_in).sum())
            loss_r = (sl_count + abs(part_loss_df.session_close_level - buy_in).sum())

            profit_fact = win_r / loss_r
            st.metric("Proft Factor:", f"{profit_fact: .2f}")

        with target_rr:
            st.metric("Target Risk Multiple", f"{target_tp:.2f}")
        with avg_rr:

            if orb_side == "Long":
                real_par_rr = (np.array(part_df.session_close_level) - buy_in) / abs(buy_in - sl)
            else:
                real_par_rr = (np.array(buy_in - part_df.session_close_level)) / abs(buy_in - sl)
            avg_risk_reward = ((tp_count * target_tp) + (sl_count * -1) + sum(real_par_rr)) / trade_count

            st.metric("Avg. Realized Risk Multiple", f"{avg_risk_reward: .2f}")
        with real_r:

            # Equity Curve
            sl_df["R"] = -1

            tp_df["R"] = target_tp
            part_df["R"] = real_par_rr

            eq_curve = pd.concat([sl_df, tp_df, part_df])
            eq_curve = eq_curve.sort_index(ascending=True)
            eq_curve["Risk Reward"] = eq_curve.R.cumsum()
            st.metric("Realized Risk Reward", f"{eq_curve.R.sum(): .2f}")

        st.divider()

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])
        with tab_chart:
            st.write("**Equity Curve**")
            st.line_chart(eq_curve, y="Risk Reward", use_container_width=True)

        with tab_data:
            eq_curve = eq_curve.drop("Risk Reward", axis=1)
            st.dataframe(eq_curve)
    st.caption(
        "Please note that the results generated by this backtesting tool may not perfectly reflect real-world trading outcomes. "
        "\nUnlike candle-to-candle backtesting methods, which analyze each individual candle's data, this tool utilizes vectorized "
        "operations based on specific levels to assess trade success."
        "\nWhile every effort has been made to accurately simulate trading conditions, there are inherent limitations "
        "in any backtesting approach. "
        "Factors such as slippage, market volatility, and liquidity conditions are not be fully accounted in this simplified model."
        "\nTherefore, it's important to interpret the results of this backtest with caution and consider them as a guide "
        "rather than a definitive prediction of actual trading performance. "
        )

with ml:
    st.write(
        "This section is still in the very early stages of testing and should never be used as a reference. It should rather be seen as a technical gimmick. ")
    st.divider()
    if greenbox == "All":
        st.error("Please select a greenbox status. It´s an important feature of the ML prediction.")
    open_level = st.selectbox("What is the price level of the opening price?", [i / 10 for i in range(11)])
    close_level = st.selectbox("What is the price level of the closing price?", [i / 10 for i in range(11)])
    # gbox = [1 if greenbox == "True" else 0]
    # st.write(gbox[0])
    pred_values = [[1 if greenbox == "True" else 0][0], open_level, close_level]

    model, scaler = load_ml_model(symbol)
    if model == 0:
        st.subheader(scaler)
    else:

        pred_values = scaler.transform([pred_values])

        y_predicted = model.predict(pred_values)
        st.divider()
        if y_predicted[0] == 0:
            st.subheader("The machine learning model predicts a :red[short] breakout for this session!")
        else:
            st.subheader("The machine learning model predicts a :red[long] breakout for this session!")

with strategy_rules:
    st.subheader("Understanding the Opening Range Strategy")
    st.write(
        f"The Opening Range strategy centers around the initial price movements that occur during the first hour of market open. This period, known as the \"opening range\", sets the tone for the trading session. "
        f"But why is the open of a trading session so important? The open often establishes the trend and sentiment for the day! More often than not, the open is near the high or low of the day. ")
    st.write("Here's a short breakdown of the strategy.")
    st.write("**1. Establishing the Opening Range:**")
    st.write(
        "Traders begin by defining the opening range, spanning the first hour of trading. This range is determined by identifying the high and low prices during this initial timeframe.")
    st.write("**2. Breakout Identifying:**")
    st.write(
        "Once the opening range is established, traders waits for a 5 minute close above the high of the range for long trades or below the low of the range for short trades. These breakout levels are called \"breakout\" as they serve as a directional bias for a possible position.")
    st.write("**3. Entry Techniques**")
    st.write(
        "There are different entry techniques. Usually traders enter the market once the price breaks above the high of the opening range (for long trades) or below the low of the opening range (for short trades). "
        "The aim of this side it to give you a deeper understanding of the historical price movements in terms of time and price levels. Therefore traders can also wait for a retracement into the orb range after price broke out on one side of the range and confirmed our directional bias. "
        "The distribution of retracement levels can provide information on where good entry levels have been in the past. "
        "The same applies to the stop price. If possible, this should be in an area where most retracements have already ended in the past. ")
    st.write("**4. Profit Target:**")
    st.write(
        "Just as with the entry technique, there are also different ways of determing profit targets. For instance they can be set based on factors such as support and resistance levels or Fibonacci extensions. "
        "This page aims to show you the distribution of expansion Fibonacci levels achieved in the past to make it easier to set targets.")

    st.divider()
    st.subheader("Additonal links for further information on this topic:")
    link1 = "https://www.warriortrading.com/opening-range-breakout/"
    st.write("Warrior Trading: [Opening Range Breakout Trading Strategy](%s)" % link1)
    link2 = "https://adamhgrimes.com/wild-things-open/"
    st.write("Adam H Grimes [Where the wild things are? No, where the open is.](%s)" % link2)
    link3 = "https://www.youtube.com/channel/UCNSlBUliRfjOxmGB0mZ9_Ag"
    st.write("TheMas7er [Youtube Channel](%s)" % link3)

with faq_tab:
    orb = st.expander("What does opening Range /iRange stand for?")
    orb.write(
        "Opening Range refers to the price range that the price covers within the first hour of trading after the stock exchange opens.")
    orb.write(
        "iRange stands for implied range and refers to the price range that the candle bodies covers within the first hour of trading after the stock exchange opens.")

    orb_breakout = st.expander("What is a range breakout (Long/Short)")
    orb_breakout.write(
        "A Range breakout refers to the closing of a 5-minute candle above or below the opening range high/low. A close above the opening range high is a long breakout and a close below the opening price range low level is a short breakout. ")

    orb_rule = st.expander("What is the opening range rule?")
    orb_rule.write(
        "The Rule states that it is very unlikely that the price will close below/above the other side of the opening range after it has confirmed one side. "
        "The historical percentages for this can be found in this dashboard.")

    orb_rule.write("No trading recommendation can be derived from this. Please read the disclaimer very carefully.")

    greenbox_rule = st.expander("What is a greenbox?")
    greenbox_rule.write(
        "The greenbox is defined by the opening price and the closing price of the opening hour. If the closing price is quoted above the opening price, then the opening range is a greenbox.")

    indicator = st.expander("Is there a good TradingView indicator?")
    indicator.write(
        "I personally like the TheMas7er scalp (US equity) 5min [promuckaj] indicator. It comes with a lot of features but there are plenty of other free indicators available")

    data_refresh = st.expander("How often is the data updated?")
    data_refresh.write(
        "At the moment the collection of data is a time intensive manual process. Therefore there is no regular interval. I will update the data once in while.")

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

st.divider()
start_date = df.index[0].strftime("%Y-%m-%d")
end_date = df.index[-1].strftime("%Y-%m-%d")
st.write(f"Statistics based on :red[{len(df)}] data points from :red[{start_date}] to :red[{end_date}]")

if len(df) < 100:
    st.error("The size of the selected samle is very small. "
             "The informative value may not be very significant. "
             "If possible, you should change the filter setting so that you select a larger amount of data.")


