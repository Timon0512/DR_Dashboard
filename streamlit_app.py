import streamlit as st
from datetime import datetime, time
import pytz
import os
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import pickle


st.set_page_config(page_title="Opening Range Breakout Analytics", layout="wide")

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


def load_ml_model(symbol):
    # load model
    # try:
    filepath_ml_model = os.path.join("ml_models", f"{symbol.lower()}_{session_dict.get(session)}_simple_confirmation_bias_model.pickle")
    filepath_ml_scaler = os.path.join("ml_models", f"{symbol.lower()}_{session_dict.get(session)}_simple_confirmation_bias_scaler.pickle")

    try:
        loaded_model = pickle.load(open(filepath_ml_model, "rb"))
        loaded_scaler = pickle.load(open(filepath_ml_scaler, "rb"))

    except FileNotFoundError:
        return 0 ,f"No trained model for {symbol} available"

    return loaded_model, loaded_scaler


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

    session_dict = {"New York (9:30 - 16:00 EST)": "dr", "London (3:00 - 8:30 EST)": "odr"}
    session = st.radio("Choose your Session",
                        ["New York (9:30 - 16:00 EST)", "London (3:00 - 8:30 EST)"])
                    #    ["DR", "oDR"])

    file = os.path.join("dr_data", f"{symbol.lower()}_{session_dict.get(session)}.csv")

    df = load_data(file)

    st.divider()


breakout = True

st.header(f"Opening Range Breakout Analytics")
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
    dr_side = st.radio("Range confirmation side", ("All", "Long", "Short"))
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

general_tab, distribution_tab, strategy_tester, retracement_manager, strategy_rules, faq_tab, disclaimer, ml = \
    st.tabs(["General Statistics", "Distribution", "Stategy Backtester", "Retracement Manager", "Strategy Rules", "FAQ", "Disclaimer", "Machine Learning"])

with general_tab:

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        count_dr_confirmed = len(df[df['dr_confirmed']])
        confirmed_dr = count_dr_confirmed / data_points
        st.metric("Range is confirmed", f"{confirmed_dr:.1%}")

    with col2:
        count_dr_true = len(df[df['dr_true']])
        dr_true = count_dr_true / data_points
        st.metric("Opposite Range holds", f"{dr_true:.1%}",
                  help="No candle close below/above the opposite side of the confirmed range.")


    with col3:
        count_days_with_retracement = len(df[df['retrace_into_dr']])
        dr_retracement = count_days_with_retracement / data_points
        st.metric("Retracement days into Range", f"{dr_retracement:.1%}",
                  help="% of days with retracement into opening range before the high/low of the day happens")


    with col4:


        count_dr_winning = len(df[df.close_outside_dr])
        dr_winning_days = count_dr_winning / data_points
        st.metric("Price closes outside opening range", f"{dr_winning_days:.1%}",
                  help="In direction of opening range confirmation")



    col5, col6, col7, col8 = st.columns(4)

    with col5:
        count_dr_long = len(df[df['dr_upday']])
        dr_conf_long = count_dr_long / data_points
        if dr_side == "All":
            st.metric("Long confirmation days", f"{dr_conf_long:.1%}")
        elif dr_side == "Long":
            st.metric("Long confirmation days", f"{1:.0%}")
        else:
            st.metric("Long confirmation days", f"{0:.0%}")



    with col6:

        if dr_side == "Long":

            breach_count = len(df[df['breached_dr_low']])
            breach_pct = 1 - (breach_count / data_points)
            st.metric("Range low unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks below Range low")

        elif dr_side == "Short":
            breach_count = len(df[df['breached_dr_high']])
            breach_pct = 1 - (breach_count / data_points)
            st.metric("Range high unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks above Range high")

        else:
            st.empty()
    with col7:
        count_days_with_retracement_idr = len(df[df['retrace_into_idr']])
        idr_retracement = count_days_with_retracement_idr / data_points
        st.metric("Retracement days into iRange", f"{idr_retracement:.1%}",
                  help="% of days with retracement into the implied opening range (candle bodies) before the high/low of the day happens")

    with col8:
        st.empty()

with distribution_tab:

    col3, col4, col5, close_dis = st.columns(4)

    with col3:
        median_time = median_time_calcualtion(df["breakout_time"])
        # median_time = statistics.median(df2["breakout_time"])
        st.metric("Median confirmation time:", value=str(median_time),
                  delta=f"Mode breakout time: {df.breakout_time.mode()[0]}")
        breakout = st.button("See Distribution", key="breakout")


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


    if breakout:
        st.write("**Distribution of opening range confirmation**")
        st.bar_chart(create_plot_df(df, "breakout_window"), y="pct")

    elif retracement:

        tab_chart, tab_data = st.tabs(["📈 Chart", "🗃 Data"])
        if dr_side == "Short":
            df2 = create_plot_df(df, "retracement_level", inverse_percentile=False, ascending=True)
        else:
            df2 = create_plot_df(df, "retracement_level", inverse_percentile=True)


        with tab_chart:
            if dr_side == "Short":
                fig = create_plotly_plot(df2, "Distribution of max retracement before low of the session", "Retracement Level", reversed_x_axis=False)
            else:
                fig = create_plotly_plot(df2, "Distribution of max retracement before high of the session", "Retracement Level", reversed_x_axis=True)
            st.plotly_chart(fig, use_container_width=True)


            st.caption(
                    "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
            st.caption(
                    "Level :red[0] is the low of the opening range and level :red[1] is the high of the opening range (wicks).")
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
                fig = create_plotly_plot(df2, "Distribution of max expansion", "Expansion Level", reversed_x_axis=True)
            else:
                fig = create_plotly_plot(df2, "Distribution of max expansion", "Expansion Level", reversed_x_axis=False)
            st.plotly_chart(fig, use_container_width=True)

            st.caption(
                    "The :red[red] line is the cumulative sum of the individual probabilities. It shows how many retracements/expansions have already ended at the corresponding level in the past.")
            st.caption(
                    "Level :red[0] is the low of the opening range and level :red[1] is the high of the opening range (wicks).")
            st.divider()

            st.write("**Distribution of max expansion time**")
            st.bar_chart(df.groupby("max_expansion_time").agg({"max_expansion_value": "count"}), use_container_width=True)

            st.write(f"Median max expansion time is: {median_time_calcualtion(df.max_expansion_time)}")

        with tab_data:
            st.dataframe(df2)

with retracement_manager:
    ny_time = datetime.now(pytz.timezone('America/New_York'))
    col_1, col_2 = st.columns(2)


    col9, col10, col11 = st.columns(3)

    if dr_side == "All":
        st.error("Please select opening range confirmation side for useful results!")

    with col9:
        cur_rt_lvl = round(st.number_input("What is your current level of retracement?", value=0.5, step=0.1,
                                            help="Deselects datapoints that show a less strong retracenent"), 2)
        if dr_side == "Long":
            df_sub = df[df.after_conf_min_level <= cur_rt_lvl]
        else:
            df_sub = df[df.after_conf_max_level >= cur_rt_lvl]

    with col10:
        st.empty()

    with col11:
        st.empty()


    st.divider()

    sub_data_points = len(df_sub.index)

    col12, col13, col14 = st.columns(3)

    with col12:

        count_dr_true_sub = len(df_sub[df_sub['dr_true']].index)
        dr_true_sub = count_dr_true_sub / sub_data_points
        st.metric("Probability that opposite range holds", f"{dr_true_sub:.1%}",
                  help="price does not close above/below opposite range")

    with col13:

        if dr_side == "Long":

            breach_count = len(df_sub[df_sub['breached_dr_low']])
            breach_pct = 1 - (breach_count / sub_data_points)
            st.metric("Opening range low unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks below DR low")

        elif dr_side == "Short":
            breach_count = len(df_sub[df_sub['breached_dr_high']])
            breach_pct = 1 - (breach_count / sub_data_points)
            st.metric("opening range high unbreached", f"{breach_pct:.1%}",
                      help="% of days where price doesn´t wicks above opening range high")

        else:
            st.empty()


    with col14:
        count_close_outside_dr = len(df_sub[df_sub.close_outside_dr].index)
        dr_winning_days_sub = count_close_outside_dr / sub_data_points
        st.metric("Price closes outside opening range", f"{dr_winning_days_sub:.1%}",
                      help="In direction of opening range confirmation")


    # Plotting Area
    col_5, col_6 = st.columns(2)


    with col_5:

        if dr_side == "Long":
            df2_sub = create_plot_df(df_sub, "session_low_level")
            fig = create_plotly_plot(df2_sub, "Distribution of Session lows (retracement)", "Session Low Level")
            st.plotly_chart(fig, use_container_width=True)
        else:
            df2_sub = create_plot_df(df_sub, "session_high_level", inverse_percentile=inv_param)
            fig = create_plotly_plot(df2_sub, "Distribution of Session highs (retracement)", "Session High Level")
            st.plotly_chart(fig, use_container_width=True)

    with col_6:

        if dr_side == "Long":

            df2_sub = create_plot_df(df_sub, "session_high_level", inverse_percentile=not inv_param)
            fig = create_plotly_plot(df2_sub, "Distribution of Session highs (expansion)", "Session High Level")
            st.plotly_chart(fig, use_container_width=True)
        else:
            df2_sub = create_plot_df(df_sub, "session_low_level", inverse_percentile=not inv_param)
            fig = create_plotly_plot(df2_sub, "Distribution of Session lows (expansion)", "Session Low Level")
            st.plotly_chart(fig, use_container_width=True)




    st.write(f"Subset of :red[{len(df_sub.index)}] datapoints are used for this scenario.")

with strategy_tester:
    if dr_side == "All":
        st.error("Please select a confirmation side! Long confirmation assumes long trade and vice versa.")
    else:

        col_buy_in, col_sl, col_tp = st.columns(3)
        with col_buy_in:
            if dr_side == "Short":
                buy_in = st.number_input("What is your sell in level:", step=0.1, value=0.0)
            else:
                buy_in = st.number_input("What is your buy in level:", step=0.1, value=1.0)
        with col_sl:
            sl = st.number_input("What is your stop loss level:", step=0.1, value=0.5)
        with col_tp:
            if dr_side == "Short":
                tp = st.number_input("What is your take profit level:", step=0.1, value=-0.5)
            else:
                tp = st.number_input("What is your take profit level:", step=0.1, value=1.5)

        if dr_side == "Long":
            # Filter dataframe
            # entries on retracement before hos or after high of the session
            strat_df = df[(df.retracement_level <= buy_in) |
                          ((df.retracement_level > buy_in) & (df.after_conf_min_level <= buy_in))]

            strat_df = strat_df[["after_conf_max_level", "after_conf_min_level", "session_close_level", "retracement_level", "expansion_level"]]
            #direct sl trades
            sl_df = strat_df[(strat_df.retracement_level <= sl) |
                             ((strat_df.retracement_level > sl) &
                             (strat_df.after_conf_min_level <= sl))
                             ]

            # tp trades
            tp_df = strat_df[(strat_df.expansion_level >= tp) & (strat_df.retracement_level > sl)]

            #delete sl from tp trades
            tp_df = tp_df.drop(sl_df.index, axis='index', errors="ignore")

            # delete sl and tp trades from overall df
            part_df = strat_df.drop(sl_df.index, axis='index')
            part_df = part_df.drop(tp_df.index, axis='index')

            #partial wins
            part_win_df = part_df[part_df.session_close_level >= buy_in]
            part_loss_df = part_df[part_df.session_close_level < buy_in]


        else:
            # Filter dataframe
            # entries on retracement before hos or after low of the session
            strat_df = df[(df.retracement_level >= buy_in) |
                          ((df.retracement_level < buy_in) & (df.after_conf_max_level > buy_in))]
            strat_df = strat_df[["after_conf_max_level", "after_conf_min_level", "session_close_level", "retracement_level",
                                 "expansion_level"]]

            # sl trades
            sl_df = strat_df[(strat_df.retracement_level >= sl) |
                             (strat_df.retracement_level < sl) &
                             (strat_df.after_conf_max_level >= sl)
                             ]
            # tp trades
            tp_df = strat_df[(strat_df.expansion_level <= tp) & (strat_df.retracement_level < sl)]

            #delete sl from tp trades
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

            if dr_side == "Long":
                real_par_rr = (np.array(part_df.session_close_level) - buy_in) / abs(buy_in - sl)
            else:
                real_par_rr = (np.array(buy_in - part_df.session_close_level)) / abs(buy_in - sl)
            avg_risk_reward = ((tp_count * target_tp) + (sl_count * -1) + sum(real_par_rr)) / trade_count

            st.metric("Avg. Realized Risk Multiple", f"{avg_risk_reward: .2f}")
        with real_r:

            #Equity Curve
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

with ml:
    st.write("This section is still in the very early stages of testing and should never be used as a reference. It should rather be seen as a technical gimmick. ")
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
            st.subheader("The machine learning model predicts a :red[short] confirmation for this session!")
        else:
            st.subheader("The machine learning model predicts a :red[long] confirmation for this session!")

with strategy_rules:
    st.subheader("Understanding the Opening Range Strategy")
    st.write(f"The Opening Range strategy centers around the initial price movements that occur during the first hour of market open. This period, known as the \"opening range\", sets the tone for the trading session. "
             f"But why is the open of a trading session so important? The open often establishes the trend and sentiment for the day! More often than not, the open is near the high or low of the day. ")
    st.write("Here's a short breakdown of the strategy.")
    st.write("**1. Establishing the Opening Range:**")
    st.write("Traders begin by defining the opening range, spanning the first hour of trading. This range is determined by identifying the high and low prices during this initial timeframe.")
    st.write("**2. Breakout Identifying:**")
    st.write("Once the opening range is established, traders waits for a 5 minute close above the high of the range for long trades or below the low of the range for short trades. These breakout levels are called \"confirmation\" as they serve as a directional bias for a possible position.")
    st.write("**3. Entry Techniques**")
    st.write("There are different entry techniques. Usually traders enter the market once the price breaks above the high of the opening range (for long trades) or below the low of the opening range (for short trades). "
             "The aim of this side it to give you a deeper understanding of the historical price movements in terms of time and price levels. Therefore traders can also wait for a retracement into the dr range after price broke out on one side of the range and confirmed our directional bias. "
             "The distribution of retracement levels can provide information on where good entry levels have been in the past. "
             "The same applies to the stop price. If possible, this should be in an area where most retracements have already ended in the past. ")
    st.write("**4. Profit Target:**")
    st.write("Just as with the entry technique, there are also different ways of determing profit targets. For instance they can be set based on factors such as support and resistance levels or Fibonacci extensions. "
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

    dr = st.expander("What does opening Range /iRange stand for?")
    dr.write("Opening Range refers to the price range that the price covers within the first hour of trading after the stock exchange opens.")
    dr.write("iRange stands for implied range and refers to the price range that the candle bodies covers within the first hour of trading after the stock exchange opens.")

    dr_confirmation = st.expander("What is a range confirmation (Long/Short)")
    dr_confirmation.write("A Range confirmation refers to the closing of a 5-minute candle above or below the opening range high/low. A close above the opening range high is a long confirmation and a close below the opening price range low level is a short confirmation. ")

    dr_rule = st.expander("What is the opening range rule?")
    dr_rule.write("The Rule states that it is very unlikely that the price will close below/above the other side of the opening range after it has confirmed one side. "
                  "The historical percentages for this can be found in this dashboard.")

    dr_rule.write("No trading recommendation can be derived from this. Please read the disclaimer very carefully.")

    greenbox_rule = st.expander("What is a greenbox?")
    greenbox_rule.write("The greenbox is defined by the opening price and the closing price of the opening hour. If the closing price is quoted above the opening price, then the opening range is a greenbox.")

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

