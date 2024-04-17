import datetime
import sys
import time as t
import math
import pandas as pd
import numpy as np
import os
from pytz import timezone
from datetime import time


def create_dataset(folder):
    data = pd.DataFrame()
    # loop through all files in folder and merge into one file
    for file in os.scandir(folder):
        # print(file)
        file_df = pd.read_csv(file.path, sep=",", index_col=0)
        # file_df["source"] = file.name
        data = pd.concat([data, file_df])

    # remove duplicate timestamps and unnessecary colums
    data = data[["open", "high", "low", "close"]]

    data = data[~data.index.duplicated()]
    data = data.sort_index()

    data.index = pd.to_datetime(data.index, utc=True)
    data.index = data.index.tz_convert('America/New_York')

    return data


def format_file(file, symbol, header=None):
    df = pd.read_csv(file, index_col=0, header=header)

    df.index = pd.to_datetime(df.index + ' ' + df[1], format='%m/%d/%Y %H:%M')  # .dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    tz = timezone('America/New_York')
    df.index = df.index.tz_localize(tz)
    df.index = df.index.strftime('%Y-%m-%d%%T%H:%M:%S%:z').str.replace(" ", "T")
    df = df.drop(1, axis=1)

    df = df.rename(columns={2: "open", 3: "high", 4: "low", 5: "close", 6: "Volume"})
    first_date = pd.to_datetime(df.index[0]).date()
    last_date = pd.to_datetime(df.index[-1]).date()
    name = f"{symbol}_{first_date}_{last_date}.csv"
    df.to_csv(name)


def remove_inconsistent_data(df, session):
    # df = pd.read_csv(df, index_col=0, parse_dates=True)
    # df.index = pd.to_datetime(df.index, utc=True)
    df["date"] = df.index.date

    df = session_calculations(df, session, only_dr_state=True)

    df["duplicate"] = df[["open", "high", "low", "close"]].diff().eq(0).all(axis=1)

    days = df.groupby("date").agg({"duplicate": "sum"})
    days = days[days.duplicate > 2]

    delete_days = np.unique(days.index)
    print(f"deleted: {len(delete_days)} days due to duplicates in {session} session")

    df = df[~df.date.isin(delete_days)]
    df = df.drop(columns=['duplicate'], axis=1)

    # print(days)
    # days.to_csv("error_days.csv")
    # df.to_csv("errors.csv")

    # df.to_csv("errors.csv")

    # df["group"] = (df.shift() == df).all(axis=1).cumsum()
    # df["date"] = df.index.date
    # df.to_csv("errors.csv")
    # # Chck for all dates that have to identical rows following each other
    # #List with errors
    # day_w_error = np.unique(df[df.group !=0].index.date)
    # print(f"removed error days {len(day_w_error)}")
    # #filter out all error days
    # df = df[~df.date.isin(day_w_error)]
    #
    # df = df.drop(["date", "group", "dr_hour", "dr_session"], axis=1)

    return df


def session_calculations(df, session="dr", only_dr_state=False):
    if session == "dr":
        start_time = time(9, 30)
        end_time = time(10, 30)
        end_of_session = time(16, 00)

    elif session == "odr":
        start_time = time(3, 0)
        end_time = time(4, 00)
        end_of_session = time(8, 30)

    elif session == "adr":
        start_time = time(19, 30)
        end_time = time(20, 30)
        end_of_session = time(2, 30)

    # NYC AM Session

    df["dr_hour"] = df.index.to_series().apply(lambda x: start_time <= x.time() < end_time)
    df["dr_session"] = df.index.to_series().apply(lambda x: end_time <= x.time() < end_of_session)

    # Remove all data from df thats not during the session
    df = df[(df.dr_session == True) | (df.dr_hour == True)].copy()

    if only_dr_state:
        return df

    df["date"] = df.index.date
    df["time"] = df.index.time

    # Needed for correct iDR values
    df['body_high'] = df[['open', 'close']].max(axis=1)
    df['body_low'] = df[['open', 'close']].min(axis=1)

    return df


def run_symbols(symbol="all", session="dr"):
    def dr_retracement_calc(df, dr_table):

        for date, row in dr_table.iterrows():

            # dr_high = row["dr_high"]
            # dr_low = row["dr_low"]
            breakout_time = row["breakout_time"]
            dr_upday = row["dr_upday"]
            filter_df = df.query("date == @date and time > @breakout_time")

            if pd.isnull(breakout_time):
                # print(f"No breakout on {date}")
                continue
            try:
                idx_min = filter_df["low"].idxmin()
                idx_max = filter_df["high"].idxmax()
                min_time = idx_min.time()
                max_time = idx_max.time()
            except ValueError:
                # print(f"error on date: {date} breakout time: {breakout_time}, upday: {dr_upday}")
                filter_df = df.query("date == @date and time >= @breakout_time")
                idx_min = filter_df["low"].idxmin()
                idx_max = filter_df["high"].idxmax()
                min_time = idx_min.time()
                max_time = idx_max.time()

            # print(f"{date} minumum at {min_time} and max at: {max_time}")
            if dr_upday:
                filter_df = filter_df.query("time <= @max_time")
                idx_min = filter_df["low"].idxmin()
                idx_max = filter_df["high"].idxmax()

                max_retracement_value = filter_df["low"].min()
                max_retracement_time = idx_min.time()
                max_expansion_value = filter_df["high"].max()
                max_expansion_time = idx_max.time()


            else:
                filter_df = filter_df.query("time <= @min_time")

                idx_min = filter_df["low"].idxmin()
                idx_max = filter_df["high"].idxmax()

                max_retracement_value = filter_df["high"].max()
                max_retracement_time = idx_max.time()
                max_expansion_value = filter_df["low"].min()
                max_expansion_time = idx_min.time()

            dr_table.at[date, "max_expansion_time"] = max_expansion_time
            dr_table.at[date, "max_retracement_time"] = max_retracement_time
            dr_table.at[date, "max_expansion_value"] = max_expansion_value
            dr_table.at[date, "max_retracement_value"] = max_retracement_value

        dr_table["retrace_into_dr"] = dr_table.apply(
            lambda row: True if (row["dr_upday"] and row["max_retracement_value"] < row["dr_high"])
                                or (not row["dr_upday"] and row["max_retracement_value"] > row["dr_low"]) else False,
            axis=1)

        dr_table["retrace_into_idr"] = dr_table.apply(
            lambda row: True if (row["dr_upday"] and row["max_retracement_value"] < row["idr_high"])
                                or (not row["dr_upday"] and row["max_retracement_value"] > row["idr_low"]) else False,
            axis=1)

        dr_table["expansion_window"] = dr_table.apply(create_time_groups, column_name="max_expansion_time", axis=1)
        dr_table["retracement_window"] = dr_table.apply(create_time_groups, column_name="max_retracement_time", axis=1)
        # dr_table.to_csv("test.csv", sep=";")

        return dr_table

    def dr_basics(df):

        # create table with dr timeframes
        dr_table = df.groupby([df.date, df.dr_hour]).agg(
            dr_high=("high", "max"),
            dr_low=("low", "min"),
            idr_high=("body_high", "max"),
            idr_low=("body_low", "min"),
            dr_open=("open", "first"),
            dr_close=("close", "last"),
        ).reset_index()

        dr_table = dr_table[dr_table.dr_hour].drop("dr_hour", axis=1).set_index("date")
        dr_table["greenbox"] = dr_table.dr_open < dr_table.dr_close

        session_table = df.groupby([df.date, df.dr_session]).agg(
            session_high=("high", "max"),
            session_low=("low", "min"),
            session_body_high=("body_high", "max"),
            session_body_low=("body_low", "min"),
            session_close=("close", "last"),
        ).reset_index()

        # Index des maximalen Werts in der Spalte "high" pro Gruppe
        max_high_indices = df[df.dr_session == True].groupby([df.date, df.dr_session])['high'].idxmax().reset_index()
        max_low_indices = df[df.dr_session == True].groupby([df.date, df.dr_session])['low'].idxmax().reset_index()
        max_high_indices.to_csv("max_high.csv")
        session_table = session_table[session_table.dr_session].drop("dr_session", axis=1).set_index("date")
        max_high_indices = max_high_indices[max_high_indices.dr_session].drop("dr_session", axis=1).set_index("date")
        max_low_indices = max_low_indices[max_low_indices.dr_session].drop("dr_session", axis=1).set_index("date")

        session_table.to_csv("session_table.csv")


        # Hinzufügen der Spalte 'session_high_index' zum session_table DataFrame
        session_table['session_high_time'] = max_high_indices['high'].dt.time
        session_table['session_low_time'] = max_low_indices['low'].dt.time

        dr_table = dr_table.join(session_table)

        dr_table["breached_dr_high"] = dr_table.dr_high < dr_table.session_high
        dr_table["breached_dr_low"] = dr_table.dr_low > dr_table.session_low
        dr_table["closed_above_dr_high"] = dr_table.dr_high < dr_table.session_body_high
        dr_table["closed_below_dr_high"] = dr_table.dr_low > dr_table.session_body_low
        dr_table["dr_confirmed"] = dr_table['closed_above_dr_high'] | dr_table['closed_below_dr_high']

        dr_table["dr_true_close"] = dr_table["closed_above_dr_high"] ^ dr_table["closed_below_dr_high"]
        dr_table["dr_true_wick"] = dr_table["breached_dr_low"] ^ dr_table["breached_dr_high"]

        # Remove days where dr high == dr low
        dr_table = dr_table[dr_table.dr_high != dr_table.dr_low]

        return dr_table

    def calc_fib_levels(dr_table):

        def calc_fib_level(fib_high, fib_low, value, step=0.1):

            fib_level = (value - fib_low) / (fib_high - fib_low)
            # print(f"fib h: {fib_high}, low: {fib_low}, expan: {value}")
            if fib_level >= 0:
                fib_level = step * math.floor(fib_level / step)
            else:
                fib_level = step * math.ceil(fib_level / step)

            return fib_level

        for date, row in dr_table.iterrows():
            fib_low = row["dr_low"]
            fib_high = row["dr_high"]
            expansion_value = row["max_expansion_value"]
            retracement_value = row["max_retracement_value"]
            open_value = row["dr_open"]
            close_value = row["dr_close"]
            session_high_value = row["session_high"]
            session_low_value = row["session_low"]
            session_close_value = row["session_close"]
            pre_conf_min = row["pre_conf_min"]
            pre_conf_max = row["pre_conf_max"]

            if pd.isna(session_high_value):
                print(date)

            fib_level_open = calc_fib_level(fib_high, fib_low, open_value, step=0.1)
            fib_level_close = calc_fib_level(fib_high, fib_low, close_value, step=0.1)
            fib_level_sess_high = calc_fib_level(fib_high, fib_low, session_high_value, step=0.1)
            fib_level_sess_low = calc_fib_level(fib_high, fib_low, session_low_value, step=0.1)
            fib_level_sess_close = calc_fib_level(fib_high, fib_low, session_close_value, step=0.1)
            fib_level_pre_min = calc_fib_level(fib_high, fib_low, pre_conf_min, step=0.1)
            fib_level_pre_max = calc_fib_level(fib_high, fib_low, pre_conf_max, step=0.1)
            dr_table.at[date, "opening_level"] = round(fib_level_open, 2)
            dr_table.at[date, "closing_level"] = round(fib_level_close, 2)
            dr_table.at[date, "session_high_level"] = round(fib_level_sess_high, 2)
            dr_table.at[date, "session_low_level"] = round(fib_level_sess_low, 2)
            dr_table.at[date, "session_close_level"] = round(fib_level_sess_close, 2)
            dr_table.at[date, "pre_conf_min_level"] = round(fib_level_pre_min, 2)
            dr_table.at[date, "pre_conf_max_level"] = round(fib_level_pre_max, 2)

            if pd.isnull(expansion_value):
                continue

            fib_level_expansion = calc_fib_level(fib_high, fib_low, expansion_value, step=0.1)
            fib_level_retracement = calc_fib_level(fib_high, fib_low, retracement_value, step=0.1)
            dr_table.at[date, "expansion_level"] = round(fib_level_expansion, 2)
            dr_table.at[date, "retracement_level"] = round(fib_level_retracement, 2)

        return dr_table

    def create_time_groups(row, column_name):

        if pd.isna(row[column_name]):
            return "No breakout"
        else:
            start_time = pd.Timestamp.combine(pd.Timestamp.now(), row[column_name]).floor('30min')
            end_time = start_time + pd.Timedelta(minutes=30)

        return f"{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}"

    def dr_confirmation_calc(df, dr_table):

        df = df[df.dr_session]
        upbreak_dates = dr_table[["dr_high"]].to_dict()
        downbreak_dates = dr_table[["dr_low"]].to_dict()
        # df.to_csv("test2.csv", sep=";")
        for date, value in upbreak_dates["dr_high"].items():

            # filter_df = df[(df.index.date == date) & (df.close > value)]
            query_string = f"date == @date and close > @value"
            # print(date)
            # print(query_string)
            # Filtern des DataFrame basierend auf der Query
            filter_df = df.query(query_string)
            # print(filter_df)
            try:
                up_break = filter_df.index[0]
            except IndexError:
                up_break = pd.NaT

            dr_table.at[date, "up_confirmation"] = up_break

        for date, value in downbreak_dates["dr_low"].items():
            # filter_df = df[(df.index.date == date) & (df.close < value)]
            query_string = f"date == @date and close < @value"
            # Filtern des DataFrame basierend auf der Query
            filter_df = df.query(query_string)
            try:
                down_break = filter_df.index[0]
            except IndexError:
                down_break = pd.NaT
            dr_table.at[date, "down_confirmation"] = down_break

        # print(dr_table)
        dr_table["breakout_time"] = dr_table[["up_confirmation", "down_confirmation"]].min(axis=1)
        dr_table["breakout_time"] = pd.to_datetime(dr_table["breakout_time"]).dt.time
        dr_table["breakout_window"] = dr_table.apply(create_time_groups, column_name="breakout_time", axis=1)

        dr_table["dr_upday"] = dr_table.apply(
            lambda row: False if (pd.isna(row['up_confirmation']) and pd.isna(row['down_confirmation']))
            else (True if pd.to_datetime(row['up_confirmation']) < pd.to_datetime(row['down_confirmation'])
                  else (True if pd.notna(row['up_confirmation']) and pd.isna(row['down_confirmation']) else False)),
            axis=1)

        dr_table["dr_true"] = pd.isna(dr_table["up_confirmation"]) | pd.isna(dr_table["down_confirmation"])
        dr_table["close_outside_dr"] = dr_table.apply(
            lambda row: (row['dr_upday'] and row['session_close'] > row['dr_high']) or (
                    not row['dr_upday'] and row['session_close'] < row['dr_low']), axis=1)

        return dr_table

    def pre_conf_calc(df, dr_table):
        df = df[df.dr_session]
        for date, row in dr_table.iterrows():
            breakout_time = row["breakout_time"]
            filter_df = df.query("date == @date and time < @breakout_time")

            min_price = filter_df["low"].min()
            max_price = filter_df["high"].max()
            if filter_df.size == 0:
                dr_table.at[date, "pre_conf_max"] = row["dr_close"]
                dr_table.at[date, "pre_conf_min"] = row["dr_close"]
            else:
                dr_table.at[date, "pre_conf_max"] = max_price
                dr_table.at[date, "pre_conf_min"] = min_price


        return dr_table

    def call_defs(symbol, url, session):
        start = t.time()

        df = create_dataset(url)

        df = remove_inconsistent_data(df, session)

        df = session_calculations(df, session=session)
        dr = dr_basics(df)
        dr = dr_confirmation_calc(df, dr)
        dr = pre_conf_calc(df, dr)
        dr = dr_retracement_calc(df, dr)
        dr = calc_fib_levels(dr)
        filename = os.path.join("dr_data", f"{symbol}_{session}.csv")
        dr.to_csv(filename, sep=";")

        print(f"{symbol} {session} took: {(t.time() - start) / 60} minutes")

    symbol_dict = {
        "nq": r"C:\Timon\Aktien\data\NQ\5Min",
        "es": r"C:\Timon\Aktien\data\ES\5Min",
        "ym": r"C:\Timon\Aktien\data\YM\5Min",
        "cl": r"C:\Timon\Aktien\data\CL\5Min",
        "gc": r"C:\Timon\Aktien\data\GC",
        "eurusd": r"C:\Timon\Aktien\data\EURUSD\5Min",
        "gbpusd": r"C:\Timon\Aktien\data\GBPUSD\5Min",
        "fdax": r"C:\Timon\Aktien\data\FDAX\5Min",
        "eu": r"C:\Timon\Aktien\data\EU",
        "bp": r"C:\Timon\Aktien\data\BP",
        "cd": r"C:\Timon\Aktien\data\CD",
        "jy": r"C:\Timon\Aktien\data\JY",

    }

    sessions = ["dr", "odr"]

    if symbol != "all":
        url = symbol_dict.get(symbol, False)
        if not url:
            print(f"No data for symbol {symbol} available")
        else:
            if session != "all":
                call_defs(symbol, url, session)
            else:
                for x in sessions:
                    call_defs(symbol, url, x)

    else:
        for i in symbol_dict.keys():
            url = symbol_dict.get(i, False)

            if session != "all":
                call_defs(i, url, session)
            else:
                for x in sessions:
                    print(f"creating symbol: {i} session: {x}")
                    call_defs(i, url, x)


run_symbols(symbol="cl", session="all")
