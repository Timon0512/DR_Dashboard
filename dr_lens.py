import sys
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
        #print(file)
        file_df = pd.read_csv(file.path, sep=",", index_col=0)
        # file_df["source"] = file.name
        data = pd.concat([data, file_df])

    # remove duplicate timestamps and unnessecary colums
    data = data[["open", "high", "low", "close", "Volume"]]
    data = data[data["Volume"].notna()]

    data = data[~data.index.duplicated()]
    data = data.sort_index()

    data.index = pd.to_datetime(data.index, utc=True)
    data.index = data.index.tz_convert('America/New_York')

    return data


def format_file(file, symbol, header=None):

    df = pd.read_csv(file, index_col=0, header=header)

    df.index = pd.to_datetime(df.index + ' ' + df[1], format='%m/%d/%Y %H:%M')#.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    tz = timezone('America/New_York')
    df.index = df.index.tz_localize(tz)
    df.index = df.index.strftime('%Y-%m-%d%%T%H:%M:%S%:z').str.replace(" ", "T")
    df = df.drop(1, axis=1)

    df = df.rename(columns={2: "open", 3: "high", 4: "low", 5: "close", 6: "Volume"})
    first_date = pd.to_datetime(df.index[0]).date()
    last_date = pd.to_datetime(df.index[-1]).date()
    name = f"{symbol}_{first_date}_{last_date}.csv"
    df.to_csv(name)


def run_symbols(symbol="all", session="dr"):

    def session_calculations(df, session="dr"):

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

        df["date"] = df.index.date
        df["time"] = df.index.time
        # df = df[df.index.weekday.isin([0, 1, 2, 3, 4])]

        # Needed for correct iDR values
        df['body_high'] = df[['open', 'close']].max(axis=1)
        df['body_low'] = df[['open', 'close']].min(axis=1)

        # df.to_csv("test.csv")

        return df

    def dr_retracement_calc(df, dr_table):

        df = df[df.dr_session]
        dr_table = dr_table[dr_table.session_close.notna()].copy()

        for date in dr_table.index:
            confirmation_time = dr_table.loc[date]["breakout_time"]
            filter_df = df[df.index.date == date]
            # print(f"{date} with confiramtion time: {confirmation_time} ")

            if (confirmation_time is pd.NaT) and (len(filter_df) == 0):
                # print(len(filter_df))
                continue

            elif confirmation_time is pd.NaT:

                high_of_day_idx = filter_df["high"].idxmax()
                low_of_day_idx = filter_df["low"].idxmin()

            else:
                filter_df = filter_df[filter_df.index.time > confirmation_time]

                high_of_day_idx = filter_df["high"].idxmax()
                low_of_day_idx = filter_df["low"].idxmin()

            if (not dr_table.loc[date]["dr_upday"]) and (dr_table.loc[date]["closed_below_dr_high"]):

                filter_df = filter_df[filter_df.index <= low_of_day_idx]

                max_retracement_value = max(filter_df["high"])
                max_expansion_value = min(filter_df["low"])
                max_retracement_idx = filter_df["high"].idxmax()
                max_expansion_idx = filter_df["low"].idxmin()

                dr_table.at[date, "max_expansion_time"] = max_expansion_idx.time()
                dr_table.at[date, "max_retracement_time"] = max_retracement_idx.time()
                dr_table.at[date, "max_expansion_value"] = max_expansion_value
                dr_table.at[date, "max_retracement_value"] = max_retracement_value

                # Non Confirmation Day is handled as Long day
            else:
                filter_df = filter_df[filter_df.index <= high_of_day_idx]

                max_retracement_value = min(filter_df["low"])
                max_expansion_value = max(filter_df["high"])
                max_retracement_idx = filter_df["low"].idxmin()
                max_expansion_idx = filter_df["high"].idxmax()

                dr_table.at[date, "max_expansion_time"] = max_expansion_idx.time()
                dr_table.at[date, "max_retracement_time"] = max_retracement_idx.time()
                dr_table.at[date, "max_expansion_value"] = max_expansion_value
                dr_table.at[date, "max_retracement_value"] = max_retracement_value

                dr_table["retrace_into_dr"] = dr_table.apply(
                    lambda row: True if (row["dr_upday"] and row["max_retracement_value"] < row["dr_high"]) or (
                                not row["dr_upday"] and row["max_retracement_value"] > row["dr_low"]) else False,
                    axis=1)
                dr_table["retrace_into_idr"] = dr_table.apply(
                    lambda row: True if (row["dr_upday"] and row["max_retracement_value"] < row["idr_high"]) or (
                                not row["dr_upday"] and row["max_retracement_value"] > row["idr_low"]) else False,
                    axis=1)

        dr_table["expansion_window"] = dr_table.apply(create_time_groups, column_name="max_expansion_time", axis=1)
        dr_table["retracement_window"] = dr_table.apply(create_time_groups, column_name="max_retracement_time", axis=1)

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

        session_table = session_table[session_table.dr_session].drop("dr_session", axis=1).set_index("date")

        dr_table = dr_table.join(session_table)

        dr_table["breached_dr_high"] = dr_table.dr_high < dr_table.session_high
        dr_table["breached_dr_low"] = dr_table.dr_low > dr_table.session_low
        dr_table["closed_above_dr_high"] = dr_table.dr_high < dr_table.session_body_high
        dr_table["closed_below_dr_high"] = dr_table.dr_low > dr_table.session_body_low
        dr_table["dr_confirmed"] = dr_table['closed_above_dr_high'] | dr_table['closed_below_dr_high']

        dr_table["dr_true_close"] = dr_table["closed_above_dr_high"] ^ dr_table["closed_below_dr_high"]
        dr_table["dr_true_wick"] = dr_table["breached_dr_low"] ^ dr_table["breached_dr_high"]

        return dr_table

    def calc_fib_levels(dr_table):

        def calc_fib_level(fib_high, fib_low, value, step=0.25):

            fib_level = (value - fib_low) / (fib_high - fib_low)

            if fib_level >= 0:
                fib_level = step * math.floor(fib_level / step)
            else:
                fib_level = step * math.ceil(fib_level / step)

            return fib_level

        for date in dr_table.index:
            fib_low = dr_table.loc[date]["idr_low"]
            fib_high = dr_table.loc[date]["idr_high"]
            expansion_value = dr_table.loc[date]["max_expansion_value"]
            retracement_value = dr_table.loc[date]["max_retracement_value"]

            fib_level_expansion = calc_fib_level(fib_high, fib_low, expansion_value, step=0.1)
            fib_level_retracement = calc_fib_level(fib_high, fib_low, retracement_value, step=0.1)

            dr_table.at[date, "expansion_level"] = round(fib_level_expansion, 2)
            dr_table.at[date, "retracement_level"] = round(fib_level_retracement, 2)

        # dr_table.to_csv("test.csv")
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

        for date, value in upbreak_dates["dr_high"].items():
            print(date)
            filter_df = df[(df.index.date == date) & (df.close > value)]
            try:
                up_break = filter_df.index[0]
            except IndexError:
                up_break = pd.NaT

            dr_table.at[date, "up_confirmation"] = up_break

        for date, value in downbreak_dates["dr_low"].items():
            filter_df = df[(df.index.date == date) & (df.close < value)]
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

    def call_defs(symbol, url, session):

        df = create_dataset(url)
        df = session_calculations(df, session=session)
        dr = dr_basics(df)
        dr = dr_confirmation_calc(df, dr)
        dr = dr_retracement_calc(df, dr)
        dr = calc_fib_levels(dr)
        filename = os.path.join("dr_data", f"{symbol}_{session}.csv")
        dr.to_csv(filename, sep=";")

    symbol_dict = {
        "nq": r"C:\Users\timon\PycharmProjects\Backtesting\data\NQ\5Min",
        "es": r"C:\Users\timon\PycharmProjects\Backtesting\data\ES\5Min",
        "ym": r"C:\Users\timon\PycharmProjects\Backtesting\data\YM\5Min",
        "cl": r"C:\Users\timon\PycharmProjects\Backtesting\data\CL\5Min",
        "eurusd": r"C:\Users\timon\PycharmProjects\Backtesting\data\EURUSD\5Min",
        "gbpusd": r"C:\Users\timon\PycharmProjects\Backtesting\data\GBPUSD\5Min",
        "fdax": r"C:\Users\timon\PycharmProjects\Backtesting\data\FDAX\5Min",
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



run_symbols(symbol="es", session="dr")


#format_file(r"C:\Users\timon\PycharmProjects\Backtesting\data\ES.txt", symbol="ES")