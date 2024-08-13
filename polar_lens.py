import time as t
import polars as pl
import os
import math
from datetime import time, timedelta, datetime

symbols = ["nq", "es", "ym", "cl", "gc", "eurusd", "gbpusd", "fdax", "audjpy",]

class OpeningRange:
    def __init__(self, symbol, orb_duration=60, start_times=None):
        if start_times is None:
            start_times = [time(9, 30), time(3, 00), time(8, 30)]
        self.orb_duration = orb_duration
        self.symbol = symbol
        self.symbol_dict = {
            "nq": r"C:\Timon\Aktien\data\NQ\5Min",
            "es": r"C:\Timon\Aktien\data\ES\5Min",
            "ym": r"C:\Timon\Aktien\data\YM\5Min",
            "cl": r"C:\Timon\Aktien\data\CL\5Min",
            "gc": r"C:\Timon\Aktien\data\GC",
            "eurusd": r"C:\Timon\Aktien\data\EURUSD\5Min",
            "gbpusd": r"C:\Timon\Aktien\data\GBPUSD\5Min",
            "fdax": r"C:\Timon\Aktien\data\FDAX\5Min",
            "audjpy": r"C:\Timon\Aktien\data\AUDJPY\5min",
        }
        self.sessions = {
            "ny": {"start_time": start_times[0],
                   "end_time": (datetime.combine(datetime.today(), start_times[0]) + timedelta(minutes=orb_duration)).time(),
                   "end_of_session": time(16, 00)},
            "ldn": {"start_time": start_times[1],
                    "end_time": (datetime.combine(datetime.today(), start_times[1]) + timedelta(minutes=orb_duration)).time(),
                    "end_of_session": time(8, 30)},
            "asia": {"start_time": start_times[2],
                     "end_time": (datetime.combine(datetime.today(), start_times[2]) + timedelta(minutes=orb_duration)).time(),
                     "end_of_session": time(14, 30)}
        }
        self.data = self.create_dataset()
        self.session_calculations()
        self.dr_calculations()
        self.fib_level_calculations()

    def create_dataset(self):
        data = pl.DataFrame()
        # loop through all files in folder and merge into one file
        for file in os.scandir(self.symbol_dict.get(self.symbol)):
            file_df = pl.read_csv(file.path, separator=",",
                                  columns=[0, 1, 2, 3, 4])  # columns=["time", "open", "high", "low", "close"]

            if file_df["time"].dtype == pl.Int64:
                file_df = file_df.with_columns(
                    file_df.select(pl.from_epoch(pl.col("time"), time_unit="s").dt.convert_time_zone("America/New_York")))
            else:
                # Konvertiere die Spalte 'time' in das datetime-Format
                file_df = file_df.with_columns(
                    pl.col("time").str.strptime(pl.Datetime)
                        .dt.convert_time_zone("America/New_York")
                        .alias("time")
                )

            data = pl.concat([data, file_df])


        data = data.unique(subset="time")
        data = data.sort(by="time")
        return data

    def export_dataset(self, file_name=None, time_definition="datetime", time_unit="s"):
        time_definition = str(time_definition).lower()
        if file_name is None:
            file_name = f"{self.symbol}_full_dataset_{time_definition}.csv"
        if time_definition == "unix":
            df = self.data.with_columns(
                (pl.col("time").dt.convert_time_zone("UTC").dt.epoch(time_unit).cast(pl.Int64).alias(
                    "time")),
            )
            df.write_csv(file_name)

        else:
            self.data.write_csv(file_name)

    def get_single_orb_table(self, session):
        return self.sessions[session]["dr_table"]

    def export_all_orb_tables(self, unix=False):
        for session in self.sessions:
            df = self.sessions[session]["dr_table"]

            if unix:
                df = df.with_columns(
                    (pl.col("up_confirmation").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("up_confirmation")),
                    (pl.col("down_confirmation").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("down_confirmation")),
                    (pl.col("breakout_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("breakout_time")),
                   # (pl.col("breakout_time").cast(pl.Int64).alias("breakout_time")),
                    (pl.col("max_retracement_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("max_retracement_time")),
                    (pl.col("max_expansion_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("max_expansion_time")),
                )
            filename = os.path.join("dr_data", f"{self.symbol}_{session}_{self.orb_duration}.csv")
            df.write_csv(filename, separator=";")

    def session_calculations(self):

        for session in self.sessions:

            df = self.data

            if session == "asia":
                # Convert to Asia time zone if we have asia session
                df = self.data.with_columns(
                    pl.col("time").dt.convert_time_zone("Asia/Tokyo")
                        .alias("time")
                )

            df = df.with_columns(
                ((pl.col("time").dt.time() >= self.sessions[session]["start_time"]) &
                 (pl.col("time").dt.time() < self.sessions[session]["end_time"])).alias("dr_hour"),

                ((pl.col("time").dt.time() >= self.sessions[session]["end_time"]) &
                 (pl.col("time").dt.time() < self.sessions[session]["end_of_session"])).alias("dr_session"),

                (pl.max_horizontal(["open", "close"])).alias("body_high"),
                (pl.min_horizontal(["open", "close"])).alias("body_low"),

                (pl.col("time").dt.date().alias("date"))
            )
            df = df.filter(
                (pl.col("dr_session")) | (pl.col("dr_hour"))
            )
            self.sessions[session]["5_min_session"] = df

    def dr_calculations(self):

        for session in self.sessions:
            # 5min session table
            df = self.sessions[session]["5_min_session"]

            # DR Table
            dr_df = df.filter(pl.col("dr_hour")).group_by(["date"]).agg([
                pl.col("high").max().alias("dr_high"),
                pl.col("low").min().alias("dr_low"),
                pl.col("body_high").max().alias("idr_high"),
                pl.col("body_low").min().alias("idr_low"),
                pl.col("open").first().alias("dr_open"),
                pl.col("close").last().alias("dr_close")
            ])

            dr_df = dr_df.with_columns([
                (pl.col("dr_open") < pl.col("dr_close")).alias("greenbox"),
                (pl.col("dr_high") - pl.col("dr_low")).round(6).alias("dr_range")
            ])

            # create session_table
            session_table = df.filter(pl.col("dr_session")).group_by(["date"]).agg([
                pl.col("high").max().alias("session_high"),
                pl.col("low").min().alias("session_low"),
                pl.col("body_high").max().alias("session_body_high"),
                pl.col("body_low").min().alias("session_body_low"),
                pl.col("close").last().alias("session_close")
            ])

            # Join dr table with sesion table
            dr_df = dr_df.join(session_table, left_on="date", right_on="date")

            dr_df = dr_df.with_columns([
                (pl.col("dr_high") < pl.col("session_high")).alias("breached_dr_high"),
                (pl.col("dr_low") > pl.col("session_low")).alias("breached_dr_low"),
                (pl.col("dr_high") < pl.col("session_body_high")).alias("closed_above_dr_high"),
                (pl.col("dr_low") > pl.col("session_body_low")).alias("closed_below_dr_low"),
            ])
            dr_df = dr_df.with_columns([
                (pl.col("closed_above_dr_high") | pl.col("closed_below_dr_low")).alias("dr_confirmed"),
                (pl.col("closed_above_dr_high") ^ pl.col("closed_below_dr_low")).alias("dr_true_close"),
                (pl.col("breached_dr_low") ^ pl.col("breached_dr_high")).alias("dr_true_wick"),
            ])
            # Remove days where dr high == dr low
            dr_df = dr_df.filter(pl.col("dr_high") != pl.col("dr_low"))

            # Seems like statement is not needed
            # dr_df = dr_df.filter(pl.col("session_high").is_not_null())

            #self.sessions[session]["dr_table"] = dr_df

            ##########################################################
            ### DR CONFIRMATION CALCULATION
            #########################################################

            df = df.join(dr_df[["date", "dr_high", "dr_low"]], left_on="date", right_on="date")

            df = df.with_columns([
                (pl.col("close") > pl.col("dr_high")).alias("long_confirmation"),
                (pl.col("close") < pl.col("dr_low")).alias("short_confirmation"),
            ])

            long_df = df.filter(
                (pl.col("long_confirmation")) &
                (pl.col("dr_session"))
            ).group_by(["date"]).agg([
                pl.col("time").first().alias("up_confirmation"),
            ])

            short_df = df.filter(
                (pl.col("short_confirmation")) &
                (pl.col("dr_session"))
            ).group_by(["date"]).agg([
                pl.col("time").first().alias("down_confirmation"),
            ])

            dr_df = dr_df.join(long_df, left_on="date", right_on="date", how="left")
            dr_df = dr_df.join(short_df, left_on="date", right_on="date", how="left")

            dr_df = dr_df.with_columns(
                pl.min_horizontal(["up_confirmation", "down_confirmation"]).alias("breakout_time")
            )

            dr_df = dr_df.with_columns(
                (pl.col("breakout_time") - (pl.col("breakout_time").dt.minute() % 30) * pl.duration(minutes=1))
                    .dt.strftime("%H:%M")
                    .alias("breakout_window_start"),
                (pl.col("breakout_time") - (pl.col("breakout_time").dt.minute() % 30) * pl.duration(
                    minutes=1) + pl.duration(minutes=30))
                    .dt.strftime("%H:%M")
                    .alias("breakout_window_end")
            )

            dr_df = dr_df.with_columns(
                pl.concat_str([
                    pl.col("breakout_window_start"),
                    pl.lit(" - "),
                    pl.col("breakout_window_end")
                ]).alias("breakout_window")
            )

            dr_df = dr_df.drop(["breakout_window_start", "breakout_window_end"])

            # DR Upday Calculation
            dr_df = dr_df.with_columns(
                pl.when(pl.col("up_confirmation").is_null() & pl.col("down_confirmation").is_null())
                    .then(False)
                    .otherwise(
                    pl.when(pl.col("up_confirmation").is_not_null() & pl.col("down_confirmation").is_null())
                        .then(True)
                        .otherwise(
                        pl.when(pl.col("up_confirmation") < pl.col("down_confirmation"))
                            .then(True)
                            .otherwise(False)
                    )
                ).alias("dr_upday"))
            # DR True
            dr_df = dr_df.with_columns(
                (pl.col("up_confirmation").is_null() | pl.col("down_confirmation").is_null()).alias("dr_true")
            )
            # Close OUTSIDE DR
            dr_df = dr_df.with_columns(
                pl.when(pl.col("dr_upday") & (pl.col("session_close") > pl.col("dr_high")))
                    .then(True)
                    .when(~pl.col("dr_upday") & (pl.col("session_close") < pl.col("dr_low")))
                    .then(True)
                    .otherwise(False)
                    .alias("close_outside_dr")
            )

            ##########################################################
            ### PRE CONFIRMATION CALCULATION
            #########################################################

            df = df.join(dr_df[["date", "breakout_time"]], left_on="date", right_on="date")

            df = df.with_columns(
                (pl.col("time") < pl.col("breakout_time")).alias("before_breakout"),
                (pl.col("time") > pl.col("breakout_time")).alias("after_breakout")
            )

            group_df = df.filter((pl.col("before_breakout")) &
                                 (pl.col("dr_session"))) \
                .group_by(["date"]).agg([
                pl.col("low").min().alias("pre_conf_min"),
                pl.col("high").max().alias("pre_conf_max")
            ])

            dr_df = dr_df.join(group_df[["date", "pre_conf_min", "pre_conf_max"]], left_on="date", right_on="date",
                               how="left")



            #######################################
            ### AFTER CONFIRMATION CALCULATION ###
            #######################################

            group_df = df.filter((pl.col("after_breakout")) &
                                 (pl.col("dr_session"))) \
                .group_by(["date"]).agg([
                pl.col("low").min().alias("after_conf_min"),
                pl.col("high").max().alias("after_conf_max"),
            ])

            dr_df = dr_df.join(
                group_df[
                    ["date", "after_conf_min", "after_conf_max"]
                ],
                left_on="date", right_on="date", how="left")

            #######################################
            ###     RETRACEMENT CALCULATIONS    ###
            #######################################
            # Retracements do not consider if high or low happend first.

            #get min and max values and times after confirmation (no hirachie)
            df = df.join(group_df[["date", "after_conf_min", "after_conf_max"]],
                         left_on="date", right_on="date", how="left")

            df = df.with_columns([
                (pl.col("low") == pl.col("after_conf_min")).alias("min_price_bool"),
                (pl.col("high") == pl.col("after_conf_max")).alias("max_price_bool"),
            ])

            min_values = df.filter(pl.col("min_price_bool")).group_by("date").agg([
                pl.col("time").first().alias("min_price_time"),
                #pl.col("low").first().alias("min_price_value"),

            ])

            max_values = df.filter(pl.col("max_price_bool")).group_by("date").agg([
                pl.col("time").first().alias("max_price_time"),
                #pl.col("high").first().alias("max_price_value"),
            ])
            # join min max time to df as well as upday information?

            min_max_values = min_values.join(
                max_values,
                left_on="date", right_on="date", how="left"
            )

            df = df.join(min_max_values,
                         left_on="date", right_on="date", how="left")

            df = df.join(dr_df[["date", "dr_upday"]],
                         left_on="date", right_on="date", how="left")

            # Max Expansion and retracement in conf direction

            df = df.with_columns(
                (pl.when(pl.col("dr_upday"))
                 .then((pl.col("time") <= pl.col("max_price_time")).alias("pre_max_expansion"))
                 .otherwise((pl.col("time") <= pl.col("min_price_time")).alias("pre_max_expansion"))
                 )
            )

            # df.head(10000).write_csv(f"{session}_5m_test.csv", separator=";")
            # dr_df.write_csv(f"{session}_test.csv", separator=";")
            #group df by after conf and before max expansion

            group_long_df = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (pl.col("dr_upday")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("low").min().alias("max_retracement_value")),
                (pl.col("high").max().alias("max_expansion_value")),
            ])

            group_short_df = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (~pl.col("dr_upday")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("high").max().alias("max_retracement_value")),
                (pl.col("low").min().alias("max_expansion_value")),
            ])

            group_df = pl.concat([group_short_df, group_long_df])

            dr_df = dr_df.join(group_df,
                               left_on="date", right_on="date", how="left")


            #get min and max times after confirmation and before max
            df = df.join(group_df,
                         left_on="date", right_on="date", how="left")

            df = df.with_columns(
                pl.col("max_retracement_value").fill_null(0).alias("max_retracement_value"),
                pl.col("max_expansion_value").fill_null(0).alias("max_expansion_value"),
            )

            df = df.with_columns([
                (pl.when(pl.col("dr_upday"))
                    .then(pl.col("low") == pl.col("max_retracement_value"))
                    .otherwise(pl.col("high") == pl.col("max_retracement_value"))
                    )
                    .alias("max_retracement_time_bool"),

                (pl.when(pl.col("dr_upday"))
                 .then(pl.col("high") == pl.col("max_expansion_value"))
                 .otherwise(pl.col("low") == pl.col("max_expansion_value"))
                 )
                    .alias("max_expansion_time_bool"),
            ])

            # Retracement
            group_df_ret = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (pl.col("max_retracement_time_bool")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("time").first().alias("max_retracement_time")),
            ])
            # Expansion
            group_df_exp = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (pl.col("max_expansion_time_bool")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("time").first().alias("max_expansion_time")),
            ])

            dr_df = dr_df.join(group_df_ret,
                               left_on="date", right_on="date", how="left")
            dr_df = dr_df.join(group_df_exp,
                               left_on="date", right_on="date", how="left")

            #df.head(10000).write_csv(f"{session}_5m_test.csv", separator=";")
            # dr_df.write_csv(f"{session}_test.csv", separator=";")


            dr_df = dr_df.with_columns(
                pl.when(
                    (pl.col("dr_upday") & (pl.col("max_retracement_value") < pl.col("dr_high"))) |
                    (~pl.col("dr_upday") & (pl.col("max_retracement_value") > pl.col("dr_low")))
                )
                    .then(True)
                    .otherwise(False)
                    .alias("retrace_into_dr")
            )

            dr_df = dr_df.with_columns(
                (pl.col("max_expansion_time") - (pl.col("max_expansion_time").dt.minute() % 30) * pl.duration(minutes=1))
                    .dt.strftime("%H:%M")
                    .alias("expansion_window_start"),
                (pl.col("max_expansion_time") - (pl.col("max_expansion_time").dt.minute() % 30) * pl.duration(
                    minutes=1) + pl.duration(minutes=30))
                    .dt.strftime("%H:%M")
                    .alias("expansion_window_end"),
                (pl.col("max_retracement_time") - (pl.col("max_retracement_time").dt.minute() % 30) * pl.duration(
                    minutes=1))
                    .dt.strftime("%H:%M")
                    .alias("retracement_window_start"),
                (pl.col("max_retracement_time") - (pl.col("max_retracement_time").dt.minute() % 30) * pl.duration(
                    minutes=1) + pl.duration(minutes=30))
                    .dt.strftime("%H:%M")
                    .alias("retracement_window_end"),

            )

            dr_df = dr_df.with_columns(
                pl.concat_str([
                    pl.col("expansion_window_start"),
                    pl.lit(" - "),
                    pl.col("expansion_window_end")
                ]).alias("expansion_window"),

                pl.concat_str([
                    pl.col("retracement_window_start"),
                    pl.lit(" - "),
                    pl.col("retracement_window_end")
                ]).alias("retracement_window"),
            )

            dr_df = dr_df.drop(["retracement_window_start", "retracement_window_end", "expansion_window_start", "expansion_window_end"])


            self.sessions[session]["dr_table"] = dr_df

            #dr_df.write_csv(f"{session}_test.csv", separator=";")
            #df.write_csv(f"{session}_5m_test.csv", separator=";")
            # group_df.write_csv(f"{session}_group_df.csv", separator=";")

    def fib_level_calculations(self):

        for session in self.sessions:
            df = self.sessions[session]["dr_table"]

            # Expansion_level Calculation
            df = df.with_columns(
                pl.when(pl.col("dr_upday"))
                    .then(
                        (((pl.col("max_expansion_value") - pl.col("dr_low")) /
                        (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                            .floor() * 0.1).round(1))
                    .otherwise(
                        (((pl.col("max_expansion_value") - pl.col("dr_low")) /
                        (pl.col("dr_high") - pl.col("dr_low"))/0.1)
                            .ceil() * 0.1).round(1))
                    .alias("expansion_level"),

                # Retracement_level Calculation
                pl.when(pl.col("dr_upday"))
                    .then(
                        (((pl.col("max_retracement_value") - pl.col("dr_low")) /
                        (pl.col("dr_high") - pl.col("dr_low"))/0.1)
                            .ceil() * 0.1).round(1))
                    .otherwise(
                        (((pl.col("max_retracement_value") - pl.col("dr_low")) /
                        (pl.col("dr_high") - pl.col("dr_low"))/0.1)
                            .floor()*0.1).round(1))
                    .alias("retracement_level"),

                # After con min, max level calc
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("after_conf_max") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("after_conf_min") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("after_conf_max_level"),

                # After con min, max level calc
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("after_conf_min") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("after_conf_max") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("after_conf_min_level"),

                # DR Open Level
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("dr_open") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("dr_open") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("opening_level"),

                # DR Close Level
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("dr_close") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("dr_close") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("closing_level"),

                # Session Close Level
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("session_close") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_close") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_close_level"),

                # Session low Level
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("session_low") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_low") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_low_level"),

                # Session low Level
                pl.when(pl.col("dr_upday"))
                    .then(
                    (((pl.col("session_high") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_high") - pl.col("dr_low")) /
                      (pl.col("dr_high") - pl.col("dr_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_high_level"),
            )

            df = df.sort("date")
            self.sessions[session]["dr_table"] = df


#Calculate ALL Symbols
for symbol in symbols:
    start = t.time()
    ORB = OpeningRange(symbol, orb_duration=60)
    ORB.export_all_orb_tables(unix=True)

    print(f"{symbol} took: {round(t.time() - start, 2)} seconds")


# ORB = OpeningRange("fdax")
# ORB.export_dataset(time_definition="unix")