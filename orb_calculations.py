import time as t
import polars as pl
import os
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
                   "end_of_session": time(16, 00),
                   "open prices": pl.DataFrame,
                   "5_min_session": pl.DataFrame,
                   "model_df": pl.DataFrame,
                   "prev_session": "ldn"},
            "ldn": {"start_time": start_times[1],
                    "end_time": (datetime.combine(datetime.today(), start_times[1]) + timedelta(minutes=orb_duration)).time(),
                    "end_of_session": time(8, 30),
                    "open_prices": pl.DataFrame,
                    "5_min_session": pl.DataFrame,
                    "model_df": pl.DataFrame,
                    "prev_session": "asia"},
            "asia": {"start_time": start_times[2],
                     "end_time": (datetime.combine(datetime.today(), start_times[2]) + timedelta(minutes=orb_duration)).time(),
                     "end_of_session": time(14, 30),
                     "open_prices": pl.DataFrame,
                     "5_min_session": pl.DataFrame,
                     "model_df": pl.DataFrame,
                     "prev_session": "ny"},
        }
        self.data = self.create_dataset()
        self.session_calculations()
        self.orb_calculations()
        self.fib_level_calculations()
        self.model_builder()
        self.join_prev_models()

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
        return self.sessions[session]["orb_table"]

    def export_all_orb_tables(self, unix=False, file_format="csv"):
        for session in self.sessions:
            df = self.sessions[session]["orb_table"]

            if unix:
                df = df.with_columns(
                    (pl.col("up_confirmation").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("up_confirmation")),
                    (pl.col("down_confirmation").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("down_confirmation")),
                    (pl.col("breakout_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("breakout_time")),
                   # (pl.col("breakout_time").cast(pl.Int64).alias("breakout_time")),
                    (pl.col("max_retracement_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("max_retracement_time")),
                    (pl.col("max_expansion_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us").cast(pl.Int64).alias("max_expansion_time")),
                )

            filename = os.path.join("data", f"{self.symbol}_{session}_{self.orb_duration}.{file_format}")
            if file_format == "xlsx":
                df.write_excel(filename)
            elif file_format == "csv":
                df.write_csv(filename, separator=";")
            else:
                print("Not supported format. Please choose csv or xlsx")

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
                 (pl.col("time").dt.time() < self.sessions[session]["end_time"])).alias("opening_range"),

                ((pl.col("time").dt.time() >= self.sessions[session]["end_time"]) &
                 (pl.col("time").dt.time() < self.sessions[session]["end_of_session"])).alias("session"),

                (pl.max_horizontal(["open", "close"])).alias("body_high"),
                (pl.min_horizontal(["open", "close"])).alias("body_low"),

                (pl.col("time").dt.date().alias("date")),

                (pl.col("time")
                 .dt.convert_time_zone("UTC")  # Konvertiere in UTC
                 .dt.time()  # Extrahiere die Zeitkomponente
                 == time(0, 0))  # Vergleiche mit 00:00 Uhr
                    .alias("midnight_utc"),  # Alias für die neue Spalte,

                (pl.col("time")
                 .dt.convert_time_zone("America/New_York")  # Konvertiere in EST
                 .dt.time()  # Extrahiere die Zeitkomponente
                 == time(0, 0))  # Vergleiche mit 00:00 Uhr
                    .alias("midnight_ny"),  # Alias für die neue Spalte,

                (pl.col("time")
                 .dt.convert_time_zone("UTC")  # Konvertiere in UTC
                 )  # Vergleiche mit 00:00 Uhr
                    .alias("utc_datetime"),  # Alias für die neue Spalte,
            )

            #Open Price here
            open_price_ny = df.filter(pl.col("midnight_ny")).group_by("date").agg([
                pl.col("open").first().alias("ny_true_open"),
            ])

            open_price_day = df.filter(pl.col("midnight_utc")).group_by("utc_datetime").agg([
                    pl.col("open").last().alias("day_open"),
            ])

            open_price_day = open_price_day.with_columns(
                pl.col("utc_datetime").dt.date().alias("date")
            ).drop("utc_datetime")



            df = df.filter(
                (pl.col("session")) | (pl.col("opening_range"))
            )
            self.sessions[session]["5_min_session"] = df
            self.sessions[session]["open_prices"] = open_price_day.join(open_price_ny, left_on="date", right_on="date", how="left")

    def orb_calculations(self):

        for session in self.sessions:
            # 5min session table
            df = self.sessions[session]["5_min_session"]

            # ORB Table
            orb_df = df.filter(pl.col("opening_range")).group_by(["date"]).agg([
                pl.col("high").max().alias("range_high"),
                pl.col("low").min().alias("range_low"),
                pl.col("body_high").max().alias("range_high_body"),
                pl.col("body_low").min().alias("range_low_body"),
                pl.col("open").first().alias("range_open"),
                pl.col("close").last().alias("range_close")
            ])

            orb_df = orb_df.with_columns([
                (pl.col("range_open") < pl.col("range_close")).alias("greenbox"),
                (pl.col("range_high") - pl.col("range_low")).round(6).alias("range_size")
            ])

            # create session_table
            session_table = df.filter(pl.col("session")).group_by(["date"]).agg([
                pl.col("high").max().alias("session_high"),
                pl.col("low").min().alias("session_low"),
                pl.col("body_high").max().alias("session_body_high"),
                pl.col("body_low").min().alias("session_body_low"),
                pl.col("close").last().alias("session_close")
            ])

            # Join ORB table with sesion table
            orb_df = orb_df.join(session_table, left_on="date", right_on="date")

            orb_df = orb_df.with_columns([
                (pl.col("range_high") < pl.col("session_high")).alias("breached_range_high"),
                (pl.col("range_low") > pl.col("session_low")).alias("breached_range_low"),
                (pl.col("range_high") < pl.col("session_body_high")).alias("closed_above_range_high"),
                (pl.col("range_low") > pl.col("session_body_low")).alias("closed_below_range_low"),
            ])
            orb_df = orb_df.with_columns([
                (pl.col("closed_above_range_high") | pl.col("closed_below_range_low")).alias("range_confirmed"),
                (pl.col("closed_above_range_high") ^ pl.col("closed_below_range_low")).alias("range_holds_close"),
                (pl.col("breached_range_low") ^ pl.col("breached_range_high")).alias("range_holds_wick"),
            ])
            # Remove days where ORB high == ORB low
            orb_df = orb_df.filter(pl.col("range_high") != pl.col("range_low"))

            # Seems like statement is not needed
            # orb_df = orb_df.filter(pl.col("session_high").is_not_null())

            #self.sessions[session]["orb_table"] = orb_df

            ##########################################################
            ### ORB CONFIRMATION CALCULATION
            #########################################################

            df = df.join(orb_df[["date", "range_high", "range_low"]], left_on="date", right_on="date")

            df = df.with_columns([
                (pl.col("close") > pl.col("range_high")).alias("long_confirmation"),
                (pl.col("close") < pl.col("range_low")).alias("short_confirmation"),
            ])

            long_df = df.filter(
                (pl.col("long_confirmation")) &
                (pl.col("session"))
            ).group_by(["date"]).agg([
                pl.col("time").first().alias("up_confirmation"),
            ])

            short_df = df.filter(
                (pl.col("short_confirmation")) &
                (pl.col("session"))
            ).group_by(["date"]).agg([
                pl.col("time").first().alias("down_confirmation"),
            ])

            orb_df = orb_df.join(long_df, left_on="date", right_on="date", how="left")
            orb_df = orb_df.join(short_df, left_on="date", right_on="date", how="left")

            orb_df = orb_df.with_columns(
                pl.min_horizontal(["up_confirmation", "down_confirmation"]).alias("breakout_time")
            )


            orb_df = orb_df.with_columns(
                (pl.col("breakout_time") - (pl.col("breakout_time").dt.minute() % 30) * pl.duration(minutes=1))
                    .dt.strftime("%H:%M")
                    .alias("breakout_window_start"),
                (pl.col("breakout_time") - (pl.col("breakout_time").dt.minute() % 30) * pl.duration(
                    minutes=1) + pl.duration(minutes=30))
                    .dt.strftime("%H:%M")
                    .alias("breakout_window_end")
            )

            orb_df = orb_df.with_columns(
                pl.concat_str([
                    pl.col("breakout_window_start"),
                    pl.lit(" - "),
                    pl.col("breakout_window_end")
                ]).alias("breakout_window")
            )

            orb_df = orb_df.drop(["breakout_window_start", "breakout_window_end"])

            # ORB Upday Calculation
            orb_df = orb_df.with_columns(
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
                ).alias("upday"))
            # ORB True
            orb_df = orb_df.with_columns(
                (pl.col("up_confirmation").is_null() | pl.col("down_confirmation").is_null()).alias("range_holds")
            )
            # Close OUTSIDE ORB
            orb_df = orb_df.with_columns(
                pl.when(pl.col("upday") & (pl.col("session_close") > pl.col("range_high")))
                    .then(True)
                    .when(~pl.col("upday") & (pl.col("session_close") < pl.col("range_low")))
                    .then(True)
                    .otherwise(False)
                    .alias("close_outside_range")
            )

            ##########################################################
            ### PRE CONFIRMATION CALCULATION
            #########################################################

            df = df.join(orb_df[["date", "breakout_time"]], left_on="date", right_on="date")

            df = df.with_columns(
                (pl.col("time") < pl.col("breakout_time")).alias("before_breakout"),
                (pl.col("time") > pl.col("breakout_time")).alias("after_breakout")
            )

            group_df = df.filter((pl.col("before_breakout")) &
                                 (pl.col("session"))) \
                .group_by(["date"]).agg([
                pl.col("low").min().alias("pre_conf_min"),
                pl.col("high").max().alias("pre_conf_max")
            ])

            orb_df = orb_df.join(group_df[["date", "pre_conf_min", "pre_conf_max"]], left_on="date", right_on="date",
                               how="left")



            #######################################
            ### AFTER CONFIRMATION CALCULATION ###
            #######################################

            group_df = df.filter((pl.col("after_breakout")) &
                                 (pl.col("session"))) \
                .group_by(["date"]).agg([
                pl.col("low").min().alias("after_conf_min"),
                pl.col("high").max().alias("after_conf_max"),
            ])

            orb_df = orb_df.join(
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

            df = df.join(orb_df[["date", "upday"]],
                         left_on="date", right_on="date", how="left")

            # Max Expansion and retracement in conf direction

            df = df.with_columns(
                (pl.when(pl.col("upday"))
                 .then((pl.col("time") <= pl.col("max_price_time")).alias("pre_max_expansion"))
                 .otherwise((pl.col("time") <= pl.col("min_price_time")).alias("pre_max_expansion"))
                 )
            )

            # df.head(10000).write_csv(f"{session}_5m_test.csv", separator=";")
            # orb_df.write_csv(f"{session}_test.csv", separator=";")
            #group df by after conf and before max expansion

            group_long_df = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (pl.col("upday")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("low").min().alias("max_retracement_value")),
                (pl.col("high").max().alias("max_expansion_value")),
            ])

            group_short_df = df.filter(
                (pl.col("after_breakout")),
                (pl.col("pre_max_expansion")),
                (~pl.col("upday")),
            ).group_by(
                ["date"]
            ).agg([
                (pl.col("high").max().alias("max_retracement_value")),
                (pl.col("low").min().alias("max_expansion_value")),
            ])

            group_df = pl.concat([group_short_df, group_long_df])

            orb_df = orb_df.join(group_df,
                               left_on="date", right_on="date", how="left")


            #get min and max times after confirmation and before max
            df = df.join(group_df,
                         left_on="date", right_on="date", how="left")

            df = df.with_columns(
                pl.col("max_retracement_value").fill_null(0).alias("max_retracement_value"),
                pl.col("max_expansion_value").fill_null(0).alias("max_expansion_value"),
            )

            df = df.with_columns([
                (pl.when(pl.col("upday"))
                    .then(pl.col("low") == pl.col("max_retracement_value"))
                    .otherwise(pl.col("high") == pl.col("max_retracement_value"))
                    )
                    .alias("max_retracement_time_bool"),

                (pl.when(pl.col("upday"))
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

            orb_df = orb_df.join(group_df_ret,
                               left_on="date", right_on="date", how="left")
            orb_df = orb_df.join(group_df_exp,
                               left_on="date", right_on="date", how="left")

            #df.head(10000).write_csv(f"{session}_5m_test.csv", separator=";")
            # orb_df.write_csv(f"{session}_test.csv", separator=";")


            orb_df = orb_df.with_columns(
                pl.when(
                    (pl.col("upday") & (pl.col("max_retracement_value") < pl.col("range_high"))) |
                    (~pl.col("upday") & (pl.col("max_retracement_value") > pl.col("range_low")))
                )
                    .then(True)
                    .otherwise(False)
                    .alias("retrace_into_range")
            )

            orb_df = orb_df.with_columns(
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

            orb_df = orb_df.with_columns(
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

            orb_df = orb_df.drop(["retracement_window_start", "retracement_window_end", "expansion_window_start", "expansion_window_end"])

            orb_df = orb_df.join(self.sessions[session]["open_prices"], left_on="date", right_on="date", how="left")

            # Has NY True Open or Daily open liquidity been taken by the ORB Hour?
            orb_df = orb_df.with_columns([
                (((pl.col("range_open") > pl.col("day_open")) &
                 (pl.col("range_low") < pl.col("day_open")) &
                  (pl.col("range_close") > pl.col("day_open")))|
                 ((pl.col("range_open") < pl.col("day_open")) &
                  (pl.col("range_high") > pl.col("day_open")) &
                  (pl.col("range_close") < pl.col("day_open"))
                 )).alias("took_out_day_open"),
                (((pl.col("range_open") > pl.col("ny_true_open")) &
                  (pl.col("range_low") < pl.col("ny_true_open")) &
                  (pl.col("range_close") > pl.col("ny_true_open"))) |
                 ((pl.col("range_open") < pl.col("ny_true_open")) &
                  (pl.col("range_high") > pl.col("ny_true_open")) &
                  (pl.col("range_close") < pl.col("ny_true_open"))
                  )).alias("took_out_ny_true_open"),

            ])

            self.sessions[session]["orb_table"] = orb_df

    def fib_level_calculations(self):

        for session in self.sessions:
            df = self.sessions[session]["orb_table"]

            # Expansion_level Calculation
            df = df.with_columns(
                pl.when(pl.col("upday"))
                    .then(
                        (((pl.col("max_expansion_value") - pl.col("range_low")) /
                        (pl.col("range_high") - pl.col("range_low")) / 0.1)
                            .floor() * 0.1).round(1))
                    .otherwise(
                        (((pl.col("max_expansion_value") - pl.col("range_low")) /
                        (pl.col("range_high") - pl.col("range_low"))/0.1)
                            .ceil() * 0.1).round(1))
                    .alias("expansion_level"),

                # Retracement_level Calculation
                pl.when(pl.col("upday"))
                    .then(
                        (((pl.col("max_retracement_value") - pl.col("range_low")) /
                        (pl.col("range_high") - pl.col("range_low"))/0.1)
                            .ceil() * 0.1).round(1))
                    .otherwise(
                        (((pl.col("max_retracement_value") - pl.col("range_low")) /
                        (pl.col("range_high") - pl.col("range_low"))/0.1)
                            .floor()*0.1).round(1))
                    .alias("retracement_level"),

                # After con min, max level calc
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("after_conf_max") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("after_conf_min") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("after_conf_max_level"),

                # After con min, max level calc
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("after_conf_min") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("after_conf_max") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("after_conf_min_level"),

                # ORB Open Level
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("range_open") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("range_open") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("opening_level"),

                # ORB Close Level
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("range_close") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("range_close") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("closing_level"),

                # Session Close Level
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("session_close") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_close") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_close_level"),

                # Session low Level
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("session_low") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_low") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_low_level"),

                # Session low Level
                pl.when(pl.col("upday"))
                    .then(
                    (((pl.col("session_high") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .ceil() * 0.1).round(1))
                    .otherwise(
                    (((pl.col("session_high") - pl.col("range_low")) /
                      (pl.col("range_high") - pl.col("range_low")) / 0.1)
                     .floor() * 0.1).round(1))
                    .alias("session_high_level"),
            )

            df = df.sort("date")
            self.sessions[session]["orb_table"] = df

    def model_builder(self):

        for session in self.sessions:
            df = self.sessions[session]["orb_table"]
            prev_session = self.sessions[session]["prev_session"]
            df_prev = self.sessions[prev_session]["orb_table"]

            df_prev = df_prev.select(
                ["date", "range_high",	"range_low", "upday", "range_holds", "range_size"]
            )

            if session == "asia":
                df_prev = df_prev.with_columns(
                    pl.col("date").shift(-1)
                )

            mdl = df.join(df_prev, left_on="date", right_on="date", suffix="_prev")


            mdl = mdl.with_columns(
                (((pl.col("range_high") - pl.col("range_low"))/2) + pl.col("range_low")).alias("midline"),
                (((pl.col("range_high_prev") - pl.col("range_low_prev")) / 2) + pl.col("range_low_prev")).alias("midline_prev_session"),
            )

            mdl = mdl.with_columns(
                pl.when((pl.col("range_low") <= pl.col("midline_prev_session")) &
                     (pl.col("range_low") >= pl.col("range_low_prev")) &
                     (pl.col("range_high") > pl.col("range_high_prev")))
                    .then(pl.lit("Weak Uptrend"))
                    .when((pl.col("range_low") > pl.col("midline_prev_session")) &
                          (pl.col("range_low") <= pl.col("range_high_prev")) &
                          (pl.col("range_high") > pl.col("range_high_prev")))
                    .then(pl.lit("Medium Uptrend"))
                    .when(pl.col("range_low") > pl.col("range_high_prev"))
                    .then(pl.lit("Strong Uptrend"))
                    .when((pl.col("range_high") >= pl.col("midline_prev_session")) &
                          (pl.col("range_high") <= pl.col("range_high_prev")) &
                          (pl.col("range_low") < pl.col("range_low_prev")))
                    .then(pl.lit("Weak Downtrend"))
                    .when((pl.col("range_high") < pl.col("midline_prev_session")) &
                          (pl.col("range_high") >= pl.col("range_low_prev")) &
                          (pl.col("range_low") < pl.col("range_low_prev")))
                    .then(pl.lit("Medium Downtrend"))
                    .when(pl.col("range_high") < pl.col("range_low_prev"))
                    .then(pl.lit("Strong Downtrend"))
                    .when((pl.col("range_high") < pl.col("range_high_prev")) &
                          (pl.col("range_low") > pl.col("range_low_prev")))
                    .then(pl.lit("Contraction"))
                    .when((pl.col("range_high") > pl.col("range_high_prev")) &
                          (pl.col("range_low") < pl.col("range_low_prev")))
                    .then(pl.lit("Expansion"))
                    .otherwise(pl.lit("None"))
                    .alias("model")
            )


            df = df.join(mdl["date", "model",], left_on="date", right_on="date", suffix="_prev", how="left")
            self.sessions[session]["orb_table"] = df
            self.sessions[session]["model_df"] = df.select(["date", "model", "upday", "range_holds", "range_size"])

    def join_prev_models(self):
        #Needs to be a seperate def because all model_df need do be calculated first.
        for session in self.sessions:
            prev_session = self.sessions[session]["prev_session"]
            df = self.sessions[session]["orb_table"]
            prev = self.sessions[prev_session]["model_df"]

            df = df.join(prev, left_on="date", right_on="date", suffix="_prev", how="left")

            df = df.with_columns(
                (pl.col("range_size") / pl.col("range_size_prev")).round(1).alias("range_multiplier")
            )
            self.sessions[session]["orb_table"] = df

#Calculate ALL Symbols
for symbol in symbols:
   start = t.time()
   ORB = OpeningRange(symbol, orb_duration=60)
   ORB.export_all_orb_tables(unix=True)
   print(f"{symbol} took: {round(t.time() - start, 2)} seconds")


# ORB = OpeningRange("es")
# ORB.export_all_orb_tables(unix=True, file_format="csv")