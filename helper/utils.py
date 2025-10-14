"""Utility helpers for the Streamlit dashboard."""
from __future__ import annotations

import os
import pickle
from datetime import datetime, time
from typing import Iterable, Optional

import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots


def median_time_calcualtion(time_array: Iterable[Optional[time]]) -> Optional[time]:
    """Return the median value of an iterable containing time like entries."""

    def parse_to_time(value: Optional[time]) -> Optional[time]:
        if pd.isna(value):
            return None
        if isinstance(value, time):
            return value
        try:
            return datetime.strptime(value, "%H:%M:%S").time()
        except ValueError as exc:  # pragma: no cover - invalid user input
            raise ValueError(
                "Ungültiges Format. Erwartet wird ein String im Format 'Stunde:Minute:Sekunde'."
            ) from exc

    def time_to_seconds(time_obj: time) -> int:
        return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second

    def seconds_to_time(seconds: int) -> time:
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

    parsed_times = [parse_to_time(value) for value in time_array]
    valid_times = [time_obj for time_obj in parsed_times if not pd.isna(time_obj)]

    if not valid_times:
        return None

    seconds_list = [time_to_seconds(time_obj) for time_obj in valid_times]
    median_seconds = sorted(seconds_list)[len(seconds_list) // 2]
    return seconds_to_time(median_seconds)


def create_plot_df(
    df: pd.DataFrame,
    groupby_column: str,
    inverse_percentile: bool = False,
    ascending: bool = True,
) -> pd.DataFrame:
    """Create a dataframe summarising a distribution used across multiple charts."""
    plot_df = df.groupby(groupby_column).agg({"breakout_window": "count"})
    plot_df = plot_df.rename(columns={"breakout_window": "count"})
    plot_df["pct"] = plot_df["count"] / plot_df["count"].sum()
    plot_df["percentile"] = plot_df["pct"].cumsum()

    if inverse_percentile:
        plot_df["percentile"] = 1 - plot_df["percentile"]

    if not ascending:
        plot_df = plot_df.sort_index(ascending=False)

    return plot_df


def create_plotly_plot(
    df: pd.DataFrame,
    title: str,
    x_title: str,
    y1_name: str = "Pct",
    y2_name: str = "Overall likelihood",
    y1: str = "pct",
    y2: str = "percentile",
    bar_color: str = "#223459",
    line_color: str = "#FF4B4B",
    reversed_x_axis: bool = False,
):
    """Build a combined bar/line plot that shares the same structure across the app."""
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
    subfig.layout.yaxis2.range = [0, 1]

    if reversed_x_axis:
        subfig.update_layout(xaxis=dict(autorange="reversed"))

    return subfig


def create_join_table(
    first_symbol: str,
    second_symbol: str,
    session_name: str,
    data_dir: str = "data",
) -> pd.DataFrame:
    """Join two symbol csvs on the shared date index."""
    cols_to_use = [
        "date",
        "greenbox",
        "breakout_time",
        "upday",
        "max_retracement_time",
        "max_expansion_time",
        "retracement_level",
        "expansion_level",
        "closing_level",
    ]

    first_symbol = first_symbol.lower()
    second_symbol = second_symbol.lower()

    file1 = os.path.join(data_dir, f"{first_symbol}_{session_name}.csv")
    file2 = os.path.join(data_dir, f"{second_symbol}_{session_name}.csv")

    df1 = pd.read_csv(file1, sep=";", index_col=[0], usecols=cols_to_use)
    df2 = pd.read_csv(file2, sep=";", index_col=[0], usecols=cols_to_use)

    return df1.join(df2, lsuffix=f"_{first_symbol}", rsuffix=f"_{second_symbol}", how="left")


def load_ml_model(
    symbol: str,
    session_key: str,
    model_dir: str = "ml_models",
):
    """Load the persisted ML model and scaler for a symbol/session combination."""
    filepath_ml_model = os.path.join(
        model_dir,
        f"{symbol.lower()}_{session_key}_simple_confirmation_bias_model.pickle",
    )
    filepath_ml_scaler = os.path.join(
        model_dir,
        f"{symbol.lower()}_{session_key}_simple_confirmation_bias_scaler.pickle",
    )

    try:
        loaded_model = pickle.load(open(filepath_ml_model, "rb"))
        loaded_scaler = pickle.load(open(filepath_ml_scaler, "rb"))
    except FileNotFoundError:
        return 0, f"No trained model for {symbol} available"

    return loaded_model, loaded_scaler
