from typing import Any, Dict, List
import pandas as pd
from functools import cmp_to_key
from datetime import datetime
from pathlib import Path
import logging

from plotly.subplots import make_subplots
from plotly.graph_objects import Figure
from plotly import offline
import plotly.graph_objects as go
from sqlalchemy.sql import select
import datetime
from sqlalchemy import and_
from pandas import DataFrame

from kektrade.plotting.plotter import Plotter
from kektrade.database.types import *

logger = logging.getLogger(__name__)


class PlotterOptimize(Plotter):
    def plot_optimize(self,
                      db_path: Path,
                      html_path: Path,
                      plot_name: str,
                      subaccount_ids: List[int]) -> None:
        session = get_session(db_path)
        conn = session.bind

        query = select(Subaccount).filter(Subaccount.id.in_(subaccount_ids))
        data_subaccounts = pd.read_sql(query, con=conn)

        query = select(Wallet).filter(Wallet.subaccount_id.in_(subaccount_ids))
        data_wallet = pd.read_sql(query, con=conn)

        query = (
            f"select * from ticker t "
            f"join pair p on t.pair_id = p.id "
            f"where fl_main <> 0 and t.subaccount_id = {subaccount_ids[0]}"
        )
        data_ticker = pd.read_sql(query, con=conn)

        fig = self._generate_fig(plot_name)
        self._plot_candlestick(fig, data_ticker)
        self._plot_wallet(fig, data_subaccounts, data_wallet)
        self._save_plot_html(fig, html_path)

    def _generate_fig(self, name: str) -> Figure:
        # Define the graph
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.5, 0.5],
        )
        fig['layout'].update(title=name)
        fig['layout']['yaxis1'].update(title='Price')
        fig['layout']['yaxis2'].update(title='Balance')
        fig['layout']['xaxis']['rangeslider'].update(visible=False)
        fig['layout']['yaxis'].update(autorange=True)
        fig['layout']['yaxis'].update(fixedrange=False)

        fig.update_xaxes(showticklabels=False)
        fig.update_xaxes(linecolor='Grey', gridcolor='Gainsboro')
        fig.update_yaxes(linecolor='Grey', gridcolor='Gainsboro')
        fig.update_xaxes(title_text='Price', row=1)
        fig.update_xaxes(title_text='Wallet', row=2)
        fig.update_xaxes(title_standoff=7, title_font=dict(size=12))

        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)

        fig.update_annotations({'font': {'size': 12, "font_family": "Consolas"}})
        fig.update_layout(template='plotly_white')
        fig.update_layout(showlegend=True)
        fig.update_layout(
            font_family="Consolas",
            autosize=True,
            margin=go.layout.Margin(
                l=0,
                r=0,
                b=0,
                t=30,
                pad=0
            )
        )

        fig.update_layout(
            hoverlabel=dict(
                font_family="Consolas"
            )
        )

        return fig

    def _plot_candlestick(self, fig: Figure, df: DataFrame):
        candles = go.Candlestick(
            x=df.date,
            open=df.open,
            high=df.high,
            low=df.low,
            close=df.close,
            name='Price Candlestick',
            visible="legendonly"
        )
        fig.add_trace(candles, 1, 1)

        candles = go.Scatter(
            x=df["date"],
            y=df["close"],
            name='Price Line',
            line={'color': "black"},
            visible=True
        )
        fig.add_trace(candles, 1, 1)

        volume = go.Bar(
            x=df["date"],
            y=df['volume'],
            name='Volume',
            marker_color='DodgerBlue',
            marker_line_color='DodgerBlue',
            visible="legendonly"
        )
        fig.add_trace(volume, 1, 1)

    def _plot_wallet(self, fig: Figure, data_subaccounts, data_wallets: DataFrame):
        length = len(data_subaccounts.index)

        import seaborn as sns
        palette = sns.color_palette("bright", length)

        for i in range(length):
            parameter = data_subaccounts.at[i, "parameter"]
            color = "rgb" + str(palette[i])
            data_wallet = data_wallets[data_wallets["subaccount_id"] == data_subaccounts.at[i, "id"]]
            Plotter.add_scatter(fig, data_wallet, "account_balance", True, 2, column_name=parameter, color=color)

    def _save_plot_html(self, fig: Figure, path: Path):
        offline.plot(fig, filename=str(path), auto_open=False)
