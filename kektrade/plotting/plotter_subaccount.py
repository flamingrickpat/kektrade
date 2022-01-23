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


class PlotterSubaccount(Plotter):
    def plot_subaccount_range(self,
                              db_path: Path,
                              html_path: Path,
                              plot_name: str,
                              subaccount_id: int,
                              start: datetime,
                              end: datetime,
                              indicators: List[Dict[str, Any]]) -> None:
        session = get_session(db_path)

        def select_classtype(classtype):
            return select([classtype]).where(and_(
                classtype.subaccount_id == subaccount_id,
                classtype.datetime >= start,
                classtype.datetime <= end))

        conn = session.bind

        query = select_classtype(Order)
        data_order = pd.read_sql(query, con=conn)

        query = select_classtype(Position)
        data_position = pd.read_sql(query, con=conn)

        query = select_classtype(Execution)
        data_execution = pd.read_sql(query, con=conn)

        query = select_classtype(Wallet)
        data_wallet = pd.read_sql(query, con=conn)

        query = (
            f"select distinct * from ticker t "
            f"join pair p on t.pair_id = p.id "
            f"where p.subaccount_id = {subaccount_id} and t.subaccount_id = {subaccount_id}"
        )
        data_ticker = pd.read_sql(query, con=conn)

        fig = self._generate_fig(plot_name)
        self._plot_candlestick(fig, data_ticker)
        self._plot_indicators(fig, data_ticker, indicators)
        self._plot_position(fig, data_position)
        self._plot_wallet(fig, data_wallet)

        reduce = data_execution[(data_execution["reduce_or_expand"] == ReduceExpandType.REDUCE) &
                                (data_execution["execution_type"] == ExecutionType.TRADE)].copy()
        expand = data_execution[(data_execution["reduce_or_expand"] == ReduceExpandType.EXPAND) &
                                (data_execution["execution_type"] == ExecutionType.TRADE)].copy()
        liquidation = data_execution[data_execution["execution_type"] == ExecutionType.LIQUIDATION].copy()
        funding = data_execution[data_execution["execution_type"] == ExecutionType.FUNDING].copy()
        self._plot_execution(fig, reduce, "Execution Reduce")
        self._plot_execution(fig, expand, "Execution Expand")
        self._plot_execution(fig, liquidation, "Execution Liquidation")
        self._plot_execution(fig, funding, "Execution Funding")

        self._plot_order(fig, data_order)
        self._save_plot_html(fig, html_path)


    def _generate_fig(self, name: str) -> Figure:
        # Define the graph
        fig = make_subplots(
            rows=4,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.55, 0.15, 0.15, 0.15],
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
        fig.update_xaxes(title_text='Indicator', row=2)
        fig.update_xaxes(title_text='Position', row=3)
        fig.update_xaxes(title_text='Wallet', row=4)
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


    def _plot_indicators(self, fig: Figure, data: DataFrame, indicators: List[Dict[str, Any]]):
        for indicator in indicators:
            name = indicator["name"]
            plot = True
            if "plot" in indicator and indicator["plot"]:
                plot = indicator["plot"]

            if name in data and plot:
                visible = "legendonly"
                if "visible" in indicator:
                    if indicator["visible"]:
                        visible = True

                mode = "lines"
                if "scatter" in indicator and indicator["scatter"]:
                    mode = "markers"

                fillcolor = None
                if "color" in indicator:
                    fillcolor = indicator["color"]

                if fillcolor == None:
                    scattergl = go.Scatter(
                        x=data['date'],
                        y=data[name].values,
                        mode=mode,
                        name=name,
                        visible=visible
                    )
                else:
                    scattergl = go.Scatter(
                        x=data['date'],
                        y=data[name].values,
                        mode=mode,
                        name=name,
                        visible=visible,
                        line={'color': fillcolor},
                    )

                overlay = True
                if "overlay" in indicator:
                    overlay = indicator["overlay"]

                if overlay:
                    fig.add_trace(scattergl, 1, 1)
                else:
                    fig.add_trace(scattergl, 2, 1)
            else:
                logger.info(
                    'Indicator "%s" ignored. Reason: This indicator is not found '
                    'in your strategy.',
                    indicator
                )


    def _plot_position(self, fig: Figure, df: DataFrame):
        Plotter.add_scatter(fig, df, "price", True, 1, zero_to_nan=True)
        Plotter.add_scatter(fig, df, "liquidationPrice", False, 1, zero_to_nan=True)
        Plotter.add_scatter(fig, df, "bankruptcyPrice", False, 1, zero_to_nan=True)

        Plotter.add_scatter(fig, df, "contracts", True, 3)
        Plotter.add_scatter(fig, df, "leverage", False, 3)

        Plotter.add_scatter(fig, df, "unrealizedPnl", False, 4)
        Plotter.add_scatter(fig, df, "collateral", False, 4)
        Plotter.add_scatter(fig, df, "initialMargin", False, 4)
        Plotter.add_scatter(fig, df, "maintenanceMargin", False, 4)


    def _plot_wallet(self, fig: Figure, df: DataFrame):
        Plotter.add_scatter(fig, df, "account_balance", True, 4)
        Plotter.add_scatter(fig, df, "margin_balance", False, 4)
        Plotter.add_scatter(fig, df, "available_balance", False, 4)
        Plotter.add_scatter(fig, df, "total_rpnl", False, 4)
        Plotter.add_scatter(fig, df, "order_margin", False, 4)
        Plotter.add_scatter(fig, df, "position_margin", False, 4)

    def _plot_order(self, fig: Figure, df: DataFrame):
        if df is not None and len(df.index) > 0:
            desc = df.apply(lambda row: row.to_string().replace('\n', '</br>'), axis=1)
            shape = df.apply(lambda row: ('circle' if row['contracts'] > 0 else 'circle'), axis=1)
            color = df.apply(lambda row: 'FireBrick' if row["contracts"] < 0 else 'DarkGreen', axis=1)

            trace = go.Scatter(
                x=df["datetime"],
                y=df["price"],
                text=desc,
                mode='markers',
                name="Open Orders",
                marker=dict(
                    symbol=shape,
                    size=5,
                    line=dict(width=0),
                    color=color
                ),
                visible=True
            )
            fig.add_trace(trace, 1, 1)
        return fig

    def _plot_execution(self, fig: Figure, df: DataFrame, name: str):
        if df is not None and len(df.index) > 0:

            color = df.apply(lambda row: 'orange' if row["reduce_or_expand"] == ReduceExpandType.EXPAND.value else
                    ('DarkGreen' if row['cost'] > 0 else 'FireBrick'), axis=1)

            shape = df.apply(lambda row: ('star' if row["execution_type"] == ExecutionType.FUNDING else
                                            'x' if row["execution_type"] == ExecutionType.LIQUIDATION else
                                            'triangle-up' if row['contracts'] > 0 else 'triangle-down'), axis=1)

            desc = df.apply(lambda row: row.to_string().replace('\n', '</br>'), axis=1)

            trace = go.Scatter(
                x=df["datetime"],
                y=df["price"],
                text=desc,
                mode='markers',
                name=name,
                marker=dict(
                    symbol=shape,
                    size=10,
                    line=dict(width=0),
                    color=color
                ),
                visible=True
            )
            fig.add_trace(trace, 1, 1)
        return fig

    def _save_plot_html(self, fig: Figure, path: Path):
        offline.plot(fig, filename=str(path), auto_open=False)