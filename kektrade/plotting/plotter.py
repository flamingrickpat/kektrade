import plotly.graph_objects as go
import numpy as np

class Plotter():
    @staticmethod
    def zero_to_nan(data):
        return data[data.columns].replace({'0':np.nan, 0:np.nan})

    @staticmethod
    def add_scatter(fig, data, column, visible, x, color=None, zero_to_nan=False, column_name=None):
        if visible:
            v = True
        else:
            v = "legendonly"

        if not column_name:
            column_name = column

        col = data[column]
        if zero_to_nan:
            col = col.replace({'0':np.nan, 0:np.nan})

        profit = go.Scatter(
            x=data["datetime"],
            y=col,
            name=column_name,
            visible=v,
            line={'color': color},
        )
        fig.add_trace(profit, x, 1)
