import numpy as np
import logging
import pandas as pd

class VolumeBarAggregator():
    @staticmethod
    def convert(df, rolling_median_window, target_timeframe, keep_unfinished_canldes=False):
        """
        Convert OHCL data from time bars to volume bars.
        """
        first_run = True

        date = None
        open = 0.0
        high = 0.0
        low = 0.0
        close = 0.0
        cum_volume = 0.0
        funding_rate = 0.0
        candle_count = 0

        arr_date = []
        arr_open = []
        arr_high = []
        arr_low = []
        arr_close = []
        arr_volume = []
        arr_candle_count = []
        arr_funding_rate = []

        df = df.reset_index(drop=True)
        df["rolling_median_volume"] = df.volume.rolling(rolling_median_window).median().shift(1) * target_timeframe

        tmp = df[['date', 'open', 'high', 'low', 'close', 'volume', 'funding_rate', 'rolling_median_volume']].copy()
        loc_date = tmp.columns.get_loc("date")
        loc_open = tmp.columns.get_loc("open")
        loc_high = tmp.columns.get_loc("high")
        loc_low = tmp.columns.get_loc("low")
        loc_close = tmp.columns.get_loc("close")
        loc_volume = tmp.columns.get_loc("volume")
        loc_funding_rate = tmp.columns.get_loc("funding_rate")
        loc_rolling_median_volume = tmp.columns.get_loc("rolling_median_volume")
        numpy_df = tmp.to_numpy()

        l = len(numpy_df)
        for i in range(l):
            threshold = numpy_df[i, loc_rolling_median_volume]
            if np.nan_to_num(threshold) > 0:
                if first_run:
                    append = True
                else:
                    append = cum_volume > threshold

                if append:
                    first_run = False

                    # Append if above Threshold
                    if date is not None:
                        arr_date.append(date)
                        arr_open.append(open)
                        arr_high.append(high)
                        arr_low.append(low)
                        arr_close.append(close)
                        arr_volume.append(cum_volume)
                        arr_candle_count.append(candle_count)
                        arr_funding_rate.append(funding_rate)

                    date = numpy_df[i, loc_date]
                    open = numpy_df[i, loc_open]
                    high = numpy_df[i, loc_high]
                    low = numpy_df[i, loc_low]
                    close = numpy_df[i, loc_close]
                    cum_volume = numpy_df[i, loc_volume]
                    funding_rate = numpy_df[i, loc_funding_rate]
                    candle_count = 1
                else:
                    date = numpy_df[i, loc_date]
                    high = max(high, numpy_df[i, loc_high])
                    low = min(low, numpy_df[i, loc_low])
                    close = numpy_df[i, loc_close]
                    cum_volume += numpy_df[i, loc_volume]
                    funding_rate += numpy_df[i, loc_funding_rate]
                    candle_count += 1

        if keep_unfinished_canldes:
            arr_date.append(date)
            arr_open.append(open)
            arr_high.append(high)
            arr_low.append(low)
            arr_close.append(close)
            arr_volume.append(cum_volume)
            arr_candle_count.append(candle_count)
            arr_funding_rate.append(funding_rate)

        vol_bars = pd.DataFrame(
            {
                "date": arr_date,
                "open": arr_open,
                "high": arr_high,
                "low": arr_low,
                "close": arr_close,
                "volume": arr_volume,
                "candle_count": arr_candle_count
            }
        )

        vol_bars = vol_bars.reset_index(drop=True)
        return vol_bars


    def reapply_to_df(self, volume_df, orig_df, columns, prefix=""):
        import sqlite3
        db = sqlite3.connect(":memory:")

        orig_df["idx"] = orig_df.index
        volume_df.to_sql("volume_df", db, if_exists="append")
        orig_df.to_sql("orig_df", db, if_exists="append")

        # Add an index on the 'street' column:
        db.execute("CREATE INDEX orig_df_idx ON orig_df(idx)")
        db.execute("CREATE INDEX lab_start_index_idx ON volume_df(lab_start_index)")
        db.execute("CREATE INDEX lab_end_index_idx ON volume_df(lab_end_index)")

        qry = "select o.date, o.open, o.high, o.low, o.close, o.volume"
        for col in columns:
            qry = qry + ", v." + col
            if prefix != "":
                qry = qry + f' as "{prefix}_{col}"'


        qry = qry + " from orig_df o"
        qry = qry + " join volume_df v on o.idx >= v.lab_start_index and o.idx <= v.lab_end_index"

        new_df = pd.read_sql_query(qry, db)
        db.close()

        # Leere Zeilen am Anfang einfügen damit man Spalten in Originales DF übernhemn kann
        l = len(orig_df.index) - len(new_df.index)
        df1 = pd.DataFrame([[np.nan] * len(new_df.columns)] * l, columns=new_df.columns)
        new_df = df1.append(new_df, ignore_index=True)

        for col in columns:
            if prefix != "":
                col = f'{prefix}_{col}'
            orig_df[col] = new_df[col]

        del df1
        del new_df

        return orig_df

