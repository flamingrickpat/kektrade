from pandas import Series
from pandas import DataFrame
import numpy as np

def rolling_mean(series, window=200, min_periods=None):
    return series.rolling(window=window, min_periods=min_periods).mean()

def wave_pm(series, window, look_back_periods=100):
    ma = rolling_mean(series, window=window)
    std = Series(series).rolling(window=window).std(ddof=0)

    def tanh(x):
        two = np.where(x > 0, -2, 2)
        what = two * x
        ex = np.exp(what)
        j = 1 - ex
        k = ex - 1
        l = np.where(x > 0, j, k)
        output = l / (1 + ex)
        return output

    def osc(input_dev, mean, look_back):
        variance = Series(power).rolling(window=look_back, min_periods=7).sum() / look_back
        calc_dev = np.sqrt(variance) * mean
        y = (input_dev / calc_dev)
        oscLine = tanh(y)
        return oscLine

    dev = 3.2 * std
    power = np.power(dev /ma, 2)
    wavePM = osc(dev, ma, look_back_periods)
    wavePM.iloc[0:window] = 0

    return DataFrame(index=series.index, data={"wavePM": wavePM})


def calculate_wavepm_bands(df, lookback=100, wavepm_column="close", periods=None, min_period=6, smoothing_period=0, multiplikator=1):
    def Sma(src, p):
        l = np.arange(len(src)) - p.values

        a = src.cumsum()
        tmp = np.where(l >= 0, a.values[l], np.nan)

        b = (a - tmp) / p
        return b

    def Stdev(src, p):
        s = np.where(p == 1, 0, np.sqrt(Sma(src * src, p) - np.power(Sma(src, p), 2)))
        return s

    def bandMaker(one, two, num):
        return one + (two * (num))


    if periods is None:
        periods = []
        p = min_period
        while True:
            periods.append(p * multiplikator)
            if p > 0:
                p += 1
            if p > 100:
                p += 2
            if p > 400:
                p += 4
            if p > 800:
                p += 8
            if p >= 2000:
                break

    # Calculate all WavePMs
    # Save the Position in Column List for faster access
    column_index_map = {}
    column_names = []
    for i in periods:
        colname = f"{i}"
        df[colname] = wave_pm(df[wavepm_column], window=i, look_back_periods=lookback * multiplikator)
        column_index_map[i] = df.columns.get_loc(colname)
        column_names.append(colname)

    df_wavepms = df[column_names]
    df_wavepms_loxp = df_wavepms.reset_index(drop=True)
    df_wavepms_clcp = df_wavepms.reset_index(drop=True)
    df_wavepms_lntp = df_wavepms.reset_index(drop=True)

    LIMIT_LOXP = 0.9
    LIMIT_CLCP = 0.3
    LIMIT_LNTP = 0.6

    COOLOFF_PERIOD = 10

    for col in column_names:
        val = int(col)
        df_wavepms_loxp[col] = np.where(df_wavepms_loxp[col] > LIMIT_LOXP, val, 0)
        df_wavepms_clcp[col] = np.where(df_wavepms_clcp[col] < LIMIT_CLCP, val, 0)
        df_wavepms_lntp[col] = np.where(df_wavepms_lntp[col] < LIMIT_LNTP, val, 0)

    df_wavepms_loxp["0"] = 1
    df_wavepms_clcp["0"] = 1
    df_wavepms_lntp["0"] = 1

    df["loxp"] = df_wavepms_loxp.idxmax(axis=1).astype(int)
    df["clcp"] = df_wavepms_clcp.idxmax(axis=1).astype(int)
    df["tlcp"] = 0
    df["lntp"] = df_wavepms_lntp.idxmax(axis=1).astype(int)

    del df_wavepms_loxp
    del df_wavepms_clcp
    del df_wavepms_lntp

    df["wloxp"] = 0
    df["wclcp"] = 0
    df["wtlcp"] = 0
    df["wlntp"] = 0

    idx_loxp = df.columns.get_loc(f"loxp")
    idx_clcp = df.columns.get_loc(f"clcp")
    idx_tlcp = df.columns.get_loc(f"tlcp")
    idx_lntp = df.columns.get_loc(f"lntp")
    idx_wloxp = df.columns.get_loc(f"wloxp")
    idx_wclcp = df.columns.get_loc(f"wclcp")
    idx_wtlcp = df.columns.get_loc(f"wtlcp")
    idx_wlntp = df.columns.get_loc(f"wlntp")

    # WLOXP muss seperat gemacht werden da er bei Mid Crossover aufhört!
    src = df["close"]
    loxp = df["loxp"]
    clcp = df["clcp"]
    lntp = df["lntp"]

    loxp_m = Sma(src, loxp)

    last_period_wloxp = 0
    last_period_wclcp = 0
    last_period_wlntp = 0

    cooloff_wloxp = 0
    cooloff_wclcp = 0
    cooloff_wlntp = 0


    l = len(df.index)
    for i in range(l):
        # WLOXP
        # LOXP aus DF nehmen
        period_wloxp = 0
        period_loxp = df.iat[i, idx_loxp]
        # Wenn LOXP höher ist als WLOXP, WLOXP auf LOXP setzen
        if period_loxp > last_period_wloxp:
            cooloff_wloxp = COOLOFF_PERIOD
            last_period_wloxp = period_loxp
        # Erst zurücksetzen wenn WLOXP < 0.7 ist
        if last_period_wloxp > 0:
            tmp_wavepm = df.iat[i, column_index_map[last_period_wloxp]]
            if tmp_wavepm > 0.7:
                cooloff_wloxp = COOLOFF_PERIOD
                period_wloxp = last_period_wloxp
            elif cooloff_wloxp > 0:
                period_wloxp = last_period_wloxp
            else:
                period_wloxp = 0
                last_period_wloxp = 0
            cooloff_wloxp -= 1
            if ((src.iloc[i] > loxp_m.iloc[i]) and (src.iloc[i - 1] < loxp_m.iloc[i - 1])) or \
                    ((src.iloc[i] < loxp_m.iloc[i]) and (src.iloc[i - 1] > loxp_m.iloc[i - 1])):
                period_wloxp = 0
                last_period_wloxp = 0
                cooloff_wloxp = 0

        df.iat[i, idx_wloxp] = period_wloxp

        # WCLCP
        # CLCP aus DF nehmen
        period_wclcp = 0
        period_clcp = df.iat[i, idx_clcp]
        # Wenn clcp höher ist als Wclcp, Wclcp auf clcp setzen
        if period_clcp > last_period_wclcp:
            cooloff_wclcp = COOLOFF_PERIOD
            last_period_wclcp = period_clcp
        # Erst zurücksetzen wenn Wclcp > 0.5 ist
        if last_period_wclcp > 0:
            tmp_wavepm = df.iat[i, column_index_map[last_period_wclcp]]
            if tmp_wavepm < 0.4:
                cooloff_wclcp = COOLOFF_PERIOD
                period_wclcp = last_period_wclcp
            elif cooloff_wclcp > 0:
                period_wclcp = last_period_wclcp
            else:
                period_wclcp = 0
                last_period_wclcp = 0
            cooloff_wclcp -= 1
        df.iat[i, idx_wclcp] = period_wclcp


        # WLNTP
        # LNTP aus DF nehmen
        period_wlntp = 0
        period_lntp = df.iat[i, idx_lntp]
        # Wenn lntp höher ist als WLNTP, WLNTP auf lntp setzen
        if period_lntp > last_period_wlntp:
            cooloff_wlntp = COOLOFF_PERIOD
            last_period_wlntp = period_lntp
        # Erst zurücksetzen wenn WLNTP > 0.8 ist
        if last_period_wlntp > 0:
            tmp_wavepm = df.iat[i, column_index_map[last_period_wlntp]]
            if tmp_wavepm < 0.7:
                cooloff_wlntp = COOLOFF_PERIOD
                period_wlntp = last_period_wlntp
            elif cooloff_wlntp > 0:
                period_wlntp = last_period_wlntp
            else:
                period_wlntp = 0
                last_period_wlntp = 0
            cooloff_wlntp -= 1
        df.iat[i, idx_wlntp] = period_wlntp

    # End-Bands generieren
    result_bands = ["loxp", "clcp", "lntp", "wloxp", "wclcp", "wlntp"]
    for band in result_bands:
        vband = df[band]
        band_m = Sma(src, vband)
        band_s = Stdev(src, vband)

        df[f"{band}_m"] = band_m
        df[f"{band}_s"] = band_s

        df[f"bb_upper_{band}32"] = bandMaker(band_m, band_s, 3.2)
        df[f"bb_upper_{band}"] = bandMaker(band_m, band_s, 1.25)
        df[f"bb_mid_{band}"] = band_m
        df[f"bb_lower_{band}"] = bandMaker(band_m, band_s, -1.25)
        df[f"bb_lower_{band}32"] = bandMaker(band_m, band_s, -3.2)

        if smoothing_period > 0:
            df[f"bb_upper_{band}32"] = df[f"bb_upper_{band}32"].rolling(smoothing_period).mean()
            df[f"bb_upper_{band}"]   = df[f"bb_upper_{band}"].rolling(smoothing_period).mean()
            df[f"bb_mid_{band}"]     = df[f"bb_mid_{band}"].rolling(smoothing_period).mean()
            df[f"bb_lower_{band}"]   = df[f"bb_lower_{band}"].rolling(smoothing_period).mean()
            df[f"bb_lower_{band}32"] = df[f"bb_lower_{band}32"].rolling(smoothing_period).mean()

    # WavePM Columns droppen weil dann alles schneller ist.
    df.drop(column_names, axis=1, inplace=True)
    return df.copy()


