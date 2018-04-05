# -*- coding: utf-8 -*-
#
# discription: 給湯機の使用など、夜間に大きな電力を使っている時間帯を特定する
# input:
#   - CSV形式の時系列電力データ
# output:
#   - 結果ファイル(内容は以下のとおり)
#     - 開始時間
#     - 終了時間
# arguments:
#   argvs[1]: 入力ファイルのパス
#   argvs[2]: 電力値データのカラム名
#   argvs[3]: 出力ファイルのパス
#   argvs[4]: バッファ幅(0.5単位の数値)
# note:
#   データファイルの条件:
#    - pandasデータフレームとして読み込めること
#    - 1レコードが1時点を表す、時系列データであること
#    - カラム名は必須、インデックス有無は問わない
#    - 以下を表すデータが含まれること
#      - 日時
#      - 電力値
#    - 日時データのカラム名は「timestamps」であること
#    - 日時データの形式は yyyy-MM-dd HH:mm:ss であること
#      (例えば、2015-01-01 12:34:56)
#    - 電力値データは、数値データであること
#    - 日時および電力データの欠損値は、空値または""であること。
#      欠損があった場合は、欠損レコードをNAで補間して動作する。

# --基本モジュール--
import pandas as pd
import numpy as np
import os
import sys

# 定義関数
import utils

# ログ用
import traceback
from logging import getLogger, StreamHandler, FileHandler, INFO, ERROR
import datetime
cmd = "find_NTZ"
pid = str(os.getpid())
logfile = "/tmp/"+cmd+"_"+pid+".log"
logger = getLogger(cmd)
Fhandler = FileHandler(logfile)
Fhandler.setLevel(INFO)
logger.addHandler(Fhandler)
Shandler = StreamHandler()
Shandler.setLevel(ERROR)
logger.addHandler(Shandler)
logger.setLevel(INFO)


# 引数取得
argvs = sys.argv
arg_str = ' '.join(map(str, argvs))


# ログ関数
def error_exit(code, msg):
    d = datetime.datetime.today()
    logger.error(d.strftime("%Y-%m-%d %H:%M:%S")+" ERROR "+cmd+" - "
                 + str(msg)+" command: "+arg_str)
    logfile2 = \
        "/var/log/"+cmd+"_"+d.strftime("%Y%m%d%H%M%S")+"_"+pid+".log"
    os.rename(logfile, logfile2)
    sys.exit(code)


def debug_print(msg):
    d = datetime.datetime.today()
    logger.info(d.strftime("%Y-%m-%d %H:%M:%S")+" INFO "+cmd+" - "
                + str(msg)+" command: "+arg_str)


# 目的：時刻(str)と変動幅合計値(float)のデータから、時刻の平均値と標準偏差を
#       計算する
# 引数：データフレーム、集計対象カラム名、時刻を表すカラム名
# 返り値1：時刻の平均値(float)
# 返り値2：時刻の標準偏差(float)
def calc_time_statistics(DF, tgt_col, time_col):
    tmp_DF = DF.copy()
    tmp_DF['float_time'] = tmp_DF[time_col].apply(utils.time2float_night)
    tmp_array = np.array([])
    for i in range(len(tmp_DF)):
        bind = np.repeat(tmp_DF["float_time"][i], int(tmp_DF[tgt_col][i]*100))
        tmp_array = np.r_[tmp_array, bind]
    return np.mean(tmp_array), np.std(tmp_array)


# 固定パラメータ
ccr_rate = 0.75  # 累積比率でデータを削除する際の閾値


# main処理
if __name__ == '__main__':
    # 引数チェック
    debug_print("start process.")

    if len(argvs) <= 4:
        error_exit(1, "number of args is less than expected. [main]")

    try:
        in_file = str(argvs[1])
        tgt_colname = str(argvs[2])
        out_file = str(argvs[3])
        buf_range = float(argvs[4])
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [str/float]")

    # 出力先ディレクトリ生成
    try:
        utils.make_outdir(out_file)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])
                   + " [make_outdir]")

    # ファイル読み込み
    debug_print("start reading input file.")
    try:
        in_data = pd.read_csv(in_file, dtype=object)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [pd.read_csv]")
    debug_print("end reading input file.")

    # カラムの確認
    debug_print('start data check.')
    if "timestamps" not in in_data.columns:
        error_exit(1, "timestamps"+" NOT in "+in_file+". [main]")

    if tgt_colname not in in_data.columns:
        error_exit(1, tgt_colname+" NOT in "+in_file+". [main]")

    # timestampsの形式チェック
    try:
        utils.validate_format_all(in_data['timestamps'])
    except:
        error_exit(1, 'format of timestamps is incorrect. [main]')

    # レコード数の確認 一ヵ月の半分より少ない場合エラー 672 = 28×48×1/2
    if len(in_data[["timestamps", tgt_colname]].dropna()) < 672:
        error_exit(1, "number of valid records is less than expected. [main]")

    # 対象カラムを数値型に変換
    try:
        in_data[tgt_colname] = in_data[tgt_colname].astype(float)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [astype]")
    debug_print('end data check.')

    debug_print("start resampling data.")
    # timestampsの欠損レコードを削除
    try:
        in_data = in_data.dropna(subset=["timestamps"])
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [dropna]")

    # レコードごと削除されている場合、補間してNAを入れる
    try:
        in_data = utils.resample_30min(in_data)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])
                   + " [reample_30min]")
    debug_print("end resampling data.")

    # 増加幅、減少幅の集計
    debug_print("start aggregating data.")
    try:
        # 時刻だけを分離
        in_data['time'] = [x.split(' ')[1][0:5] for x in in_data['timestamps']]

        # 1時点前の電力量の差分項を追加
        in_data["whp_diff"] = in_data["whp"]-in_data["whp"].shift(1)

        # 欠損があった場合にNAとなったdiffの値を0にする ※念のため
        in_data["whp_diff"] = in_data["whp_diff"].fillna(0)

        # 時間毎の増加幅、減少幅の集計
        tmp_data = in_data[in_data['whp_diff'] > 0][['time', 'whp_diff']]
        up_aggr = tmp_data.groupby('time', as_index=False).sum()
        up_aggr = up_aggr.set_index("time")
        up_aggr.columns = ["UP"]

        tmp_data = in_data[in_data['whp_diff'] < 0][['time', 'whp_diff']]
        dw_aggr = tmp_data.groupby('time', as_index=False).sum()
        dw_aggr['whp_diff'] = dw_aggr['whp_diff']*(-1)
        dw_aggr = dw_aggr.set_index("time")
        dw_aggr.columns = ["DOWN"]
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [main]")
    debug_print("end aggregating data.")

    # 集計範囲の限定
    debug_print("start limiting data.")
    try:
        up_aggr = up_aggr.reset_index()
        up_aggr = up_aggr[(up_aggr["time"] >= "22:59:00") |
                          (up_aggr["time"] <= "05:01:00")]
        dw_aggr = dw_aggr.reset_index()
        dw_aggr = dw_aggr[(dw_aggr["time"] >= "00:59:00") &
                          (dw_aggr["time"] <= "08:01:00")]
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [main]")
    if len(up_aggr) == 0:
        error_exit(1, "there are no up changes in aggregate range. [main]")
    if len(dw_aggr) == 0:
        error_exit(1, "there are no down changes in aggregate range. [main]")
    debug_print("end limiting data.")

    debug_print("start CCR cut.")
    # 値の小さい時間を削除（累積比率で削除）
    try:
        up_aggr = utils.ccr_cut(up_aggr, "UP", ccr_rate)
        up_aggr = up_aggr.reset_index(drop=True)
        dw_aggr = utils.ccr_cut(dw_aggr, "DOWN", ccr_rate)
        dw_aggr = dw_aggr.reset_index(drop=True)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [ccr_cut]")
    debug_print("end CCR cut.")

    debug_print("start calculating start/end time.")
    # 平均時間、標準偏差を算出
    try:
        stime_mean, stime_std = calc_time_statistics(up_aggr, "UP", "time")
        etime_mean, etime_std = calc_time_statistics(dw_aggr, "DOWN", "time")
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])
                   + " [calc_time_statistics]")

    # 開始時間、終了時間を算出
    try:
        stime = stime_mean - stime_std - buf_range
        etime = etime_mean + etime_std + buf_range
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [main]")

    # 0.5単位の数値への変換
    try:
        stime = utils.fix_median(stime)
        etime = utils.fix_median(etime)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])+" [fix_median]")
    debug_print("end calculating start/end time.")

    # 結果ファイルの出力
    debug_print('start output file.')
    try:
        result_lst = [utils.check24(stime), utils.check24(etime),
                      utils.check24(stime_mean), stime_std,
                      utils.check24(etime_mean), etime_std]
        result_data = pd.DataFrame(result_lst, columns=['NoiseTimeZone'])
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])
                   + " [check24/DataFrame]")
    if result_data.isnull().values.any():
        error_exit(1, "unexpected error: NaN included in result. [main]")
    try:
        result_data.to_csv(out_file, index=False)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc(sys.exc_info()[2])
                   + " [to_csv]")
    debug_print('end output file.')

    debug_print("end process.")

    os.remove(logfile)

sys.exit(0)
