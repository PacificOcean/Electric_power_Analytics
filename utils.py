# -*- coding: utf-8 -*-
#
# discription: define utility functions

# --基本モジュール--
import pandas as pd
import numpy as np
import os
from math import modf  # 小数部分を抽出するために使用
import dateutil.parser  # 変数の時間型への変換で使用
import datetime


# 関数定義
# 目的：時間を小数に変換する関数
# 引数：時間データ[str型](形式:HH:mm:ss)
# 返り値：時間データ[float型]
def time2float(x):
    num1 = float(x.split(':')[0])
    num2 = float(x.split(':')[1])
    return num1 + num2 / 60


# 目的：時間を小数に変換する関数
# 引数：時間データ[str型](形式:HH:mm:ss)
# 返り値：時間データ[float型]、ただし、0-12事は夜時間(+24h)に変換
def time2float_night(x):
    num1 = float(x.split(':')[0])
    num2 = float(x.split(':')[1])
    ret_num = num1 + num2/60
    # 0-12時を夜時間(+24h)に変換
    if ret_num <= 12:
        ret_num = ret_num + 24
    return ret_num


# 目的：深夜の時間表記を変更する関数
# 引数：時間データ[str型](形式:HH:mm:ss)
# 返り値：時間データ[str型](形式:HH:mm:ss)
def convert_nighttime(x):
    if '00:00:00' <= x and '03:00:00' >= x:
        tmp = int(x.split(':')[0]) + 24
        return str(tmp) + ':' + x.split(':')[1] + ':' + x.split(':')[2]
    else:
        return x


# 目的：中央値の値を整形する関数
# 引数：算出した中央値[float型]
# 返り値：整形した中央値[float型]
def fix_median(x):
    float_part = round(modf(x)[0], 2)
    if 0 <= float_part and 0.25 > float_part:
        return modf(x)[1]
    elif 0.25 <= float_part and 0.75 > float_part:
        return modf(x)[1] + 0.5
    elif 0.75 <= float_part and 1.0 > float_part:
        return modf(x)[1] + 1.0
    else:
        return modf(x)[1]


# 目的：24時間表記の確認関数
# 引数：fix_medianのreturn値[float型]
# 返り値：整形した中央値[float型]
def check24(x):
    if x >= 24.0:
        return x - 24.0
    else:
        return x


# 目的：timestampのフォーマットを確認するための関数のサブ関数
# 引数：判定したいtimestampsの文字列[str型]
# 返り値：正常の場合に復帰値0、異常の場合に復帰値0以外
def validate_format(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        raise e
    return 0


# 目的：timestampのフォーマット確認するための関数
# 引数：timestampsのデータ[Series型]
# 返り値：正常の場合に復帰値0、異常の場合に復帰値0以外
def validate_format_all(data_sr):
    try:
        data_sr.dropna().apply(validate_format)
    except Exception as e:
        raise e
    return 0


# 目的：指定したファイルの親ディレクトリが存在しない場合に、ディレクトリを作成
#       する
# 引数：ファイルのパス
# 返り値：処理が正常に動作した場合に、0を返す。
#         エラーが発生した場合は、pythonのエラーコードを返す。
def make_outdir(out_path):
    out_dir = os.path.dirname(out_path)
    if len(out_dir) == 0:
        out_dir = "."  # カレントディレクトリ指定の場合、何もしない
    elif not os.path.exists(out_dir):
        os.makedirs(out_dir)
    return 0


# 目的：日時を表す文字列をdatetime型への変換
# 引数：日時を表す文字列(str型)
# 返り値：datetime型に変換された日時
def datetime_parser(x):
    return dateutil.parser.parse(x)


# 目的：30分値の電力データにおいて、欠損値がレコードごと削除されている場合に
#       NAで補間をする。
# 引数：以下の条件を満たすデータフレーム
#       - 日時を表すtimestampsカラムを含んでいること
#       - timestampsカラムの型はobject(str)であること
#       - timestampsカラムの欠損はdropnaしておくこと
#       - 1つ以上、数値型のカラムを含むこと※
#         ※resample()を適用すると、数値カラム以外は消えるため
# 返り値：補間されたデータフレーム
#     　　【注】元データのインデックス番号は変わらないようにするため、
#               補間された行のインデックス番号は前後で連番とならない
def resample_30min(DF):
    tmp_DF = DF.copy()
    # datatime型に変換
    tmp_DF["timestamps"] = tmp_DF["timestamps"].apply(datetime_parser)
    # 30分値にresample
    tmp_DF = tmp_DF.set_index("timestamps").resample("1800S").mean()
    tmp_DF = tmp_DF.reset_index()
    # resampleして長さが変わらなければそのまま返す
    if len(DF) == len(tmp_DF):
        tmp_DF = DF.copy()
    else:
        # object(str)に戻す
        tmp_DF["timestamps"] = tmp_DF["timestamps"].astype(str)
        # 元のデータに、補間したtimestampsをマージ
        tmp_DF = pd.merge(DF, tmp_DF[["timestamps"]], on="timestamps",
                          how="outer")
        # 並べ替え
        tmp_DF = tmp_DF.sort_values("timestamps")
    return tmp_DF


# 目的：データフレームに対して、指定した累積構成比率を超えたところまでの行を
#       抽出する
# 引数：データフレーム、対象カラム名、累積構成比率
#       ※データフレームは、テーブル形式データを想定
# 返り値：構成比率の低い行を除外したデータフレーム
#     　　【注】元データのインデックス番号は変わらないようにするため、
#               インデックス番号も間引かれた状態で返す
def ccr_cut(DF, tgt_col, rate):
    # 指定したカラムtgt_colで並べ替える
    tmp_DF = DF.sort_values(tgt_col, ascending=False).reset_index(drop=True)
    # 累積比率を算出
    tmp_DF["cumsum_percent"] = np.cumsum(tmp_DF[tgt_col])/tmp_DF[tgt_col].sum()
    # 指定した比率rateを超える境界の値を取得
    border_val = tmp_DF[tmp_DF["cumsum_percent"] >= rate][tgt_col].max()
    # 境界を超えるデータを抽出して返す
    return DF[DF[tgt_col] >= border_val]
