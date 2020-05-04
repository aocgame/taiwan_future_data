
import pandas as pd
import os
from configparser import ConfigParser
from .db import DBEngine, ExchangeGoodsInfosSchema, BarDataTicksSchema
import datetime
cfg = ConfigParser()
cfg.read('config.ini')


class StorageEngine:
    """
    Data:2020-04-23
    Auther:win
    Description:把1分钟bar存入数据库
    """
    def __init__(self):
        """"""
        Base_Dir = cfg.get('base', 'dir')

        # 轉換後保存的檔案名
        NewFile_Path = os.path.join(Base_Dir, cfg.get('base', 'parse_file'))
        df = pd.read_csv(NewFile_Path, encoding="GB18030")
        # 選取關心的欄位
        df = df.loc[:, ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df['interval'] = 1
        df.rename(columns={'Date': 'date', 'Time': 'time', 'Open': 'open_price', 'High': 'high_price', 'Low': 'low_price', 'Close': 'close_price', 'Volume': 'volume'}, inplace=True)

        # 除掉没用的字段
        df = df.loc[:, ['date', 'time', 'interval', 'volume', 'open_price', 'high_price', 'low_price', 'close_price']]

        db = DBEngine().getInstantiation()
        exchange_goods_info = db.query(ExchangeGoodsInfosSchema)\
            .filter(ExchangeGoodsInfosSchema.exchange_info_id == 1, ExchangeGoodsInfosSchema.symbol == 'FITXN')\
            .first()

        if exchange_goods_info.exectime_t1:
            date = exchange_goods_info.exectime_t1.strftime('%Y/%m/%d')
            time = exchange_goods_info.exectime_t1.strftime('%H:%M:%S')
            df = df.loc[((df['date'] == date) & (df['time'] > time)) | (df['date'] > date)]

        for i in range(0, df.shape[0], 10000):
            df_cur = df.loc[i:i + 9999]
            db.bulk_insert_mappings(BarDataTicksSchema, df_cur.to_dict(orient="records"))

            # 更新 1 分钟线最新时间
            df_tail = df_cur.iloc[-1]
            exchange_goods_info.exectime_t1 = datetime.datetime.strptime(df_tail['date'] + ' ' + df_tail['time'], '%Y/%m/%d %H:%M:%S')
            db.commit()

        db.close()


if __name__ == "__main__":
    StorageEngine()
