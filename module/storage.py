
import pandas as pd
import os
import sqlalchemy
from configparser import ConfigParser
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
        df['datetime'] = pd.to_datetime(
            df['Date'].str.cat(df['Time'], sep=' '),
            format="%Y/%m/%d %H:%M:%S",
            infer_datetime_format=True
        )
        df.rename(columns={'Open': 'open_price', 'High': 'high_price', 'Low': 'low_price', 'Close': 'close_price', 'Volume': 'volume'}, inplace=True)

        # 除掉没用的字段
        df = df.loc[:, ['datetime', 'interval', 'volume', 'open_price', 'high_price', 'low_price', 'close_price']]

        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}' . format(
            cfg.get('db', 'username'),
            cfg.get('db', 'password'),
            cfg.get('db', 'ip'),
            cfg.get('db', 'name')
        ))

        for i in range(0, df.shape[0], 9000):
            df.loc[i:i + 8999].to_sql(con=database_connection, name='bar_data', if_exists='append', index=False)


if __name__ == "__main__":
    StorageEngine()
