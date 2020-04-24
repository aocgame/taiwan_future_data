
import pandas as pd
import datetime
import os
from configparser import ConfigParser
cfg = ConfigParser()
cfg.read('config.ini')


class TransfromEngine:
    """
    Data:2020-04-23
    Auther:win
    Description:把合成后的CSV转为1分钟bar
    """
    def __init__(self):
        """"""
        Base_Dir = cfg.get('base', 'dir')
        step = datetime.timedelta(minutes=1)
        # 拚接後保存的檔案名
        GermFile_Path = os.path.join(Base_Dir, cfg.get('base', 'germ_file'))
        # 轉換後保存的檔案名
        NewFile_Path = os.path.join(Base_Dir, cfg.get('base', 'parse_file'))
        df = pd.read_csv(GermFile_Path, encoding="GB18030")
        # 選取關心的欄位
        df = df.loc[:, ['Date', 'Time', 'Price', 'Volume']]

        # 收盘tick数据 -1 秒，算进这分钟
        df.loc[df['Time'] == 134500, ['Time']] = 134459
        df.loc[df['Time'] == 50000, ['Time']] = 45959

        # 若 1330 ~ 1335(1345) 交易数量与 1330 相同，表示当日是结算日，把收盘tick数据 -1 秒，算进这分钟
        di1_df = df.loc[(df['Time'] >= 133000) & (df['Time'] <= 133500)].groupby('Date')['Volume'].count().reset_index(name="Count")
        di2_df = df.loc[df['Time'] == 133000].groupby('Date')['Volume'].count().reset_index(name="Count")
        di3_df = di1_df.loc[di1_df['Count'] == di2_df['Count']]
        df.loc[(df['Date'].isin(di3_df['Date'])) & (df['Time'] == 133000), ['Time']] = 132959

        #  把 0 秒的tick +1 秒算进下一分钟
        df.loc[df['Time'] % 100 == 0, ['Time']] = df['Time'] + 1

        # 加个日期索引
        df[["Date"]] = df[["Date"]].astype(str)
        df['Time'] = df['Time'].map(lambda x: str(x).zfill(6))
        df['Datetime'] = pd.to_datetime(
            df['Date'].str.cat(df['Time'], sep=' '),
            format="%Y%m%d %H%M%S",
            infer_datetime_format=True
        )

        # 加索引准备运算
        df = df.set_index('Datetime')

        # 计算开高低收
        price_df = df['Price'].resample('1T', label='right', closed='right').ohlc().dropna()

        indexlist = price_df.index.tolist()
        for i in range(1, len(indexlist)):
            self.ffill(indexlist, i, step)

        indexlist.sort()
        price_df = price_df.reindex(index=indexlist, method='ffill')

        # 合并成交量
        vols = df['Volume'].resample('1T', label='right', closed='right').sum().dropna()
        vol_df = pd.DataFrame(vols, columns=['Volume'])
        vol_df['Volume'] = vol_df.apply(lambda x: int(x['Volume'] / 2), axis=1)
        df = price_df.merge(vol_df, left_index=True, right_index=True)

        # 调整名字与类型
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
        df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].astype(int)

        # 对0的成交数据用收盘价填充开高低价
        df.loc[df['Volume'] == 0, ['Open', 'High', 'Low']] = df['Close']

        df = df.reset_index()
        df['Date'] = df.apply(lambda x: x['Datetime'].strftime('%Y/%m/%d'), axis=1)
        df['Time'] = df.apply(lambda x: x['Datetime'].strftime('%H:%M:%S'), axis=1)

        # 除掉没用的字段
        df = df.loc[:, ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # 儲存整理後的數據到新的csv檔
        df.to_csv(NewFile_Path, encoding="GB18030", index=False)

        # print(df)

    def ffill(self, indexlist, i, step):
        '''
        交易1分线 8:46~13:45、15:01~5:00
        除了头以外需要有上一分钟的交易
        '''
        last = indexlist[i - 1]
        now = indexlist[i]
        if now.hour == 8 and now.minute == 46:
            # 上一分钟应该是5:00
            if last.hour != 5 or last.minute != 0:
                print('夜盘未满5:00', last)
            pass
        elif now.hour == 15 and now.minute == 1:
            # 上一分钟应该是13:45 or 结算日的 13:30
            if last.hour != 13 or (last.minute != 45 and last.minute != 30):
                print('日盘未满13:45', last)
            pass
        elif now - last != step:
            # 必须要存在1分钟的交易
            for i in range(1, int((now - last) / step)):
                indexlist.append(last + step * i)


if __name__ == "__main__":
    TransfromEngine()
