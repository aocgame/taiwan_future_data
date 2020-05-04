from sqlalchemy import or_, and_
from .db import DBEngine, ExchangeGoodsInfosSchema, BarDataTicksSchema, BarDataDaysSchema
from datetime import timedelta
import datetime
import pandas as pd


class ToDayEngine:
    """
    Data:2020-05-03
    Auther:win
    Description:把1分钟bar转成1天bar并存入数据库
    """
    def __init__(self):
        """"""
        while True:
            db = DBEngine().getInstantiation()

            # 取得当前最新状态
            exchange_goods_info = db.query(ExchangeGoodsInfosSchema)\
                .filter(ExchangeGoodsInfosSchema.exchange_info_id == 1, ExchangeGoodsInfosSchema.symbol == 'FITXN')\
                .first()

            # 获取日线更新后的分钟频率数据
            bar_data_ticks = db.query(BarDataTicksSchema)
            if exchange_goods_info.exectime_d1:
                date = exchange_goods_info.exectime_d1.strftime('%Y-%m-%d')
                time = exchange_goods_info.exectime_d1.strftime('%H:%M:%S')
                bar_data_ticks = bar_data_ticks\
                    .filter(
                        or_(
                            BarDataTicksSchema.date > date,
                            and_(BarDataTicksSchema.date == date, BarDataTicksSchema.time > time)
                        )
                    )

            bar_data_ticks = bar_data_ticks.order_by(BarDataTicksSchema.date.asc(), BarDataTicksSchema.time.asc()).limit(100000)
            df = pd.read_sql(bar_data_ticks.statement, db.get_bind())

            # 把时间加上收盘后的时间
            day_end_str = exchange_goods_info.session[5:9]
            day_end_min = 1439 - int(day_end_str[0:2]) * 60 - int(day_end_str[2:4])
            df['datetime'] = pd.to_datetime(df.date.astype(str) + ' ' + df.time.astype(str)) + timedelta(minutes=day_end_min)

            # 查找当天的最大时间是否大于23:43，倘若不是就把那天加入下一个交易日(因为结算日13:30就收盘了)
            df['date'] = df['datetime'].dt.date
            df['time'] = df['datetime'].dt.time
            di1_df = df.groupby([df['date']])['time']
            tmp_date = ''
            for name, group in di1_df:
                if tmp_date:
                    df.loc[df['date'] == tmp_date, ['date']] = name
                    tmp_date = ''
                if str(group.max()) < '23:42:00':
                    tmp_date = name

            if 0 == df.shape[0]:
                break

            # 删除日线还未收盘的数据
            if tmp_date:
                df.drop(df[df.date == tmp_date].index, inplace=True)

            df['datetime'] = pd.to_datetime(df.date.astype(str) + ' ' + df.time.astype(str))

            # 加索引准备运算
            df = df.set_index('datetime')

            # 计算开高低收
            ohlc_dict = {'open_price': 'first', 'high_price': 'max', 'low_price': 'min', 'close_price': 'last', 'volume': 'sum'}
            df = df.resample('D').apply(ohlc_dict).dropna(how='any')

            df = df.reset_index()

            if 0 == df.shape[0]:
                print('没有解析出日线频率数据')
                break

            diffnow = datetime.datetime.now() - df.iloc[-1]['datetime']

            # 塞进数据库
            df['date'] = df.apply(lambda x: x['datetime'].strftime('%Y-%m-%d'), axis=1)
            df = df.loc[:, ['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
            db.bulk_insert_mappings(BarDataDaysSchema, df.to_dict(orient="records"))

            # 更新 1 天线最新时间
            df_tail = df.iloc[-1]
            exchange_goods_info.exectime_d1 = datetime.datetime.strptime(df_tail['date'] + ' ' + day_end_str + '00', '%Y-%m-%d %H%M%S')
            db.commit()

            db.close()

            print('完成', diffnow.days, '天前的日线数据')
            if diffnow.days < 1:
                break


if __name__ == "__main__":
    ToDayEngine()
