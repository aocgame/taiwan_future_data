from sqlalchemy import Column, String, ForeignKey, create_engine, text
from sqlalchemy.dialects.mysql import MEDIUMINT, SMALLINT, TINYINT, FLOAT, TIMESTAMP, CHAR, ENUM, DATE, TIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from configparser import ConfigParser
import enum
cfg = ConfigParser()
cfg.read('config.ini')

Base = declarative_base()


class DBEngine():
    """
    Data:2020-05-02
    Auther:win
    Description:数据库
    """
    def __init__(self):
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}' . format(
            cfg.get('db', 'username'),
            cfg.get('db', 'password'),
            cfg.get('db', 'ip'),
            cfg.get('db', 'name')
        ))
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.db = Session()

    def getInstantiation(self):
        return self.db


class ExchangeInfosSchema(Base):
    __tablename__ = 'exchange_infos'
    __table_args__ = {'comment': '交易所信息'}

    id = Column(TINYINT(1, unsigned=True), comment='主键', primary_key=True)
    name = Column(String(50), comment='交易所名称', nullable=False)
    symbol = Column(String(50), comment='交易所代号', nullable=False)
    desc = Column(String(50), comment='描述', nullable=False)
    timezone = Column(String(50), comment='时区', nullable=False)
    created_at = Column(TIMESTAMP, comment='创建时间', nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, comment='更新时间', nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def __repr__(self):
        return "<ExchangeInfosSchema(id='%s', name='%s', symbol='%s', desc='%s', timezone'%s', created_at'%s', updated_at'%s')>" % (
            self.id,
            self.name,
            self.symbol,
            self.desc,
            self.timezone,
            self.created_at,
            self.updated_at
        )


class EnumExchangeGoodsInfoType(enum.Enum):
    other = 1
    stock = 2
    index = 3
    future = 4


class ExchangeGoodsInfosSchema(Base):
    __tablename__ = 'exchange_goods_infos'
    __table_args__ = {'comment': '交易商品信息'}

    id = Column(MEDIUMINT(8, unsigned=True), comment='主键', primary_key=True)
    exchange_info_id = Column(TINYINT(1, unsigned=True), ForeignKey('exchange_infos.id'), comment='交易所ID', nullable=False)
    name = Column(String(50), comment='注释')
    symbol = Column(String(10), comment='商品代号', nullable=False)
    type = Column(ENUM(EnumExchangeGoodsInfoType), comment='交易商品类型', nullable=False)
    session = Column(CHAR(9), comment='每日交易时间(HHmm-HHmm)', nullable=False)
    intermission = Column(String(50), comment='中场休息时间(HHmm-HHmm,...)', nullable=False)
    minmov = Column(MEDIUMINT(8, unsigned=True), comment='最小波动价格', nullable=False, server_default=text('1'))
    pricescale = Column(MEDIUMINT(8, unsigned=True), comment='价格精度(最小可能价格变动=minmov/pricescale)', nullable=False)
    minmov2 = Column(MEDIUMINT(8, unsigned=True), comment='价格变动的子精度', nullable=False, server_default=text('0'))
    exectime_t1 = Column(TIMESTAMP, comment='1分钟进度', nullable=False, server_default=text('0'))
    exectime_h1 = Column(TIMESTAMP, comment='1小时进度', nullable=False, server_default=text('0'))
    exectime_d1 = Column(TIMESTAMP, comment='1天进度', nullable=False, server_default=text('0'))
    exectime_m1 = Column(TIMESTAMP, comment='1个月进度', nullable=False, server_default=text('0'))
    exectime_y1 = Column(TIMESTAMP, comment='1年进度', nullable=False, server_default=text('0'))
    created_at = Column(TIMESTAMP, comment='创建时间', nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, comment='更新时间', nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    user = relationship("ExchangeInfosSchema", backref=backref('exchange_goods_infos'))

    def __repr__(self):
        return "<ExchangeGoodsInfosSchema(id='%s', exchange_info_id='%s', name='%s', symbol='%s', type='%s', session='%s', intermission='%s', minmov='%s', pricescale='%s', minmov2='%s', exectime_t1='%s', exectime_h1='%s', exectime_d1='%s', exectime_m1='%s', exectime_y1='%s', created_at='%s', updated_at='%s')>" % (
            self.id,
            self.exchange_info_id,
            self.name,
            self.symbol,
            self.type,
            self.session,
            self.intermission,
            self.minmov,
            self.pricescale,
            self.minmov2,
            self.exectime_t1,
            self.exectime_h1,
            self.exectime_d1,
            self.exectime_m1,
            self.exectime_y1,
            self.created_at,
            self.updated_at
        )


class BarDataTicksSchema(Base):
    __tablename__ = 'bar_data_ticks'
    __table_args__ = {
        'mysql_engine': 'MyISAM',
        'comment': '分钟频率数据表'
    }

    date = Column(DATE, comment='记录日期', server_default=text('0'), primary_key=True)
    time = Column(TIME, comment='记录时间', server_default=text('0'), primary_key=True)
    interval = Column(SMALLINT(5, unsigned=True), comment='周期(分)', server_default=text('1'), primary_key=True)
    volume = Column(MEDIUMINT(8, unsigned=True), comment='成交量', nullable=False, server_default=text('0'))
    open_price = Column(FLOAT, comment='开盘价', nullable=False)
    high_price = Column(FLOAT, comment='最高价', nullable=False)
    low_price = Column(FLOAT, comment='最低价', nullable=False)
    close_price = Column(FLOAT, comment='收盘价', nullable=False)

    def __repr__(self):
        return "<BarDataTicksSchema(date='%s', time='%s', interval='%s', volume'%s', open_price'%s', high_price'%s', low_price'%s', close_price'%s')>" % (
            self.date,
            self.time,
            self.interval,
            self.volume,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price
        )


class BarDataDaysSchema(Base):
    __tablename__ = 'bar_data_days'
    __table_args__ = {
        'mysql_engine': 'MyISAM',
        'comment': '每日频率数据表'
    }

    date = Column(DATE, comment='记录日期', server_default=text('0'), primary_key=True)
    volume = Column(MEDIUMINT(8, unsigned=True), comment='成交量', nullable=False, server_default=text('0'))
    open_price = Column(FLOAT, comment='开盘价', nullable=False)
    high_price = Column(FLOAT, comment='最高价', nullable=False)
    low_price = Column(FLOAT, comment='最低价', nullable=False)
    close_price = Column(FLOAT, comment='收盘价', nullable=False)

    def __repr__(self):
        return "<BarDataDaysSchema(date='%s', volume'%s', open_price'%s', high_price'%s', low_price'%s', close_price'%s')>" % (
            self.date,
            self.volume,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price
        )


if __name__ == "__main__":
    ed_user = ExchangeInfosSchema(name='台湾期货交易所', symbol='TAIFEX', desc='台指期', timezone='Asia/Taipei')
    ed_user.exchange_goods_infos = [ExchangeGoodsInfosSchema(
        name='加权指数期货连续月',
        symbol='FITXN',
        type='future',
        session='1500-1345',
        intermission='0500-0845,1345-1500',
        minmov=1,
        pricescale=1,
        minmov2=0,
        exectime_t1='2020-04-23 13:45:00'
    )]

    db = DBEngine()
    session = db.getDB()

    session.add(ed_user)
    session.commit()

    exchange_infos = session.query(ExchangeInfosSchema)\
        .filter(ExchangeGoodsInfosSchema.symbol=='FITXN')\
        .first()

    print(exchange_infos)
    print(exchange_infos.exchange_goods_infos)

    db.close()
