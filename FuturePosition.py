from heads import *

# 读取持仓总数据
def ReadFuturePos(start,end):
    Df = pd.read_sql("select productid,datatypeid,varposition,tradingday from FuturePositionAggregate_vty where tradingday>='"+start+"' and tradingday<='"+end+"' and datatypeid in (1,2) and instrumentid is null  order by tradingday",Engine103)
    Df.datatypeid = Df.datatypeid.replace([1,2],['多','空'])
    Df.tradingday = Df.tradingday.astype('str')
    return Df.to_json(orient="records")

# 计算主力持仓
def QueryMainPosition(start,end,insname):
    Df = pd.read_sql("select productid,instrumentid,datatypeid,sum(volume) position,tradingday from FuturePositionChange where tradingday>='" + start + "' and tradingday<='" + end + "' and datatypeid in (1,2) and shortname = '"+insname+"' group by productid,instrumentid,datatypeid,tradingday order by tradingday",Engine103)
    Df.datatypeid = Df.datatypeid.replace([1, 2], ['多', '空'])
    Df.tradingday = Df.tradingday.astype('str')
    Df['type'] =  Df.datatypeid + '_' + Df.instrumentid
    Df.productid = Df.productid.str.replace(' ','')
    return Df.ix[:,['type','position','tradingday','productid','datatypeid']].to_json(orient="records")

# 计算主力净持仓
def QueryMainNetPosition(start,end,insname):
    Df = pd.read_sql("select shortname,sum(minusposition) netposition,productid,tradingday from minusposition_vty where tradingday>='" + start + "' and tradingday<='" + end + "'  and shortname = '"+insname+"' group by shortname,productid,tradingday order by tradingday",Engine103)
    Df.tradingday = Df.tradingday.astype('str')
    Df.productid = Df.productid.str.replace(' ','')
    return Df.to_json(orient="records")
