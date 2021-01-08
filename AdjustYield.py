from heads import *

def GeyAdjYield():
    Df = pd.read_sql("select * from AdjYield where TradeDate = (select max(TradeDate) from AdjYield)",EngineIS)
    return Df.to_json(orient='records')


def AdjYieldHistory(startday,endday):
    Df = pd.read_sql(
        "select  * from AdjYield where TradeDate>='" + startday + "'     and TradeDate<='" + endday + "'   order by TradeDate",
        EngineIS)
    Df.TradeDate = pd.to_datetime(Df.TradeDate)
    Df.TradeDate = Df.TradeDate.astype('str')
    return Df.to_json(orient='records')