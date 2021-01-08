from heads import *

# 查询剩余名义本金
def RestNominalPrinciple():
    Df = pd.read_sql("select * from restNomimalPrinciple_VTY",Engine73)
    return Df.to_json(orient="records")

# 查询互换DV01
def IRSDV01():
    Df = pd.read_sql("select * from [MarketMaker].[dbo].[IRSDV01_VTY]",Engine73)
    Df = Df.round(2)
    return Df.to_json(orient="records")