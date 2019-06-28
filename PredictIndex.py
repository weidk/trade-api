from heads import *

# XBond指标
def XBondIndex():
    Df = pd.read_sql("select * from openquery(TEST1,'select *  from xbondindex_VTY t  where  tradedate>=''2019-01-01'' order by tradedate ')",Engine)
    Df.NEXTDAYINDEX = Df.NEXTDAYINDEX.astype(float)
    Df = Df[Df.NEXTDAYINDEX!=0]
    Df.TRADEDATE = Df.TRADEDATE.astype(str)
    return Df.to_json(orient="records")