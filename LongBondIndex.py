from heads import *

# 纯债基金指数
def LBIndex(start,end):
    Df = pd.read_sql("select date 日期,转债, 长久期 from vty_purebondfund where date >='"+start+"' and date<='"+end+"'  order by date",EngineIS)
    ConvertDf = Df.ix[:,['日期','转债']]
    LongDurationDf = Df.ix[:,['日期','长久期']]

    ConvertDf['类型'] = '转债'
    LongDurationDf['类型'] = '长久期'

    ConvertDf.rename(columns={'转债': '数值'}, inplace=True)
    LongDurationDf.rename(columns={'长久期': '数值'}, inplace=True)

    RstDf = pd.DataFrame()
    RstDf = RstDf.append(ConvertDf)
    RstDf = RstDf.append(LongDurationDf)

    RstDf = RstDf.sort('日期')
    RstDf.日期 = RstDf.日期.astype('str')
    return RstDf.to_json(orient="records")