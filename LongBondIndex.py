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

    RstDf = RstDf.sort_values('日期')
    RstDf.日期 = RstDf.日期.astype('str')
    return RstDf.to_json(orient="records")

# 转债总体转股溢价率
def ConvpRatio(start,end):
    Df = pd.read_sql(
        "select dt 日期, convp_ratio 加权溢价率,convp_ratio_simple 简单溢价率, conp 中债转债指数, conp_parity 正股估算, impliedvol_weight 加权波动率,impliedvol_simple 简单波动率 from convp_ratio where dt >='" + start + "' and dt<='" + end + "'  order by dt",
        EngineIS)
    Df = Df.round(2)
    Df.日期 = Df.日期.astype('str')
    return Df.to_json(orient="records")

def ConvpRatioNew(start,end):
    Df = pd.read_sql(
        "select * from ConvpRatio where date >='" + start + "' and date<='" + end + "'  order by date",
        EngineIS)
    Df = Df.round(2)
    Df.date = Df.date.astype('str')
    return Df.to_json(orient="records")



# 纯债个券历史
def ConvpRatioHistory(start,end,code):
    if 'SH' in code or 'SZ' in code:
        Df = pd.read_sql("SELECT  [CONVPREMIUMRATIO] 转股溢价率 ,[STRBPREMIUMRATIO] 纯债溢价率 ,[CLOSE] 收盘价 ,[dt] 日期  FROM [InvestSystem].[dbo].[cbdetail] where dt >='" + start + "' and dt<='" + end + "'  and code = '"+code+"'  order by dt",EngineIS)
    else:
        Df = pd.read_sql("SELECT  [CONVPREMIUMRATIO] 转股溢价率 ,[STRBPREMIUMRATIO] 纯债溢价率 ,[CLOSE] 收盘价 ,[dt] 日期  FROM [InvestSystem].[dbo].[cbdetail] where dt >='" + start + "' and dt<='" + end + "'  and SEC_NAME = '"+code+"'  order by dt",EngineIS)
    Df = Df.round(4)
    Df.日期 = Df.日期.astype('str')
    return Df.to_json(orient="records")

# 转债列表
def CBondList():
    Df = pd.read_sql("SELECT  distinct code,SEC_NAME  FROM [InvestSystem].[dbo].[cbdetail] where (code like '%SH' or code like '%SZ') and code not like 'q%'  and SEC_NAME not like '%退市%'",EngineIS)
    DfNew = Df.iloc[:,0].append(Df.iloc[:,1])
    DfNew = pd.DataFrame(DfNew,columns=['code'])
    return DfNew.to_json(orient="records")


# 转债赎回触发
def CBondTrigger():
    Df = pd.read_sql("SELECT * FROM [InvestSystem].[dbo].[ConvpJK] where  date = (select max(date) from [InvestSystem].[dbo].[ConvpJK])",EngineIS)
    Df.进度 = round(Df.进度,2)
    Df.转股价值 = round(Df.转股价值,2)
    Df.转股比例 = round(Df.转股比例,2)
    Df.转股流动比例 = round(Df.转股流动比例,2)
    # Df = Df.sort_values('赎回触发情况',ascending=False)
    Df.date = Df.date.astype('str')
    return Df.to_json(orient="records")