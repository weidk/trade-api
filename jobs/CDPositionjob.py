from  heads import *
# 查询库里所有的存单
def getAllPositons():
    # Position = pd.read_sql("select * from openquery(TEST1,'select dealusername,bondname,bondcode,(remainingfacevalue+takesupfacevalue) facevalue,fullprice costFullprice,begindate,backupday from smp_dhzq_new.VTY_BONDPOOL_HIS where type_pool = 5  and backupday = (select max(backupday) from smp_dhzq_new.VTY_BONDPOOL_HIS) and bondname like ''%CP%''  and dealusername = ''危定坤'' ')",Engine)
    Position = pd.read_sql("select * from openquery(TEST1,'select dealusername,bondname,bondcode,(remainingfacevalue+takesupfacevalue) facevalue,fullprice costFullprice,begindate,backupday from smp_dhzq_new.VTY_BONDPOOL_HIS where type_pool = 5  and backupday = (select max(backupday) from smp_dhzq_new.VTY_BONDPOOL_HIS) and bondname like ''%CD%'' ')",Engine)
    return Position

# 全价计算到期收益率 y = ((Fv-Pv)/Pv)*(TY/D)
def CalYield(code,Pv,NextTradeDay):
    try:
        code = code + ".IB"
        Df = pd.read_sql(
            # "select * from openquery(WIND,'select ceil(to_date(B_INFO_MATURITYDATE,''yyyyMMdd'') - sysdate) D from CBondDescription  where S_INFO_WINDCODE = ''" + code + "'' and B_INFO_COUPON = ''505003000''')",
            "select * from openquery(WIND,'select ceil(to_date(B_INFO_MATURITYDATE,''yyyyMMdd'') - to_date(''"+NextTradeDay+"'',''yyyyMMdd'')) D from CBondDescription  where S_INFO_WINDCODE = ''" + code + "'' ')",
            Engine)
        D = float(Df.ix[0, 0])
        TY = 365
        Fv = 100
        y = round(100 * ((Fv - Pv) / Pv) * (TY / D), 4)
        return y
    except:
        return 0

# 计算持有期资金成本
def CalFundCost(Position,costRate):
    # 获取最近的T+1债券交易日
    NextTradeDay = pd.read_sql("select * from openquery(WIND,'select min(trade_days) from CBondCalendar where S_INFO_EXCHMARKET = ''NIB'' and trade_days>sysdate  order by trade_days') ",Engine)
    NextTradeDay = NextTradeDay.ix[0, 0]
    NextTradeDay = dh.str2date(NextTradeDay,type = 2)
    # 过了几个晚上算几天的资金成本
    Position['holdDays'] = (NextTradeDay - Position.BEGINDATE).apply(lambda x: x.days)
    # Position['holdDays'] = (dh.str2date(dh.today2str()) - Position.BEGINDATE).apply(lambda x: x.days)
    Position['FundCost'] = (Position.FACEVALUE.astype(float) * Position.COSTFULLPRICE) * 100 * costRate * Position.holdDays / 365
    Position['FundCostFullPrice'] = ((Position.FACEVALUE.astype(float) * Position.COSTFULLPRICE) * 100 + Position['FundCost'])/(Position.FACEVALUE.astype(float)*100)
    return Position

# 下个交易日
def CalNextTdDay():
    NextTradeDay = pd.read_sql(
        "select * from openquery(WIND,'select min(trade_days) from CBondCalendar where S_INFO_EXCHMARKET = ''NIB'' and trade_days>sysdate  order by trade_days') ",
        Engine)
    NextTradeDay = NextTradeDay.ix[0, 0]
    return NextTradeDay

# 写入数据库
def WritePositionToSql(Position):
    Position.to_sql('Position', EngineIS, if_exists='replace', index=False, index_label=Position.columns,
           dtype={'backupday': sqlalchemy.DateTime,
                  'DEALUSERNAME': sqlalchemy.String,
                  'BONDNAME': sqlalchemy.String,
                  'BONDCODE': sqlalchemy.String,
                  'BEGINDATE': sqlalchemy.DateTime,
                  })

NextTradeDay = CalNextTdDay()
Position = getAllPositons()
Position = CalFundCost(Position,0.038)
Position['COSTYIELD'] = Position.apply(lambda x:CalYield(x['BONDCODE'],x['FundCostFullPrice'],NextTradeDay),axis=1)
WritePositionToSql(Position)
