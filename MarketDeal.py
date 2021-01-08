from heads import *


def TrimTypeData(startDate,endDate):
    sonDf = pd.read_sql("select LEFT(InsType,4) InsType ,Treasury,Policy,MTN,SCP,Corporate,CP,CDS,ABS,Other,Date  from [VirtualExchange].[dbo].[BondTypeNetAmt]  where InsType not like '%外资%' and InsType not like '%其他产品类%' and Date>='"+startDate+"' and   Date<='"+endDate+"' order by Date", Engine)
    # 将列汇总为几大类
    Other = sonDf['ABS'] + sonDf['Other']
    CP = sonDf['CP'] + sonDf['SCP']
    CD = sonDf['CDS']
    MTN_COR = sonDf['Corporate']+sonDf['MTN']
    DfTypeNew = pd.concat([sonDf.iloc[:, 0:3], CP, CD,MTN_COR,   sonDf['Date']], axis=1)
    DfTypeNew.columns = ['机构', '国债', '金融债', '短融', '存单','中票/企业债', '日期']
    #根据机构汇总数据
    groupedDf = DfTypeNew.groupby('机构').sum()
    renameIndex(groupedDf.index)

    TreasruyDf = pd.DataFrame(groupedDf['国债']).sort_values('国债',ascending=False)
    PolicyDf = pd.DataFrame(groupedDf['金融债']).sort_values('金融债', ascending=False)
    CpDf = pd.DataFrame(groupedDf['短融']).sort_values('短融', ascending=False)
    CdDf = pd.DataFrame(groupedDf['存单']).sort_values('存单', ascending=False)
    MTNDf = pd.DataFrame(groupedDf['中票/企业债']).sort_values('中票/企业债', ascending=False)

    TreasruyDf = TreasruyDf.reset_index()
    PolicyDf = PolicyDf.reset_index()
    CpDf = CpDf.reset_index()
    CdDf = CdDf.reset_index()
    MTNDf = MTNDf.reset_index()

    TreasruyDf['种类'] = '国债'
    PolicyDf['种类'] = '金融债'
    CpDf['种类'] = '短融'
    CdDf['种类'] = '存单'
    MTNDf['种类'] = '中票/企业债'

    TreasruyDf.columns = ['机构', '数量', '种类']
    PolicyDf.columns = ['机构', '数量', '种类']
    CpDf.columns = ['机构', '数量', '种类']
    CdDf.columns = ['机构', '数量', '种类']
    MTNDf.columns = ['机构', '数量', '种类']

    TypeResultDF = pd.concat([TreasruyDf,PolicyDf,CpDf,CdDf,MTNDf],axis=0,ignore_index=True)

    return TypeResultDF.to_json(orient = 'records')

def renameIndex(ind):
    def switch(argument):
        switcher = {
            '保险': "保险",
            '信托': "信托",
            '农村': "农商行",
            '城市': "城商行",
            '基金': "基金",
            '境外': "境外机构",
            '大型': "大行/政策行",
            '理财': "理财",
            '股份': "股份制",
            '证券': "券商",
            '其他': "其他类型",
        }
        return switcher.get(argument, "nothing")
    i=0
    for item in ind:
        ind.values[i] = switch(item[0:2])
        i+=1


def TrimTermData(startDate,endDate):
    sonDf = pd.read_sql("select LEFT(InsType,4) InsType,Year1,Year3,Year5,Year7,Year10,Year15,Year20,Year30,YearLong,Date from [VirtualExchange].[dbo].[BondTermNetAmt]  where InsType not like '%外资%' and InsType not like '%其他产品类%'  and  Date>='"+startDate+"' and   Date<='"+endDate+"' order by Date", Engine)
    # 将列汇总为几大类
    Long = sonDf['Year15'] + sonDf['Year20'] + sonDf['Year30'] + sonDf['YearLong']
    Ten = sonDf['Year7'] + sonDf['Year10']

    DfTermNew = pd.concat([sonDf.iloc[:, 0:4], Ten,  Long, sonDf['Date']], axis=1)
    DfTermNew.columns = ['机构', '一年', '三年', '五年', '七年&十年','大于十年', '日期']
    #根据机构汇总数据
    groupedDf = DfTermNew.groupby('机构').sum()
    renameIndex(groupedDf.index)

    OneDF = pd.DataFrame(groupedDf['一年']).sort_values('一年',ascending=False)
    ThreeDF = pd.DataFrame(groupedDf['三年']).sort_values('三年',ascending=False)
    FiveDF = pd.DataFrame(groupedDf['五年']).sort_values('五年',ascending=False)
    TenDF = pd.DataFrame(groupedDf['七年&十年']).sort_values('七年&十年',ascending=False)
    LongDF = pd.DataFrame(groupedDf['大于十年']).sort_values('大于十年',ascending=False)

    OneDF = OneDF.reset_index()
    ThreeDF = ThreeDF.reset_index()
    FiveDF = FiveDF.reset_index()
    TenDF = TenDF.reset_index()
    LongDF = LongDF.reset_index()

    OneDF['种类'] = '一年'
    ThreeDF['种类'] = '三年'
    FiveDF['种类'] = '五年'
    TenDF['种类'] = '七年十年'
    LongDF['种类'] = '大于十年'

    OneDF.columns = ['机构', '数量', '种类']
    ThreeDF.columns = ['机构', '数量', '种类']
    FiveDF.columns = ['机构', '数量', '种类']
    TenDF.columns = ['机构', '数量', '种类']
    LongDF.columns = ['机构', '数量', '种类']

    TermResultDF = pd.concat([OneDF,ThreeDF,FiveDF,TenDF,LongDF],axis=0,ignore_index=True)

    return TermResultDF.to_json(orient = 'records')


# 查询交易中心成交
def QueryDeal(endDate,bondname):
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False
    if is_number(bondname):
        DF = pd.read_sql("SELECT * FROM openquery(TEST1,'select  DEALBONDNAME,DEALBONDCODE, DEALCLEANPRICE, DEALYIELD, DEALTOTALFACEVALUE/10000 DEALTOTALFACEVALUE,TRADEMETHOD, TRANSACTTIME from marketanalysis.CMDSCBMDEALT where to_char(dealdate, ''yyyy-mm-dd'') >= ''"+endDate+"'' and   DEALBONDCODE like ''%"+bondname+"%''  order by TRANSACTTIME ' )",Engine)
    else:
        DF = pd.read_sql(
            "SELECT * FROM openquery(TEST1,'select  DEALBONDNAME,DEALBONDCODE, DEALCLEANPRICE, DEALYIELD, DEALTOTALFACEVALUE/10000 DEALTOTALFACEVALUE,TRADEMETHOD, TRANSACTTIME from marketanalysis.CMDSCBMDEALT where to_char(dealdate, ''yyyy-mm-dd'') >= ''" + endDate + "'' and   DEALBONDNAME like ''%" + bondname + "%''  order by TRANSACTTIME ' )",
            Engine)
    DF.TRANSACTTIME = DF.TRANSACTTIME.astype(str)
    return DF.to_json(orient="records")

def QueryTSData(Ins,type):
    if '年' not in type:
        Ins = switchInsname(Ins)
        type = switchType(type)
        # Df = pd.read_sql("select date,"+type+" type from [VirtualExchange].[dbo].[BondTypeNetAmt]   where Instype like '%"+Ins+"%'  and date>='2018-01-01' order by Date",Engine)
        Df = pd.read_sql("select date,"+type+" type from [VirtualExchange].[dbo].[BondTypeNetAmt]   where Instype like '%"+Ins+"%'   order by Date",Engine)
        Df.type = Df.type.cumsum()
        Df['tongbi'] = Df.apply(lambda x: CalTB(x, Df), axis=1)
        Df['tongbi'] = Df['tongbi'].fillna(method='pad')
        Df['MA5-MA20'] = pd.rolling_mean(Df.type, 5) - pd.rolling_mean(Df.type, 20)
        Df.date = Df.date.astype(str)
        return Df.to_json(orient = 'records')
    else:
        Ins = switchInsname(Ins)
        type = switchTermType(type)
        Df = pd.read_sql(
            "select date," + type + " type from [VirtualExchange].[dbo].[BondTermNetAmt]   where Instype like '%" + Ins + "%'   order by Date",
            Engine)
        Df.type = Df.type.cumsum()

        Df['tongbi'] = Df.apply(lambda x:CalTB(x,Df),axis=1)
        Df['tongbi'] = Df['tongbi'].fillna(method='pad')
        # pd.rolling_mean(Df['tongbi'], 5).plot()
        # Dp = Df.set_index('date')
        # # Dp[Dp.tongbi<100].plot(secondary_y='tongbi')
        # # Dp1 = Dp[Dp.tongbi < 100]
        # Dp['tongbiMa'] = pd.rolling_mean(Dp['tongbi'], 5)
        # Dp.plot(secondary_y='type')
        Df['MA5-MA20'] = pd.rolling_mean(Df.type,5)-pd.rolling_mean(Df.type,20)
        Df.date = Df.date.astype(str)
        return Df.to_json(orient='records')

def QueryTSData1(Ins,type):
    if '年' not in type:
        Ins = switchInsname(Ins)
        type = switchType(type)
        Df = pd.read_sql("select date,"+type+" type from [VirtualExchange].[dbo].[BondTypeNetAmt]   where Instype like '%"+Ins+"%'   order by Date",Engine)
        Df.type = Df.type.cumsum()
        Df['tongbi'] = Df.apply(lambda x: CalTB(x, Df), axis=1)
        Df['tongbi'] = Df['tongbi'].fillna(method='pad')
        Df['MA5-MA20'] = pd.rolling_mean(Df.type, 5) - pd.rolling_mean(Df.type, 20)
        Df.date = Df.date.astype(str)
        return Df.to_json(orient = 'records')
    else:
        Ins = switchInsname(Ins)
        CurveDf = CBCurveTs(type)
        type = switchTermType(type)
        Df = pd.read_sql(
            "select date," + type + " type from [VirtualExchange].[dbo].[BondTermNetAmt]   where Instype like '%" + Ins + "%'   order by Date",
            Engine)
        Df = Df.set_index('date')
        DfRst = pd.concat([Df,CurveDf],axis=1)
        DfRst = DfRst.reset_index()
        DfRst.columns = ['date', 'type', 'yield']
        DfRst.type = DfRst.type.cumsum()
        Df['tongbi'] = Df.apply(lambda x: CalTB(x, Df), axis=1)
        Df['tongbi'] = Df['tongbi'].fillna(method='pad')
        Df['MA5-MA20'] = pd.rolling_mean(Df.type, 5) - pd.rolling_mean(Df.type, 20)
        DfRst.date = DfRst.date.astype(str)
        return DfRst.to_json(orient='records')

def QueryTSDataNew(Ins,type,term):
    type = switchTypeNew(type)
    if term != '10年以上':
        Df = pd.read_sql(
            "select date," + type + " type from [VirtualExchange].[dbo].[BondDetailNetAmt]   where Instype like '%" + Ins + "%'  and term like '%"+term+"%'  order by Date",
            Engine)
    else:
        Df = pd.read_sql("select date,sum(" + type + ") type from [VirtualExchange].[dbo].[BondDetailNetAmt]   where Instype like '%" + Ins + "%'  and term in ('10-15年\n（10~15Y）','15-20年\n（15~20Y）','20-30年\n（20~30Y）','30年以上\n（More then 30Y）')  group by Date  order by Date",Engine)
        # Df = pd.read_sql("select date,sum(TreasuryOld+TreasuryNew) type from [VirtualExchange].[dbo].[BondDetailNetAmt]   where Instype like '%" + Ins + "%'  and term in ('10-15年\n（10~15Y）','15-20年\n（15~20Y）','20-30年\n（20~30Y）','30年以上\n（More then 30Y）')  group by Date  order by Date",Engine)
    Df.type = Df.type.cumsum()
    # Df['type1'] = Df.type.cumsum()
    Df['tongbi'] = Df.apply(lambda x: CalTB(x, Df), axis=1)
    Df['tongbi'] = Df['tongbi'].fillna(method='pad')
    # Df['MA5-MA20'] = pd.rolling_mean(Df.type, 5) - pd.rolling_mean(Df.type, 20)
    Df['MA5-MA20'] = Df.type.rolling(5).mean()- Df.type.rolling(20).mean()
    Df.date = Df.date.astype(str)
    return Df.to_json(orient='records')


def switchInsname(argument):
    switcher = {
        "保险":'保险',
        "信托":'信托',
        "农商行":'农村',
        "城商行":'城市',
        "基金":'基金',
        "境外机构":'境外',
        "大行/政策行":'大型',
        "理财":'理财',
        "股份制":'股份',
        "券商":'证券',
        "其他类型":'其他',
    }
    return switcher.get(argument, "nothing")

def switchType(argument):
    switcher = {
        "国债":'Treasury',
        "金融债":'Policy',
        "短融":'SCP+CP',
        "存单":'CDS',
        "ABS":'ABS',
        "中票/企业债":'MTN+Corporate',
    }
    return switcher.get(argument, "nothing")

def switchTypeNew(argument):
    switcher = {
        "国债":'TreasuryOld+TreasuryNew',
        "国债新券":'TreasuryNew',
        "国债老券":'TreasuryOld',
        "金融债":'PolicyOld+PolicyNew',
        "金融债新券":'PolicyNew',
        "金融债老券":'PolicyOld',
        "地方债":'LocalGoverment',
        "存单":'CDS',
        "ABS":'ABS',
        "中票/企业债":'MTN+Corporate',
    }
    return switcher.get(argument, "nothing")

def switchTermType(argument):
    switcher = {
        "一年":'Year1',
        "三年":'Year3',
        "五年":'Year5',
        "七年十年":'Year7+Year10',
        "大于十年":'Year15+Year20+Year30+YearLong',
    }
    return switcher.get(argument, "nothing")

# 获取国开收益曲线
def CBCurveTs(type):
    if type == '七年十年':
        CurveDf = pd.read_sql("select * from openquery(WINDNEW,'select t.trade_dt tradeday,t.b_anal_yield yield from CBondCurveCNBD t  where t.b_anal_curvename  =   ''中债国开债收益率曲线'' and t.b_anal_curvetype = 2 and t.b_anal_curveterm = 10 and t.trade_dt>=''20180101'' order by trade_dt')",Engine)
    else:
        CurveDf = pd.read_sql(
            "select * from openquery(WINDNEW,'select t.trade_dt tradeday,t.b_anal_yield yield from CBondCurveCNBD t  where t.b_anal_curvename  =   ''中债国开债收益率曲线'' and t.b_anal_curvetype = 2 and t.b_anal_curveterm = 5 and t.trade_dt>=''20180101'' order by trade_dt')",
            Engine)
    CurveDf.TRADEDAY = pd.to_datetime(CurveDf.TRADEDAY)
    CurveDf = CurveDf.set_index('TRADEDAY')
    return CurveDf


# 新版数据
def NewData(startDate,endDate):
    Df = pd.read_sql("select *  from [VirtualExchange].[dbo].[BondDetailNetAmt]  where  Date>='"+startDate+"' and   Date<='"+endDate+"' order by Date", Engine)
    Df = Df.replace('\n.*', '', regex=True)
    Df = Df[Df.Term!='合计']
    Df = Df.ix[:,['InsType', 'Term', 'TreasuryNew', 'TreasuryOld', 'PolicyNew','PolicyOld', 'MTN', 'CP', 'Corporate', 'LocalGoverment', 'CDS', 'ABS','Other','Date']]
    Df.Term = Df.Term.replace([ '10-15年', '15-20年','20-30年', '30年以上'],'10年以上')
    GroupDf = Df.groupby(['InsType', 'Term']).sum().reset_index()
    GroupDf['Treasury'] = GroupDf['TreasuryNew'] + GroupDf['TreasuryOld']
    GroupDf['Policy'] = GroupDf['PolicyNew'] + GroupDf['PolicyOld']
    GroupDf['CPMTNCorporate'] = GroupDf['MTN'] + GroupDf['CP'] + GroupDf['Corporate']
    GroupDf = GroupDf.ix[:, ['InsType', 'Term','Treasury','TreasuryNew','TreasuryOld', 'Policy','PolicyNew','PolicyOld','LocalGoverment', 'CDS','CPMTNCorporate', 'ABS']]
    CDS = GroupDf.ix[:,['InsType','CDS']].groupby('InsType').sum().reset_index()
    ABS = GroupDf.ix[:,['InsType','ABS']].groupby('InsType').sum().reset_index()
    MTN = GroupDf.ix[:,['InsType','Term','CPMTNCorporate']]
    MTN.Term = MTN.Term.replace(['10年以上','5-7年', '7-10年'],'5年以上')
    MTN = MTN.groupby(['InsType','Term']).sum().reset_index()
    return json.dumps({'a':GroupDf.to_dict(orient='records'),'b':CDS.to_dict(orient='records'),'c':ABS.to_dict(orient='records'),'d':MTN.to_dict(orient='records')})


# 计算同比
def CalTB(x,Df):
    rst = np.nan
    for i in range(-365,-376,-1):
        base = Df[Df.date == x.date + datetime.timedelta(-365)].type
        if base.values.__len__()==1:
            if base.values[0]!=0:
                rst = x.type-base.values[0]
                rst = float('%.2f' % rst)
                break
    return rst



# Df = pd.read_excel('abc.xlsx')
# writer = pd.ExcelWriter('output.xlsx')
# endDate = '2019-01-01'
# for bondname in Df.名称:
#     print(bondname)
#     rst = pd.read_sql(
#         "SELECT * FROM openquery(TEST1,'select  DEALBONDNAME,DEALBONDCODE, DEALCLEANPRICE, DEALYIELD, DEALTOTALFACEVALUE/10000 DEALTOTALFACEVALUE,TRADEMETHOD, TRANSACTTIME from marketanalysis.CMDSCBMDEALT where to_char(dealdate, ''yyyy-mm-dd'') >= ''" + endDate + "'' and   DEALBONDNAME like ''%" + bondname + "%''  order by TRANSACTTIME ' )",
#         Engine)
#     rst.to_excel(writer,bondname)
# writer.save()


# 当日现券流动性
def BondFlowLatest():
    Df = pd.read_sql("select * from BasicInfo where Date = (select max(Date) from BasicInfo)",EngineIS)
    Df.fillna(0,inplace=True)
    Df.DealTimes = Df.DealTimes.astype('int')
    Df = Df.sort_values('DealTimes',ascending=False)
    Df.Date = Df.Date.astype('str')
    return Df.to_json(orient="records")

# 当日现券利差
def ReadSpreadDf():
    Df = pd.read_sql("select * from SpreadDf where Date = (select max(Date) from SpreadDf)",EngineIS)
    Df.fillna(0,inplace=True)
    Df = Df.sort_values('PercentileMin',ascending=False)
    Df.Date = Df.Date.astype('str')
    return Df.to_json(orient="records")