from heads import *


def TrimTypeData(startDate,endDate):
    sonDf = pd.read_sql("select * from [VirtualExchange].[dbo].[BondTypeNetAmt]  where Date>='"+startDate+"' and   Date<='"+endDate+"' order by Date", Engine)
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

    TreasruyDf = pd.DataFrame(groupedDf['国债']).sort(columns='国债',ascending=False)
    PolicyDf = pd.DataFrame(groupedDf['金融债']).sort(columns='金融债', ascending=False)
    CpDf = pd.DataFrame(groupedDf['短融']).sort(columns='短融', ascending=False)
    CdDf = pd.DataFrame(groupedDf['存单']).sort(columns='存单', ascending=False)
    MTNDf = pd.DataFrame(groupedDf['中票/企业债']).sort(columns='中票/企业债', ascending=False)

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
    sonDf = pd.read_sql("select * from [VirtualExchange].[dbo].[BondTermNetAmt]  where Date>='"+startDate+"' and   Date<='"+endDate+"' order by Date", Engine)
    # 将列汇总为几大类
    Long = sonDf['Year15'] + sonDf['Year20'] + sonDf['Year30'] + sonDf['YearLong']
    Ten = sonDf['Year7'] + sonDf['Year10']

    DfTermNew = pd.concat([sonDf.iloc[:, 0:4], Ten,  Long, sonDf['Date']], axis=1)
    DfTermNew.columns = ['机构', '一年', '三年', '五年', '七年&十年','大于十年', '日期']
    #根据机构汇总数据
    groupedDf = DfTermNew.groupby('机构').sum()
    renameIndex(groupedDf.index)

    OneDF = pd.DataFrame(groupedDf['一年']).sort(columns='一年',ascending=False)
    ThreeDF = pd.DataFrame(groupedDf['三年']).sort(columns='三年',ascending=False)
    FiveDF = pd.DataFrame(groupedDf['五年']).sort(columns='五年',ascending=False)
    TenDF = pd.DataFrame(groupedDf['七年&十年']).sort(columns='七年&十年',ascending=False)
    LongDF = pd.DataFrame(groupedDf['大于十年']).sort(columns='大于十年',ascending=False)

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
        DF = pd.read_sql("SELECT * FROM openquery(TEST,'select  DEALBONDNAME,DEALBONDCODE, DEALCLEANPRICE, DEALYIELD, DEALTOTALFACEVALUE/10000 DEALTOTALFACEVALUE,TRADEMETHOD, TRANSACTTIME from marketanalysis.CMDSCBMDEALT where to_char(dealdate, ''yyyy-mm-dd'') >= ''"+endDate+"'' and   DEALBONDCODE like ''%"+bondname+"%''  order by TRANSACTTIME ' )",Engine)
    else:
        DF = pd.read_sql(
            "SELECT * FROM openquery(TEST,'select  DEALBONDNAME,DEALBONDCODE, DEALCLEANPRICE, DEALYIELD, DEALTOTALFACEVALUE/10000 DEALTOTALFACEVALUE,TRADEMETHOD, TRANSACTTIME from marketanalysis.CMDSCBMDEALT where to_char(dealdate, ''yyyy-mm-dd'') >= ''" + endDate + "'' and   DEALBONDNAME like ''%" + bondname + "%''  order by TRANSACTTIME ' )",
            Engine)
    DF.TRANSACTTIME = DF.TRANSACTTIME.astype(str)
    return DF.to_json(orient="records")

def QueryTSData(Ins,type):
    if '年' not in type:
        Ins = switchInsname(Ins)
        type = switchType(type)
        Df = pd.read_sql("select date,"+type+" type from [VirtualExchange].[dbo].[BondTypeNetAmt]   where Instype like '%"+Ins+"%'  and date>='2018-01-01' order by Date",Engine)
        Df.date = Df.date.astype(str)
        return Df.to_json(orient = 'records')
    else:
        Ins = switchInsname(Ins)
        type = switchTermType(type)
        Df = pd.read_sql(
            "select date," + type + " type from [VirtualExchange].[dbo].[BondTermNetAmt]   where Instype like '%" + Ins + "%'  and date>='2018-01-01' order by Date",
            Engine)
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