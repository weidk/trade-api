from heads import *

# 查询所有持仓
def getAllPosition():
    Df = pd.read_sql("SELECT * FROM [InvestSystem].[dbo].[checkPosition]  where date >= convert(nvarchar(8),getdate(),112) ",Engine)
    Df.date = Df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
    return Df.to_json(orient="records")

# 新建一条记录
def createNewPositon(df):
    df.to_sql('checkPosition', EngineIS, if_exists='append', index=False, index_label=df.columns,dtype={
        'trader': sqlalchemy.String,
        'bondcode': sqlalchemy.String,
        'bondname': sqlalchemy.String,
    })

# 删除一条记录
def deleteData(data):
    pd.read_sql_query("delete from checkPosition where id = '"+str(data['id'])+"' ",EngineIS)


# ******************************************************************************************************
# ******************************************************************************************************

# 新建一条结算头寸
def createNewSettle(df):
    # df['netamt'] = df.sellamt - df.buyamt
    df.drop(['key'], axis=1, inplace=True)
    df.to_sql('settleposition', EngineIS, if_exists='append', index=False, index_label=df.columns,dtype={
        'trader': sqlalchemy.String,
        'bondcode': sqlalchemy.String,
        'note': sqlalchemy.String,
        'nonbond': sqlalchemy.String,
    })

# 查询所有结算头寸
def getSettle(date = 'convert(nvarchar(8),getdate(),112)'):
    Df = pd.read_sql("SELECT id,trader,bondcode,buyamt,sellamt,netamt,note,frozeamt,nonbond FROM [InvestSystem].[dbo].[settleposition]  where convert(nvarchar(8),date,112) = "+date+"  and isdelete = 0 order by id desc",Engine)
    return Df.to_json(orient="records")

# 删除一条结算记录
def deleteSettle(data):
    pd.read_sql_query("update settleposition set isdelete = 1 where id = '"+str(data)+"' ",EngineIS)

# 查询明日结算金额
def getTotalAmt(date = 'convert(nvarchar(8),getdate(),112)'):
    Df = pd.read_sql("SELECT sum(netamt) totalamt FROM [InvestSystem].[dbo].[settleposition]  where convert(nvarchar(8),date,112) = "+date+"  and isdelete = 0 and bondcode is not null",Engine)
    if Df.ix[0,0]!=None:
        return jsonify(round(Df.ix[0,0],1))
    else:
        return jsonify(0)

# 查询明日非现券结算头寸
def getNonBond(date='convert(nvarchar(8),getdate(),112)'):
    Df = pd.read_sql(
        "SELECT trader,nonbond FROM [InvestSystem].[dbo].[settleposition]  where convert(nvarchar(8),date,112) = " + date + "  and isdelete = 0 and bondcode is  null",
        Engine)
    return Df.to_json(orient="records")

# 查询净卖净买债券
def getNetBond(date = 'convert(nvarchar(8),getdate(),112)'):
    # Df = pd.read_sql("SELECT bondcode,sum(netamt) net FROM [InvestSystem].[dbo].[settleposition]  where date >= convert(nvarchar(8),getdate(),112)  and isdelete = 0 group by bondcode",Engine)
    Df = pd.read_sql("SELECT bondcode,sum(netamt) net,sum(frozeamt) froze  FROM [InvestSystem].[dbo].[settleposition]  where convert(nvarchar(8),date,112) = "+date+"  and isdelete = 0 and bondcode is not null  group by bondcode",Engine)
    Df = Df.round(1)
    DfSell = Df[Df.net > 0].ix[:,['bondcode','net']]
    DfSell.index = [i for i in range(DfSell.shape[0])]
    DfBuy = Df[Df.net < 0].ix[:,['bondcode','net']]
    DfBuy.net = -1 * DfBuy.net
    DfBuy.index = [i for i in range(DfBuy.shape[0])]
    DfFroze = Df[Df.froze > 0].ix[:,['bondcode','froze']]
    DfFroze.index = [i for i in range(DfFroze.shape[0])]
    DfNew = pd.concat([DfBuy,DfSell,DfFroze], axis=1)
    DfNew.columns = ['buybond', 'buynet','sellbond', 'sellnet','frozebond', 'frozenet']
    return DfNew.to_json(orient="records")

# 查询未提交头寸的交易员
def getUnSubs(date = 'convert(nvarchar(8),getdate(),112)'):
    Df = pd.read_sql("SELECT distinct trader  FROM [InvestSystem].[dbo].[settleposition]  where convert(nvarchar(8),date,112) = "+date+"  and isdelete = 0 ",Engine)
    traderList = ['姚子剑', '施晓乐', '危定坤', '翁应良', '郭圣雨', '欧阳璐璐', '李剑波', '寿林荣','宋文博','周春来']
    return jsonify(list(set(traderList) ^ set(Df.trader.tolist())))


# 查询密码
def getPSW():
    Df = pd.read_sql("SELECT [password] FROM [InvestSystem].[dbo].[positionpsw]",Engine)
    return jsonify(Df.ix[0,0])

# 查询是否允许添加头寸
def getAllowStatus():
    Df = pd.read_sql("SELECT [NotAllowAdd] FROM [InvestSystem].[dbo].[positionpsw]",Engine)
    return Df.to_json(orient="records")

# 改变新增状态
def ChangAllowStatus(NoAdd):
    try:
        pd.read_sql("UPDATE positionpsw SET NotAllowAdd = "+str(NoAdd),EngineIS)
    except:
        pass
    Df = pd.read_sql("SELECT [NotAllowAdd] FROM [InvestSystem].[dbo].[positionpsw]", EngineIS)
    return Df.to_json(orient="records")

# 查询上市缴款
def QueryOutstanding():
    cursor = crmDb['crm_downstream_flat'].find({"downstream.s.trader":{"$in":["翁应良","施晓乐","钱晓雨","章早暐","姚子剑","周春来","郭圣雨","欧阳璐璐","危定坤","李剑波","宋文博","寿林荣","周春来"]},"downstream.a.amount":{"$gt":0},"bond.outstandingDay":{"$gte":dh.today2str()}},{"bond.name":1,"bond.code":1,"bond.paymentDay":1,"bond.outstandingDay":1,"downstream.s.trader":1,"downstream.a.amount":1,"downstream.a.mediator.value":1})
    list_cursor = list(cursor)
    Df = pd.DataFrame()
    for lt in list_cursor:
        Df = Df.append([{
            'bondname': lt.get('bond').get('name'),
            'bondcode': lt.get('bond').get('code'),
            'paymentDay': lt.get('bond').get('paymentDay'),
            'outstandingDay': lt.get('bond').get('outstandingDay'),
            'trader': lt.get('downstream').get('s').get('trader'),
            'amount': lt.get('downstream').get('a').get('amount'),
            'mediator': lt.get('downstream').get('a').get('mediator').get('value'),
        }], ignore_index=True)
    if(any(Df)):
        Df['settleDay'] = Df.apply(lambda x: x.paymentDay if x.mediator =='东海证券' else x.outstandingDay,axis=1)
        Df = Df[Df.settleDay>dh.today2str()]
        Df = Df.sort_values('settleDay')
    return Df.to_json(orient="records")


# 查询+1结算流水
def QueryNextDaySettle(trader):
    Df = pd.read_sql("select * from openquery(TEST1,'select * from VTY_SETTLEPOSITION t where t.selftradername = ''"+trader+"''')",EngineIS)
    if Df.shape[0]>0:
        Df[Df.SETTLEDATE.min() == Df.SETTLEDATE]
        Df.SETTLEDATE = Df.SETTLEDATE.astype('str')
        Df.BUYFACEVALUE = Df.BUYFACEVALUE.astype('float')
        Df.SELLFACEVALUE = Df.SELLFACEVALUE.astype('float')
        Df.NETFACEVALUE = Df.NETFACEVALUE.astype('float')
        Df.TOTALDEALTIMES = Df.TOTALDEALTIMES.astype('float')
        Df.columns = ['trader', 'SETTLEDATE', 'bondcode', 'bondname', 'buyamt','sellamt', 'netamt', 'TOTALDEALTIMES']
        return Df.to_json(orient="records")
    else:
        return Df.to_json(orient="records")


# 查询借贷头寸
def BondLendPosition():
    Df = pd.read_sql("select * from openquery(TEST1,'select t.bondname,t.bondcode,t.initfacevalue,t.begindate,t.enddate,t.PLEDGEBOND from VTY_BONDLEND t  where t.Enddate is not null and t.enddate>=trunc(sysdate)  order by t.enddate  ')",Engine)
    Df2 = pd.read_sql("SELECT [date],[trader],[nonbond] FROM [InvestSystem].[dbo].[settleposition] where (nonbond like '%债券借贷%' or nonbond like '%新增借贷%') and  nonbond not like '%到期%' and  isdelete=0   and date>DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)  order by id desc",Engine)
    pattern = re.compile(r'\d{6}')
    def getCode(rawtext):
        reList = pattern.findall(rawtext)
        if len(reList)>0:
            return reList[0]
        else:
            return ''
    Df2['bondlendcode'] = Df2.nonbond.apply(getCode)
    def GuanLian(code,begindate):
        tempDf = Df2[(Df2.bondlendcode==code) & (Df2.date<begindate)]
        if tempDf.shape[0]>0:
            return tempDf.iloc[0,1] +" : "+tempDf.iloc[0,2]
    Df['info'] = Df.apply(lambda x:GuanLian(x.BONDCODE,x.BEGINDATE),axis=1)
    Df.BEGINDATE = Df.BEGINDATE.astype('str')
    Df.ENDDATE = Df.ENDDATE.astype('str')
    return Df.dropna(subset=['info']).to_json(orient="records")

# 查询债券通头寸
def BCSettlementPosition():
    Df = pd.read_sql("select * from openquery(TEST1,'select  t.SETTLEMENTDATE,t.BONDCODE,t.BONDNAME,t.SIDE,t.TotalFaceValue/100000000 TotalFaceValue,t.COUNTERPARTYSHORTNAME,t.SelfTraderName,t.TradeDate from marketanalysis.CSTPCBMEXECUTION t   where    t.COUNTERPARTYSHORTNAME like ''%bc%'' and  to_date(SETTLEMENTDATE,''YYYYMMDD'')>=  SYSDATE order by t.SETTLEMENTDATE desc ')",Engine)
    Df.SETTLEMENTDATE = Df.SETTLEMENTDATE.astype('str')
    return Df.to_json(orient="records")
