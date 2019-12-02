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