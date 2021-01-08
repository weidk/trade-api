from  heads import  *


def ReadPosition():
    PosDf = pd.read_sql("select DEALUSERNAME, BONDNAME, BONDCODE, FACEVALUE ,BEGINDATE,round(FundCost/10000,2) FundCost,COSTYIELD from POSITION ",EngineIS)
    PosDf.BEGINDATE = PosDf.BEGINDATE.astype(str)
    return PosDf.to_json(orient="records")

# 查詢所有現券持倉
def ReadBPMPosition():
    Position = pd.read_sql("select * from openquery(TEST1,'select dealusername,bondname,bondcode,sum(AVAILABLEFACEVALUE) AVAILABLEFACEVALUE,sum(remainingfacevalue+takesupfacevalue) totalfacevalue from smp_dhzq_new.VTY_BONDPOOL_NOW where type_pool = 5   group by dealusername,bondname,bondcode  ')",Engine)
    return Position.to_json(orient="records")

# 查詢所有現券持倉
def ReadHisBPMPosition(date):
    Position = pd.read_sql("select * from openquery(TEST1,'select dealusername,bondname,bondcode,sum(AVAILABLEFACEVALUE) AVAILABLEFACEVALUE,sum(remainingfacevalue+takesupfacevalue) TOTALFACEVALUE,to_date(backupday,''yyyyMMdd'') backupday from smp_dhzq_new.VTY_BONDPOOL_HIS where type_pool = 5   and backupday =''"+date+"''  group by dealusername,bondname,bondcode,backupday  ')",Engine)
    Position.BACKUPDAY = Position.BACKUPDAY.astype('str')
    Position.AVAILABLEFACEVALUE = Position.AVAILABLEFACEVALUE.astype('float')
    Position.TOTALFACEVALUE = Position.TOTALFACEVALUE.astype('float')
    Position = Position.sort_values('DEALUSERNAME')
    return Position.to_json(orient="records")


# 查询当日结算持仓
def QuerySettlePosition(date):
    date = date.replace('-','')
    Position = pd.read_sql("select * from openquery(TEST1,'select selftradername,side,bondcode,bondname,sum(totalfacevalue)/100000000 SETTLEFACEVALUE  from marketanalysis.CSTPCBMEXECUTION t where  settlementdate  =''"+date+"'' group by selftradername,side,bondcode,bondname order by selftradername,bondcode,bondname,side ')",Engine)
    Position.SETTLEFACEVALUE = Position.SETTLEFACEVALUE.astype('float')
    return Position.to_json(orient="records")


# 查詢最新持仓、计算成本收益
def CalPosition():
    Df = pd.read_sql("select * from openquery(TEST1,'select a.DealuserName, a.BONDNAME,  a.BONDCODE, sum(AVAILABLEFACEVALUE) AVAILABLEFACEVALUE, sum((remainingfacevalue + takesupfacevalue)) totalfacevalue  from VTY_BONDPOOL_NOW a group by dealusername,bondname ,bondcode')",Engine)
    Df.TOTALFACEVALUE = Df.TOTALFACEVALUE.astype('float')
    return Df.to_json(orient="records")


# def CalPosition():
#     Df = pd.read_sql("select * from openquery(TEST1,'select f14_1429 facerate, gjjj cbNet, f4_1804 cbDirty, f7_1804 cbYield, a.DealuserName, a.BONDNAME,  a.BONDCODE, AVAILABLEFACEVALUE, (remainingfacevalue + takesupfacevalue) totalfacevalue, TRUNC(sysdate) - TRUNC(Begindate) days,  a.netprice COSTNETPRICE  from VTY_BONDPOOL_NOW a inner join tbm_bnd_bond b  on a.BONDCODE = b.f16_1090  ')",Engine)
#     def CompleteFacerate(facerate,code):
#         if pd.isnull(facerate):
#             if 'S' not in code:
#                 code = code + '.IB'
#                 tempDf = pd.read_sql("select * from openquery(WINDNEW,'select B_INFO_COUPONRATE from CBondDescription t where t.s_info_windcode=''"+code+"'' ')",Engine)
#                 return tempDf.ix[0,0]
#         else:
#             return facerate
#     Df.DAYS = Df.DAYS.astype('float')
#     Df.CBYIELD = Df.CBYIELD.astype('float')
#     Df.TOTALFACEVALUE = Df.TOTALFACEVALUE.astype('float')
#     Df.CBNET = Df.CBNET.astype('float')
#     Df.COSTNETPRICE = Df.COSTNETPRICE.astype('float')
#     Df = Df.fillna(0)
#     Df['FACERATE'] = Df.apply(lambda x:CompleteFacerate(x.FACERATE,x.BONDCODE),axis=1)
#     Df['COUPONINCOME'] = round(Df.TOTALFACEVALUE * Df.DAYS * Df.FACERATE/(365*100),2)
#     Df['CAPITALGAINS'] = round(Df.TOTALFACEVALUE * (Df.CBNET - Df.COSTNETPRICE) / 100,2)
#     Df['TOTALGAINS'] = round(Df['COUPONINCOME'] + Df['CAPITALGAINS'],2)
#     Df['AMTDAYS'] = Df.TOTALFACEVALUE * Df.DAYS
#     G = Df.groupby(['DEALUSERNAME','BONDNAME','BONDCODE'])
#     GDf = G['AVAILABLEFACEVALUE', 'TOTALFACEVALUE', 'COUPONINCOME', 'CAPITALGAINS', 'TOTALGAINS','AMTDAYS'].sum()
#     GDf['COST'] = G['CBYIELD'].mean()
#     GDf['WEIGHTEDDAYS'] = round(GDf.AMTDAYS / GDf.TOTALFACEVALUE,2)
#     GDf = GDf.reset_index()
#     return GDf.to_json(orient="records")