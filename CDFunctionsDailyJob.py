from  heads import  *


def ReadPosition():
    PosDf = pd.read_sql("select DEALUSERNAME, BONDNAME, BONDCODE, FACEVALUE ,BEGINDATE,round(FundCost/10000,2) FundCost,COSTYIELD from POSITION ",EngineIS)
    PosDf.BEGINDATE = PosDf.BEGINDATE.astype(str)
    return PosDf.to_json(orient="records")

# 查詢所有現券持倉
def ReadBPMPosition():
    Position = pd.read_sql("select * from openquery(TEST,'select dealusername,bondname,bondcode,sum(AVAILABLEFACEVALUE) AVAILABLEFACEVALUE,sum(remainingfacevalue+takesupfacevalue) totalfacevalue from smp_dhzq_new.VTY_BONDPOOL_HIS where type_pool = 5  and backupday = (select max(backupday) from smp_dhzq_new.VTY_BONDPOOL_HIS) group by dealusername,bondname,bondcode  ')",Engine)
    return Position.to_json(orient="records")

# 查询当日结算持仓
def QuerySettlePosition(date):
    Position = pd.read_sql("select * from openquery(TEST1,'select selftradername,side,bondcode,bondname,sum(totalfacevalue)/100000000 SETTLEFACEVALUE  from marketanalysis.CSTPCBMEXECUTION t where  to_char(settlementdate, ''yyyy-mm-dd'')  =''"+date+"'' group by selftradername,side,bondcode,bondname order by selftradername,bondcode,bondname,side ')",Engine)
    Position.SETTLEFACEVALUE = Position.SETTLEFACEVALUE.astype('float')
    return Position.to_json(orient="records")