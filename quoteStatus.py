from heads import *


# 返回做市报价表
def GetMarketMakerQUoteStatus(conn):
    Df = pd.read_sql("select securityid,buyleaveqty,sellleaveqty,quoteid,quotestatus from CBMARKETQUOTE where quoteid>0 order by id desc",conn);
    def SwitchStatus(status):
        switcher = {
            '16': '正常',
            '19': '撤销',
            '21': '过期',
            '107': '全部成交',
            '108': '部分成交',
        }
        return switcher.get(status, '')

    Df.QUOTESTATUS = Df.QUOTESTATUS.apply(SwitchStatus)
    return Df.to_json(orient="records")

# 返回限价报价表
def GetLimitMakerQUoteStatus(conn):
    Df = pd.read_sql("select securityid,leavesqty,status,orderid,ordstatus,clordid,price,yield from CBLIMITQUOTE t where orderid>0 order by id desc",conn);
    def SwitchStatus(status):
        switcher = {
            '0': '正常',
            '4': '撤销',
            'C': '过期',
            '2': '全部成交',
            '1': '部分成交',
        }
        return switcher.get(status, '')

    Df.ORDSTATUS = Df.ORDSTATUS.apply(SwitchStatus)
    return Df.to_json(orient="records")