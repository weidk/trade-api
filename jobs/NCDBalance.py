# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # #                     计算NCD余额变动                       # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import pandas as pd
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import sqlalchemy
import time
import warnings
warnings.simplefilter(action = "ignore", category = Warning)

Engine = DB.getEngine('10.28.7.43', 'bond', 'bond', 'InvestSystem')

# 计算单日的NCD余额数据
def OneDayBalance(dateString):
    Df = pd.read_sql("select * from openquery(WINDNEW,'select  nvl(a.b_info_issuertype,b.b_info_issuertype) banktype,nvl(a.amt,0)-nvl(b.amt,0) netamt from (select b_info_issuertype,sum(B_ISSUE_AMOUNTACT) amt  from CBONDDESCRIPTION   where  b_info_specialbondtype  = ''同业存单'' and B_INFO_LISTDATE=''"+dateString+"''  group by b_info_issuertype) a full join (select b_info_issuertype,sum(B_ISSUE_AMOUNTACT) amt  from CBONDDESCRIPTION  where  b_info_specialbondtype  = ''同业存单'' and B_INFO_MATURITYDATE=''"+dateString+"''  group by b_info_issuertype) b on a.b_info_issuertype = b.b_info_issuertype')",Engine)
    if Df.shape[0]>0:
        Df.NETAMT = Df.NETAMT.astype('float')
        bankTypeList = ['城市商业银行','国有商业银行','股份制商业银行','政策性银行']
        DfOld = pd.read_sql("select  BANKTYPE , NETAMT from NCDBalance where date = (select max(date) from NCDBalance  where date<'"+dateString+"')  and BANKTYPE !='合计'", Engine)
        DfOld.NETAMT = DfOld.NETAMT.astype('float')
        def ComsumAmt(type,oldAmt):
            newAmt = 0
            if type in Df.BANKTYPE.values:
                newAmt = Df[Df.BANKTYPE == type].NETAMT.iloc[0]
            return newAmt + oldAmt
        DfOld['NETAMT'] = DfOld.apply(lambda x:ComsumAmt(x.BANKTYPE,x.NETAMT),axis=1)
        Df = DfOld
        Df = Df.append({'BANKTYPE':'合计', 'NETAMT':Df.NETAMT.sum()}, ignore_index=True)
        Df['DATE'] = dateString
    return Df

def ToSql(Df):
    Df.to_sql('NCDBalance', Engine, if_exists='append', index=False, index_label=Df.columns,
                    dtype={'DATE': sqlalchemy.DateTime, 'BANKTYPE': sqlalchemy.String})


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == '__main__':
        try:
            startday = pd.read_sql("select max(date) from NCDBalance",Engine).ix[0,0].strftime("%Y%m%d")
            dateList = pd.date_range(start=startday,end=dh.today2str(2),closed='right')
            for date in dateList:
                dateStr = date.strftime('%Y%m%d')
                Df = OneDayBalance(dateStr)
                if Df.shape[0] > 0:
                    ToSql(Df)
                    print(dateStr+' success')
        except Exception:
            print("error")