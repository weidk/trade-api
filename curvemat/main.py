from heads import *

D0 = pd.read_sql("select max(Date) from MatTs",Engine).ix[0,0]
D1 = pd.read_sql("select * from openquery(WIND,'select max(trade_dt) from CBondCurveCNBD')",Engine).ix[0,0]
ToCalDateRange = pd.date_range(D0,D1)[1:]

for day in ToCalDateRange:
    Fn.CurveBag(day.strftime('%Y%m%d'))

# ToCalDateRangeFuture = pd.date_range('20170105','20180126')
# for day in ToCalDateRangeFuture:
#     Fn.CurveBagFuture(day.strftime('%Y%m%d'))