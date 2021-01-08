import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)

conn = DB.getConn('192.168.87.103', 'sa', 'sa123', 'FutureTradeTest')
cursor = conn.cursor()
cursor.execute("update [FutureTradeTest].[dbo].[Account]  set total_profit = 0,float_profit=0,real_profit=0,enable_amount = total_amount")
cursor.execute("update [FutureTrade].[dbo].[Account]  set total_profit = 0,float_profit=0,real_profit=0,enable_amount = total_amount")


import redis
pool = redis.ConnectionPool(host='192.168.87.103', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
r.set('positionStr','0')
r.set('positionStrTest','0')