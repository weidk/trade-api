import pandas as pd
import sqlalchemy
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)
Engine = DB.getEngine('172.18.3.43', 'sa', 'tcl+nftx', 'VirtualExchange')

BondList = [
170004,
170006,
170007,
170010,
170013,
170014,
170016,
170018,
170020,
170021,
170023,
170025,
170027,
180001,
180002,
180007,
180008,
180013,
180014,
180015,
180016,
180019,
180203,
180210,
180211,
170303,
170304,
170309,
170310,
180302,
180304,
180309,
170403,
170404,
170405,
170409,
170411,
170412,
170415,
180401,
180402,
180403,
180405,
180409]

for bond in BondList:
    print("insert into QUOTATIONSUBCRIBOND (ID, BONDCODE, BONDNAME, CREATETIME) values (quotationsubcribond_seq.nextval, '"+str(bond)+"', null, null);")
    # pd.read_sql_query("select * from openquery(TRADENEW,'insert into QUOTATIONSUBCRIBOND (ID, BONDCODE, BONDNAME, CREATETIME) values (quotationsubcribond_seq.nextval, ''"+str(bond)+"'', null, null)')",Engine)
