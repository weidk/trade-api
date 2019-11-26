from heads import *

def QueryLatestDealYield(bondlist):
    bondstr = ''
    for bond in bondlist:
        bondstr = bondstr + "'"+bond+"',"
    bondstr = "("+bondstr[:-1]+")"
    Df = pd.read_sql("select bondcode,dealprice from qbdb.dbo.LatestQBTRADE_VTY where bondcode in "+bondstr,Engine)
    return Df.to_json(orient='records')