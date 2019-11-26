import pandas as pd
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)

EngineIS = DB.getEngine('10.28.7.43', 'sa', 'tcl+nftx', 'InvestSystem')
