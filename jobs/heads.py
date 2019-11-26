import pandas as pd
import sqlalchemy
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import os
import warnings
warnings.simplefilter(action = "ignore", category = Warning)
Engine = DB.getEngine('10.28.7.43', 'sa', 'tcl+nftx', 'VirtualExchange')
EngineIS = DB.getEngine('10.28.7.43', 'sa', 'tcl+nftx', 'InvestSystem')