from flask import Flask,jsonify,make_response,request
from flask_socketio import SocketIO ,emit

import pandas as pd
from pandas.io.json import json_normalize

# import cx_Oracle
import sqlalchemy
from functools import wraps
import Tools.DateHelper as dh
import Tools.DataBaseHelper as DB
import warnings
warnings.simplefilter(action = "ignore", category = Warning)
# conn=cx_Oracle.connect('FITRADINGSERVER/FITRADINGSERVER@FITRADE')    #连接数据库
Engine = DB.getEngine('172.18.3.43', 'sa', 'tcl+nftx', 'VirtualExchange')
EngineIS = DB.getEngine('172.18.3.43', 'sa', 'tcl+nftx', 'InvestSystem')

