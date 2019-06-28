from heads import *

# 查询所有持仓
def getAllTodo():
    Df = pd.read_sql("SELECT * FROM [InvestSystem].[dbo].[TodoItems]  where date >= convert(nvarchar(8),getdate(),112) order by id desc",Engine)
    Df.date = Df.date.astype('str')
    return Df.to_json(orient="records")

# 新建一条记录
def createNewTodo(df):
    df.to_sql('TodoItems', EngineIS, if_exists='append', index=False, index_label=df.columns,dtype={
        'trader': sqlalchemy.String,
        'todo': sqlalchemy.String,
    })

# 删除一条记录
def deleteTodo(data):
    pd.read_sql_query("delete from TodoItems where id = '"+str(data['id'])+"' ",EngineIS)