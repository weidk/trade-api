using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace RealtimeApi.Funcs
{
    public class MonitorBp
    {

        //将codestring解析成codeDict
        public static Dictionary<string, double> TransCodeString(string codestr)
        {
            Dictionary<string, double> dict = new Dictionary<string, double>();
            MatchCollection mc = Regex.Matches(codestr, @"\d*");
            foreach (Match m in mc)
            {
                if (m.Value.Length > 0)
                {
                    dict[m.Value] = 0;
                }
            }
            return dict;
        }

        //将codestring解析成typedict
        public static Dictionary<string, string> GetTypeDict(string codestr)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            MatchCollection mc = Regex.Matches(codestr, @"\d*");
            foreach (Match m in mc)
            {
                if (m.Value.Length > 0)
                {
                    char symbol = m.Value[3];
                    if (symbol == '0')
                    {
                        dict[m.Value] = "国债";
                    }else if(symbol == '2')
                    {
                        dict[m.Value] = "国开";
                    }
                    else
                    {
                        dict[m.Value] = "非国开";
                    }

                }
            }
            return dict;
        }

        //初始化读取当日估值数据
        public static Dictionary<string, double> CBondValues(string constr,string codes)
        {
            Dictionary<string, double> Cbdict = new Dictionary<string, double>();
            using (SqlConnection db = new SqlConnection(constr))
            {
                db.Open();
                string sql = "SELECT max(edate) FROM [msnRobot].[dbo].[bondValue]";
                SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, db);
                DataTable Dt = new DataTable();
                DataAdapter.Fill(Dt);
                string MaxDate = Dt.Rows[0][0].ToString();

                string sql1 = "SELECT code,eyield FROM [msnRobot].[dbo].[bondValue] where code in " + codes + " and edate = '" + MaxDate + "'";
                SqlDataAdapter DataAdapter1 = new SqlDataAdapter(sql1, db);
                DataTable Dt1 = new DataTable();
                DataAdapter1.Fill(Dt1);
            
            foreach(DataRow dr in Dt1.Rows)
            {
                Cbdict[dr[0].ToString()] = float.Parse(dr[1].ToString());
            }
            }
            return Cbdict;
        }

        //计算出最新价格变动bp
        public static bool CalChangeBpOld(string CodeString)
        {
            bool IsUpdate = false;
            if (Config.db.State != ConnectionState.Open)
            {
                Config.db.Open();
            }
            string sql = "select a.code,a.yield,a.dealtime from [VirtualExchange].[dbo].[IBHQ] a inner join (select code,max(dealtime) dealtime FROM [VirtualExchange].[dbo].[IBHQ] where dealtime > '" + Config.Dtime + "'   group by code) b on a.code = b.code and a.dealtime = b.dealtime order by a.dealtime ";
            SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, Config.db);
            DataTable Dt = new DataTable();
            DataAdapter.Fill(Dt);
            if (Dt.Rows.Count > 0)
            {
                Config.Dtime = DateTime.Parse(Dt.Rows[0][2].ToString()).ToString("yyyy-MM-dd HH:mm:ss");
                foreach (DataRow dr in Dt.Rows)
                {
                    string code = dr[0].ToString();
                    double yield = double.Parse(dr[1].ToString());
                    if (Config.BpChangeDict.ContainsKey(code))
                    {
                        Config.DealYieldDict[code] = yield;
                        Config.BpChangeDict[code] = Math.Round((yield - Config.CBonds[code]) * 100,2);
                    }
                }
                IsUpdate = true;
            }
            return IsUpdate;
        }


        //计算出最新价格变动bp
        public static bool CalChangeBp(string CodeString)
        {
            bool IsUpdate = false;
            if (Config.db.State != ConnectionState.Open)
            {
                Config.db.Open();
            }
            //string sql = "select a.code,a.yield,a.dealtime from [VirtualExchange].[dbo].[IBHQ] a inner join (select code,max(dealtime) dealtime FROM [VirtualExchange].[dbo].[IBHQ] where dealtime > '" + Config.Dtime + "'   group by code) b on a.code = b.code and a.dealtime = b.dealtime order by a.dealtime ";
            string sql = "select code,yield,dealtime,id from [QBDB].[dbo].[QBTRADEVIEW] where id>"+Config.id+" order by id ";
            SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, Config.db);
            DataTable Dt = new DataTable();
            DataAdapter.Fill(Dt);
            if (Dt.Rows.Count > 0)
            {
                DataRow r = Dt.AsEnumerable().Last<DataRow>();
                Config.id = int.Parse(r[3].ToString());
                foreach (DataRow dr in Dt.Rows)
                {
                    string code = dr[0].ToString();
                    double yield = double.Parse(dr[1].ToString());
                    if (Config.BpChangeDict.ContainsKey(code))
                    {
                        Config.DealYieldDict[code] = yield;
                        Config.BpChangeDict[code] = Math.Round((yield - Config.CBonds[code]) * 100, 2);
                    }
                }
                IsUpdate = true;
            }
            return IsUpdate;
        }
        //将债券期限字典
        public static void GetTermDict(string codestr,string term)
        {
            MatchCollection mc = Regex.Matches(codestr, @"\d*");
            foreach (Match m in mc)
            {
                if (m.Value.Length > 0)
                {
                    Config.BondsTermDict[m.Value] = term;
                }
            }
        }

        //整理成最后发送的json
        public static List<BpInfoClass> GetJsonString(Dictionary<string, double> dict)
        {
            List<BpInfoClass> JsonList = new List<BpInfoClass>();
            try {
                foreach (var d in dict)
                {
                    BpInfoClass Bi = new BpInfoClass();
                    Bi.code = d.Key;
                    Bi.bp = d.Value;
                    Bi.term = Config.BondsTermDict[Bi.code];
                    Bi.type = Config.BondsTypeDict[Bi.code];
                    Bi.yield = Config.DealYieldDict[Bi.code];
                    JsonList.Add(Bi);
                }
            }
            catch(Exception ex)
            {
            }
            
            return JsonList;
            //return JsonConvert.SerializeObject(JsonList, Formatting.Indented);
        }
    }
}
