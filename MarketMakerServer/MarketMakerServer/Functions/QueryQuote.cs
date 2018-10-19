using MarketMakerServer.Utils;
using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MarketMakerServer.Functions
{
    public class QueryQuote
    {
        public static List<quote> QueryMarketQuote(string bondcode, SqlConnection db)
        {
            List<quote> QuoteList = new List<quote>();
            try
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                }
                string sql = "select * from vty_combinequote where code in "+bondcode+"  order by ID desc";
                SqlDataAdapter DataAdapter = new SqlDataAdapter(sql,db);
                DataTable Dt = new DataTable();
                DataAdapter.Fill(Dt);
                if (Dt.Rows.Count > 0)
                {
                    QuoteList = Tools.ToList<quote>(Dt);
                }     
                return QuoteList;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return QuoteList;
            }
        } 

        public async static Task AsynFreshQuote()
        {
            DateTime endTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, 16, 30, 00);
            await Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        if(Startup._hubContext != null)
                        {
                            List<quote> QuoteList = QueryQuote.QueryMarketQuote(Config.BondcodeList, Config.db);
                            int thisID;
                            if (QuoteList.Count == 0)
                            {
                                thisID = 0;
                            }
                            else
                            {
                                thisID = QuoteList[0].id;
                            }
                                
                            if (thisID == 0)
                            {
                                if (Config.MAXQUOTEID > 0)
                                {
                                    Startup._hubContext.Clients.All.SendAsync("NewQuotes", QuoteList);
                                    Config.MAXQUOTEID = thisID;
                                }
                            }
                            else if (thisID > Config.MAXQUOTEID)
                            {
                                Startup._hubContext.Clients.All.SendAsync("NewQuotes", QuoteList);
                                Config.MAXQUOTEID = thisID;
                            }
                        }
                        if (DateTime.Now > endTime)
                        {
                            System.Environment.Exit(0);
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            });
        }
    }

    public class quote
    {
        public int id { get; set; }
        public string code { get; set; }
        public double bid { get; set; }
        public double bidvol { get; set; }
        public double ofr { get; set; }
        public double ofrvol { get; set; }
        public string ordtime { get; set; }
    }
}
