using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace XBondServer
{
    public class XBonds
    {
        public static List<quote> QueryXBond(SqlConnection db)
        {
            List<quote> XBondList = new List<quote>();
            try
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                }
                string sql = "select *  FROM XBONDVIEW2";
                SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, db);
                DataTable Dt = new DataTable();
                DataAdapter.Fill(Dt);
                if (Dt.Rows.Count > 0)
                {
                    XBondList = Tools.ToList<quote>(Dt);
                }
                return XBondList;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return XBondList;
            }
        }

        public async static Task AsynFreshXBond()
        {
            DateTime endTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, 21, 00, 00);
            await Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        if (Startup._hubContext != null)
                        {
                            List<quote> XBondList = XBonds.QueryXBond(Config.db);
                            if (XBondList.Count > 0)
                            {
                                Startup._hubContext.Clients.All.SendAsync("XBond", XBondList);
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
        public string SELFTRADERNAME { get; set; }
        public string BONDNAME { get; set; }
        public string BONDCODE { get; set; }
        public double CURRENTFACEVALUE { get; set; }
        public double COSTNETPRICE { get; set; }
        public double COSTYIELD { get; set; }
        public double YIELD { get; set; }
        public double FLOATYIELDPROFIT { get; set; }
        public double DONEPROFIT { get; set; }
    }
}
