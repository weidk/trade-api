using MarketMakerServer.Utils;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MarketMakerServer.Functions
{
    public class Orders
    {
        #region 发送、撤销委托
        //发送新委托
        public static string NewOrder(Order order)
        {
            try {
                String sql = "INSERT INTO MarketMaker.dbo.ORDERS (ORDERID,BONDCODE,DIRECTION,PRICE,QTY,TRADER,ORDERTIME,STATUS) VALUES (@ORDERID,@BONDCODE,@DIRECTION,@PRICE,@QTY,@TRADER,@ORDERTIME,@STATUS)";

                using (SqlCommand command = new SqlCommand(sql, Config.WriteNewOrderDb))
                {
                    command.Parameters.AddWithValue("@ORDERID", order.orderid);
                    command.Parameters.AddWithValue("@BONDCODE", order.bondcode);
                    command.Parameters.AddWithValue("@DIRECTION", order.direction);
                    command.Parameters.AddWithValue("@PRICE", order.price);
                    command.Parameters.AddWithValue("@QTY", order.qty);
                    command.Parameters.AddWithValue("@TRADER", order.trader);
                    command.Parameters.AddWithValue("@ORDERTIME", order.ordertime);
                    command.Parameters.AddWithValue("@STATUS", "买卖");

                    if (Config.WriteNewOrderDb.State != ConnectionState.Open)
                    {
                        Config.WriteNewOrderDb.Open();
                    }
                    int result = command.ExecuteNonQuery();

                    // Check Error
                    if (result < 0)
                    {
                        return order.orderid + "   订单发送失败.";
                    }
                    else
                    {
                        return order.orderid + "   订单发送成功!";
                    }

                }
            } catch(Exception ex) {
                return ex.ToString();
            }
        }
        //撤销委托
        public static string Cancle(Order order)
        {
            try
            {
                String sql = "INSERT INTO MarketMaker.dbo.ORDERS (ORDERID,BONDCODE,DIRECTION,PRICE,QTY,TRADER,ORDERTIME,STATUS) VALUES (@ORDERID,@BONDCODE,@DIRECTION,@PRICE,@QTY,@TRADER,@ORDERTIME,@STATUS)";

                using (SqlCommand command = new SqlCommand(sql, Config.WriteNewOrderDb))
                {
                    command.Parameters.AddWithValue("@ORDERID", order.orderid);
                    command.Parameters.AddWithValue("@BONDCODE", order.bondcode);
                    command.Parameters.AddWithValue("@DIRECTION", order.direction);
                    command.Parameters.AddWithValue("@PRICE", order.price);
                    command.Parameters.AddWithValue("@QTY", order.qty);
                    command.Parameters.AddWithValue("@TRADER", order.trader);
                    command.Parameters.AddWithValue("@ORDERTIME", order.ordertime);
                    command.Parameters.AddWithValue("@STATUS", "撤销");

                    if (Config.WriteNewOrderDb.State != ConnectionState.Open)
                    {
                        Config.WriteNewOrderDb.Open();
                    }
                    int result = command.ExecuteNonQuery();

                    // Check Error
                    if (result < 0)
                    {
                        return order.orderid + "   撤销发送失败.";
                    }
                    else
                    {
                        return order.orderid + "   撤销发送成功!";
                    }

                }
            }
            catch (Exception ex)
            {
                return ex.ToString();
            }
        }
        #endregion

        #region 查询委托最新状态
        public static List<Order> QueryOrders(SqlConnection db)
        {
            List<Order> OrderList = new List<Order>();
            try
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                }
                string sql = "select id,orderid,bondcode,direction,price,qty,trader,convert(varchar(100),[ORDERTIME] ,108) ordertime,status from marketmaker.dbo.vty_orders order by id desc";
                SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, db);
                DataTable Dt = new DataTable();
                DataAdapter.Fill(Dt);
                if (Dt.Rows.Count > 0)
                {
                    OrderList = Tools.ToList<Order>(Dt);
                }
                return OrderList;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return OrderList;
            }
        }

        public async static Task AsynFreshOrder()
        {
            await Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        if (Startup._hubContext != null)
                        {
                            List<Order> OrderList = Orders.QueryOrders(Config.OrderDb);
                            if (OrderList.Count > 0)
                            {
                                int thisID = OrderList[0].id;
                                if (thisID > Config.MAXORDERID)
                                {
                                    Startup._hubContext.Clients.All.SendAsync("Orders", OrderList);
                                    Config.MAXORDERID = thisID;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            });
        }
        #endregion

        #region 查询成交最新状态
        public static List<Deal> QueryDeals(SqlConnection db)
        {
            List<Deal> DealList = new List<Deal>();
            try
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                }
                string sql = "select id,orderid,dealid,bondcode,direction,dealprice,dealqty,trader,convert(varchar(100),[ORDERTIME] ,108) ordertime,status from marketmaker.dbo.deals order by id desc";
                SqlDataAdapter DataAdapter = new SqlDataAdapter(sql, db);
                DataTable Dt = new DataTable();
                DataAdapter.Fill(Dt);
                if (Dt.Rows.Count > 0)
                {
                    DealList = Tools.ToList<Deal>(Dt);
                }
                return DealList;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return DealList;
            }
        }

        public async static Task AsynFreshDeal()
        {
            await Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        if (Startup._hubContext != null)
                        {
                            List<Deal> DealList = Orders.QueryDeals(Config.DealDb);
                            if (DealList.Count > 0)
                            {
                                int thisID = DealList[0].id;
                                if (thisID > Config.MAXDEALID)
                                {
                                    Startup._hubContext.Clients.All.SendAsync("Deals", DealList);
                                    Config.MAXDEALID = thisID;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            });
        }
        #endregion

    }





    public class Order
    {
        public int id { get; set; }

        [JsonProperty("price")]
        public Double price { get; set; }

        [JsonProperty("amt")]
        public Double qty { get; set; }

        [JsonProperty("bondcode")]
        public String bondcode { get; set; }

        [JsonProperty("direction")]
        public String direction { get; set; }

        [JsonProperty("name")]
        public String trader { get; set; }

        [JsonProperty("ordertime")]
        public String ordertime { get; set; }

        public String status { get; set; }
        [JsonProperty("orderid")]
        public String orderid { get; set; }
    }

    public class Deal
    {
        public int id { get; set; }

        public Double dealprice { get; set; }

        public Double dealqty { get; set; }

        public String bondcode { get; set; }

        public String direction { get; set; }

        public String trader { get; set; }

        public String ordertime { get; set; }

        public String status { get; set; }

        public String orderid { get; set; }

        public String dealid { get; set; }
    }
}
