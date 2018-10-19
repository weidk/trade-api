using MarketMakerServer.Functions;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MarketMakerServer.Hubs
{
    public class MarketMakerHub : Hub
    {
        private readonly object _dblock = new object();
        public override async Task OnConnectedAsync()
        {
            //string ToSendJson = JsonConvert.SerializeObject(Config.BpChangeDict, Formatting.Indented);
            List<quote> QuoteList = QueryQuote.QueryMarketQuote(Config.BondcodeList, Config.db);
            await Clients.All.SendAsync("NewQuotes", QuoteList);

            List<Order> OrderList = Orders.QueryOrders(Config.OrderDb);
            await Clients.All.SendAsync("Orders", OrderList);

            List<Deal> DealList = Orders.QueryDeals(Config.DealDb);
            await Clients.All.SendAsync("Deals", DealList);
        }

        public async Task SendNewOrder(Order order)
        {
            await Task.Run(() => {
                try
                {
                    string result;
                    lock (_dblock)
                    {
                        result = Orders.NewOrder(order);
                    }
                    Console.WriteLine(result);
                    Clients.Caller.SendAsync("OrderResult", result);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            });
            //await Clients.All.SendAsync("ReceiveMessage", user, message);
        }

        public async Task CancleOrder(Order order)
        {
            await Task.Run(() => {
                try
                {
                    string result;
                    lock (_dblock)
                    {
                        result = Orders.Cancle(order);
                    }
                    Console.WriteLine(result);
                    Clients.Caller.SendAsync("OrderResult", result);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            });
            //await Clients.All.SendAsync("ReceiveMessage", user, message);
        }

    }

    
}
