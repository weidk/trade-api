using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XBondServer
{
    public class XBondHub : Hub
    {
        public override async Task OnConnectedAsync()
        {
            //string ToSendJson = JsonConvert.SerializeObject(Config.BpChangeDict, Formatting.Indented);
            List<quote> XBondList = XBonds.QueryXBond( Config.db);
            if (XBondList.Count > 0)
            {
                await Clients.All.SendAsync("XBond", XBondList);
            }  
        }
    }
}
